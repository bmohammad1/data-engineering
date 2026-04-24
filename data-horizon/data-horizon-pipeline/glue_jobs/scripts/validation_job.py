"""Glue validation job — Child3, step 2 of 2.

Reads cleaned Parquet from the cleaned S3 bucket, applies per-table data quality
rules, and routes records to one of two destinations:

  PASS → s3://<VALIDATED_BUCKET>/validated/<run_id>/<table>/  (Parquet, snappy)
  FAIL → s3://<QUARANTINE_BUCKET>/quarantine/<run_id>/<table>/ (JSON, _validation_errors)

Updates DynamoDB with per-tag and per-run validation outcomes.
Registers validated Parquet tables in the Glue Data Catalog so Athena
can query them immediately without MSCK REPAIR TABLE.

A tag is marked VALIDATE=FAILED only when every record it contributed across
all 13 tables was quarantined (valid_count == 0). Partial quarantine is
acceptable — the tag still contributed usable data.

Raises RuntimeError if every record across all tables fails validation —
this signals a catastrophic data quality failure to the Step Function.
"""

import logging
import sys
import time

from awsglue.utils import getResolvedOptions
from pyspark.sql import functions as F

from utils.schema_definitions import TABLE_SCHEMAS
from utils.spark_helpers import (
    add_audit_columns,
    create_glue_context,
    read_parquet_from_s3,
    write_json_to_s3,
    write_parquet_to_catalog,
)
from utils.validation_rules import apply_validation
from utils.dynamodb_updater import (
    bulk_update_tag_validate_status,
    fetch_transform_succeeded_tags,
    update_run_validate_status,
)

from shared.constants import STATUS_FAILED, STATUS_RUNNING, STATUS_SUCCESS, load_ssm_config
from shared.logger import configure_logging, run_id_ctx

configure_logging()
logger = logging.getLogger(__name__)


# Maps each fact table to the source column used to derive the partition date.
# Tables absent from this map are written flat (no partitioning) because they
# are dimension or transactional tables with low row counts where partitioning
# would create too many tiny files relative to the data volume.
_PARTITION_SOURCE_COLUMN: dict[str, str] = {
    "measurements":          "Timestamp",
    "alarms":                "Timestamp",
    "events":                "Timestamp",
    "inventory":             "LastUpdated",
    "maintenance":           "MaintenanceDate",
    "regulatory_compliance": "InspectionDate",
    "financial_forecasts":   "ForecastDate",
}

# Name of the derived date column — kept consistent with the cleaned layer so
# Athena partition projection and downstream Glue jobs rely on a single name.
_PARTITION_DATE_COLUMN = "partition_date"


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------


def main() -> None:
    """Glue job entry point."""
    args = getResolvedOptions(sys.argv, ["JOB_NAME", "run_id", "ENVIRONMENT"])

    run_id = args["run_id"]
    ssm_config = load_ssm_config(args["ENVIRONMENT"])
    cleaned_bucket = ssm_config["cleaned-bucket-name"]
    validated_bucket = ssm_config["validated-bucket-name"]
    quarantine_bucket = ssm_config["quarantine-bucket-name"]
    dynamo_table_name = ssm_config["pipeline-state-table"]
    glue_database = ssm_config["glue-database"]

    run_id_ctx.set(run_id)
    job_start_time = time.perf_counter()

    glue_ctx, _, job = create_glue_context(args["JOB_NAME"], args)

    logger.info(
        "Validation job started",
        extra={
            "run_id": run_id,
            "cleaned_bucket": cleaned_bucket,
            "validated_bucket": validated_bucket,
            "quarantine_bucket": quarantine_bucket,
        },
    )

    update_run_validate_status(dynamo_table_name, run_id, STATUS_RUNNING)

    total_valid = 0
    total_quarantined = 0

    # tag_counts tracks valid and quarantined row counts per tag across all tables.
    # Structure: { tag_id: { "valid": int, "invalid": int } }
    tag_counts: dict[str, dict] = {}

    all_tag_ids: set[str] = fetch_transform_succeeded_tags(dynamo_table_name, run_id)
    logger.info(
        "Tags loaded from DynamoDB",
        extra={"run_id": run_id, "tag_count": len(all_tag_ids)},
    )

    for table_name, table_schema in TABLE_SCHEMAS.items():
        table_start_time = time.perf_counter()
        cleaned_path = f"s3://{cleaned_bucket}/cleaned/{run_id}/{table_name}/"

        try:
            table_dataframe = read_parquet_from_s3(glue_ctx, cleaned_path, table_schema)

            valid_dataframe, invalid_dataframe = apply_validation(table_dataframe, table_name)

            valid_record_count = valid_dataframe.count()
            invalid_record_count = invalid_dataframe.count()

            if valid_record_count > 0:
                catalog_table_name = f"validated_{table_name}"
                validated_path = f"s3://{validated_bucket}/validated/{run_id}/{table_name}/"
                valid_dataframe_with_audit = add_audit_columns(valid_dataframe, run_id, table_name)

                source_col = _PARTITION_SOURCE_COLUMN.get(table_name)
                if source_col is not None:
                    valid_dataframe_with_audit = valid_dataframe_with_audit.withColumn(
                        _PARTITION_DATE_COLUMN,
                        F.to_date(F.col(source_col)),
                    )
                    partition_cols = [_PARTITION_DATE_COLUMN]
                else:
                    partition_cols = None

                write_parquet_to_catalog(
                    glue_ctx,
                    valid_dataframe_with_audit,
                    glue_database,
                    catalog_table_name,
                    validated_path,
                    partition_cols=partition_cols,
                )

            if invalid_record_count > 0:
                quarantine_path = f"s3://{quarantine_bucket}/quarantine/{run_id}/{table_name}/"
                invalid_dataframe_with_audit = add_audit_columns(invalid_dataframe, run_id, table_name)
                write_json_to_s3(invalid_dataframe_with_audit, quarantine_path)

            total_valid += valid_record_count
            total_quarantined += invalid_record_count

            # Accumulate per-tag valid counts for DynamoDB outcome classification.
            table_has_tag_id_column = "TagID" in valid_dataframe.columns
            if table_has_tag_id_column:
                rows_per_tag = valid_dataframe.groupBy("TagID").count().collect()
                for row in rows_per_tag:
                    tag_id = row.TagID
                    if tag_id:
                        tag_counts.setdefault(tag_id, {"valid": 0, "invalid": 0})
                        tag_counts[tag_id]["valid"] += row["count"]

            # Accumulate per-tag invalid counts for DynamoDB outcome classification.
            table_has_tag_id_in_invalid = "TagID" in invalid_dataframe.columns
            if table_has_tag_id_in_invalid:
                rows_per_tag = invalid_dataframe.groupBy("TagID").count().collect()
                for row in rows_per_tag:
                    tag_id = row.TagID
                    if tag_id:
                        tag_counts.setdefault(tag_id, {"valid": 0, "invalid": 0})
                        tag_counts[tag_id]["invalid"] += row["count"]

            table_duration_ms = round((time.perf_counter() - table_start_time) * 1_000, 2)
            logger.info(
                "Table validated",
                extra={
                    "run_id": run_id,
                    "table": table_name,
                    "valid": valid_record_count,
                    "invalid": invalid_record_count,
                    "duration_ms": table_duration_ms,
                },
            )

        except Exception:
            logger.exception(
                "Failed to validate table",
                extra={"run_id": run_id, "table": table_name},
            )
            raise

    # Classify every tag into SUCCESS or FAILED and build the DynamoDB update list.
    # A tag is FAILED only if it produced zero valid records across all 13 tables.
    tags_success = 0
    tags_failed = 0
    tag_update_list = []

    for tag_id in all_tag_ids:
        tag_record_counts = tag_counts.get(tag_id, {"valid": 0, "invalid": 0})
        tag_has_valid_records = tag_record_counts["valid"] > 0

        if tag_has_valid_records:
            tag_status = STATUS_SUCCESS
            tags_success += 1
        else:
            tag_status = STATUS_FAILED
            tags_failed += 1
            logger.error(
                "Tag validation failed — all records quarantined",
                extra={
                    "run_id": run_id,
                    "tag_id": tag_id,
                    "valid_count": 0,
                    "invalid_count": tag_record_counts["invalid"],
                },
            )

        tag_update_list.append({
            "tag_id": tag_id,
            "status": tag_status,
            "valid_count": tag_record_counts["valid"],
            "invalid_count": tag_record_counts["invalid"],
        })

    bulk_update_tag_validate_status(dynamo_table_name, run_id, tag_update_list)

    total_duration_ms = round((time.perf_counter() - job_start_time) * 1_000, 2)

    update_run_validate_status(
        dynamo_table_name,
        run_id,
        STATUS_SUCCESS,
        validate_tags_success=tags_success,
        validate_tags_failed=tags_failed,
        records_validated=total_valid,
        records_rejected=total_quarantined,
        duration_ms=int(total_duration_ms),
    )

    logger.info(
        "Validation job completed",
        extra={
            "run_id": run_id,
            "total_valid": total_valid,
            "total_quarantined": total_quarantined,
            "tags_success": tags_success,
            "tags_failed": tags_failed,
            "total_duration_ms": total_duration_ms,
        },
    )

    all_records_failed_validation = total_valid == 0
    if all_records_failed_validation:
        raise RuntimeError(
            f"All {total_quarantined} records failed validation for run {run_id} — pipeline aborted"
        )

    job.commit()


if __name__ == "__main__":
    main()
