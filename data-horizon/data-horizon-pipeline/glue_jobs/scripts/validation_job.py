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

from utils.schema_definitions import REDSHIFT_COLUMN_MAP, TABLE_SCHEMAS
from utils.spark_helpers import (
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
            valid_dataframe = valid_dataframe.cache()
            invalid_dataframe = invalid_dataframe.cache()

            table_has_tag_id = "TagID" in valid_dataframe.columns

            if table_has_tag_id:
                # Single groupBy over a union of valid + invalid rows — one shuffle
                # instead of two separate groupBy actions per table.
                labeled = (
                    valid_dataframe.select("TagID", F.lit("valid").alias("_outcome"))
                    .union(invalid_dataframe.select("TagID", F.lit("invalid").alias("_outcome")))
                )
                for row in labeled.groupBy("TagID", "_outcome").count().collect():
                    tag_id = row.TagID
                    if tag_id:
                        tag_counts.setdefault(tag_id, {"valid": 0, "invalid": 0})
                        tag_counts[tag_id][row["_outcome"]] += row["count"]

                valid_record_count = sum(v["valid"] for v in tag_counts.values()) - total_valid
                invalid_record_count = sum(v["invalid"] for v in tag_counts.values()) - total_quarantined
            else:
                # Non-TagID tables (tag, equipment, location, customer) — no tag
                # tracking needed, count directly to avoid silent under-counting.
                valid_record_count = valid_dataframe.count()
                invalid_record_count = invalid_dataframe.count()

            if valid_record_count > 0:
                catalog_table_name = f"validated_{table_name}"
                validated_path = f"s3://{validated_bucket}/validated/{run_id}/{table_name}/"

                # Rename PascalCase columns to snake_case in one select so Redshift
                # COPY FORMAT AS PARQUET matches by name without silent zero-row loads.
                rename_map = REDSHIFT_COLUMN_MAP[table_name]
                rename_exprs = [
                    F.col(c).alias(rename_map.get(c, c)) for c in valid_dataframe.columns
                ]
                renamed_dataframe = valid_dataframe.select(rename_exprs).coalesce(1)

                write_parquet_to_catalog(
                    renamed_dataframe,
                    glue_database,
                    catalog_table_name,
                    validated_path,
                )

            if invalid_record_count > 0:
                quarantine_path = f"s3://{quarantine_bucket}/quarantine/{run_id}/{table_name}/"
                # Invalid records already carry _run_id, _source_table, _ingested_at
                # from the transform job — writing directly preserves the original audit trail.
                # coalesce(1): quarantine files are read by humans/monitoring tools — one
                # file per table is easier to inspect than scattered part files.
                write_json_to_s3(invalid_dataframe.coalesce(1), quarantine_path)

            valid_dataframe.unpersist()
            invalid_dataframe.unpersist()

            total_valid += valid_record_count
            total_quarantined += invalid_record_count

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
