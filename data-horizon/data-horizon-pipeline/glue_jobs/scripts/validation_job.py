"""Glue validation job — Child3, step 2 of 2.

Reads cleaned JSON from the cleaned S3 bucket, applies per-table data quality
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

from utils.schema_definitions import TABLE_SCHEMAS
from utils.spark_helpers import (
    add_audit_columns,
    create_glue_context,
    read_json_from_s3,
    write_json_to_s3,
    write_parquet_to_catalog,
)
from utils.validation_rules import apply_validation
from utils.dynamodb_updater import (
    fetch_transform_succeeded_tags,
    update_run_validate_status,
    update_tag_validate_status,
)

from shared.constants import STATUS_FAILED, STATUS_RUNNING, STATUS_SUCCESS
from shared.logger import configure_logging, run_id_ctx

configure_logging()
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------


def main() -> None:
    """Glue job entry point."""
    args = getResolvedOptions(sys.argv, [
        "JOB_NAME",
        "run_id",
        "CLEANED_BUCKET",
        "VALIDATED_BUCKET",
        "QUARANTINE_BUCKET",
        "PIPELINE_STATE_TABLE",
        "GLUE_DATABASE",
    ])

    run_id = args["run_id"]
    cleaned_bucket = args["CLEANED_BUCKET"]
    validated_bucket = args["VALIDATED_BUCKET"]
    quarantine_bucket = args["QUARANTINE_BUCKET"]
    table_name_dynamo = args["PIPELINE_STATE_TABLE"]
    glue_database = args["GLUE_DATABASE"]

    run_id_ctx.set(run_id)
    job_start = time.perf_counter()

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

    update_run_validate_status(table_name_dynamo, run_id, STATUS_RUNNING)

    total_valid = 0
    total_quarantined = 0

    # { tag_id: {"valid": int, "invalid": int} } — accumulated across all tables
    tag_counts: dict[str, dict] = {}
    all_tag_ids: set[str] = fetch_transform_succeeded_tags(table_name_dynamo, run_id)
    logger.info(
        "Tags loaded from DynamoDB",
        extra={"run_id": run_id, "tag_count": len(all_tag_ids)},
    )

    for table_name, schema in TABLE_SCHEMAS.items():
        table_start = time.perf_counter()
        cleaned_path = f"s3://{cleaned_bucket}/cleaned/{run_id}/{table_name}/"

        try:
            df = read_json_from_s3(glue_ctx, cleaned_path, schema)
            valid_df, invalid_df = apply_validation(df, table_name)

            valid_count = valid_df.count()
            invalid_count = invalid_df.count()

            if valid_count > 0:
                catalog_table = f"validated_{table_name}"
                validated_path = f"s3://{validated_bucket}/validated/{run_id}/{table_name}/"
                write_parquet_to_catalog(
                    glue_ctx,
                    add_audit_columns(valid_df, run_id, table_name),
                    glue_database,
                    catalog_table,
                    validated_path,
                )

            if invalid_count > 0:
                quarantine_path = f"s3://{quarantine_bucket}/quarantine/{run_id}/{table_name}/"
                write_json_to_s3(
                    add_audit_columns(invalid_df, run_id, table_name),
                    quarantine_path,
                )

            total_valid += valid_count
            total_quarantined += invalid_count

            if "TagID" in valid_df.columns:
                for row in valid_df.groupBy("TagID").count().collect():
                    tid = row.TagID
                    if tid:
                        tag_counts.setdefault(tid, {"valid": 0, "invalid": 0})
                        tag_counts[tid]["valid"] += row["count"]

            if "TagID" in invalid_df.columns:
                for row in invalid_df.groupBy("TagID").count().collect():
                    tid = row.TagID
                    if tid:
                        tag_counts.setdefault(tid, {"valid": 0, "invalid": 0})
                        tag_counts[tid]["invalid"] += row["count"]

            duration_ms = round((time.perf_counter() - table_start) * 1_000, 2)
            logger.info(
                "Table validated",
                extra={
                    "run_id": run_id,
                    "table": table_name,
                    "valid": valid_count,
                    "invalid": invalid_count,
                    "duration_ms": duration_ms,
                },
            )

        except Exception:
            logger.exception(
                "Failed to validate table",
                extra={"run_id": run_id, "table": table_name},
            )
            raise

    # Per-tag DynamoDB updates
    tags_success = 0
    tags_failed = 0

    for tag_id in all_tag_ids:
        counts = tag_counts.get(tag_id, {"valid": 0, "invalid": 0})
        tag_status = STATUS_SUCCESS if counts["valid"] > 0 else STATUS_FAILED

        if tag_status == STATUS_FAILED:
            tags_failed += 1
            logger.error(
                "Tag validation failed — all records quarantined",
                extra={
                    "run_id": run_id,
                    "tag_id": tag_id,
                    "valid_count": 0,
                    "invalid_count": counts["invalid"],
                },
            )
        else:
            tags_success += 1

        try:
            update_tag_validate_status(
                table_name_dynamo,
                run_id,
                tag_id,
                tag_status,
                valid_count=counts["valid"],
                invalid_count=counts["invalid"],
            )
        except Exception:
            logger.warning(
                "Failed to update DynamoDB for tag — continuing",
                extra={"run_id": run_id, "tag_id": tag_id},
            )

    total_duration_ms = round((time.perf_counter() - job_start) * 1_000, 2)

    update_run_validate_status(
        table_name_dynamo,
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

    if total_valid == 0:
        raise RuntimeError(
            f"All {total_quarantined} records failed validation for run {run_id} — pipeline aborted"
        )

    job.commit()


if __name__ == "__main__":
    main()
