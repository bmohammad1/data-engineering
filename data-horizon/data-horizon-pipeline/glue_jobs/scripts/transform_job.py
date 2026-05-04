"""Glue transformation job — Child3, step 1 of 2.

Reads raw TagResponse JSON files from the raw S3 bucket (one file per tag),
flattens each nested domain table into a separate DataFrame, applies transformations, and writes the cleaned output to
the cleaned S3 bucket as Parquet (one folder per table).

Fault isolation: a single corrupt tag file does not abort the job. Corrupt
envelopes are detected after PERMISSIVE read and that tag is marked FAILED in
DynamoDB while all other tags continue processing.

Input  → s3://<RAW_BUCKET>/raw/<run_id>/          (one JSON file per tag)
Output → s3://<CLEANED_BUCKET>/cleaned/<run_id>/<table>/
"""

import logging
import os
import sys
import time
from datetime import datetime, timezone

import boto3
from awsglue.utils import getResolvedOptions
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from utils.schema_definitions import LIST_TABLES, PRIMARY_KEYS, TABLE_SCHEMAS
from utils.spark_helpers import add_audit_columns, create_glue_context, write_parquet_to_s3
from utils.dynamodb_updater import (
    bulk_update_tag_transform_status,
    update_run_transform_status,
)

from shared.constants import STATUS_FAILED, STATUS_RUNNING, STATUS_SUCCESS, load_ssm_config
from shared.logger import configure_logging, run_id_ctx

configure_logging()
logger = logging.getLogger(__name__)

# Columns whose string values must be uppercased — they represent fixed enum-like states.
_ENUM_COLUMNS = {"QualityFlag", "Status", "PaymentStatus"}


# ---------------------------------------------------------------------------
# Raw data discovery
# ---------------------------------------------------------------------------


def _discover_tag_ids(raw_bucket: str, run_id: str) -> list[str]:
    """List all tag IDs present in the raw S3 prefix by reading object keys."""
    s3_client = boto3.client("s3")
    raw_prefix = f"raw/{run_id}/"
    paginator = s3_client.get_paginator("list_objects_v2")

    tag_ids = []
    for page in paginator.paginate(Bucket=raw_bucket, Prefix=raw_prefix):
        for s3_object in page.get("Contents", []):
            filename = s3_object["Key"].split("/")[-1]
            if filename.endswith(".json"):
                tag_id = filename[:-5]
                tag_ids.append(tag_id)

    return tag_ids


# ---------------------------------------------------------------------------
# Corrupt envelope detection
# ---------------------------------------------------------------------------

_CORRUPT_RECORD_COL = "_corrupt_record"


def _extract_corrupt_tag_ids(raw_dataframe) -> set[str]:
    """Return tag IDs whose JSON file failed to parse, using the already-cached raw DataFrame.

    The raw DataFrame must have been read with PERMISSIVE mode and
    columnNameOfCorruptRecord set to _CORRUPT_RECORD_COL so that unparseable
    rows carry a non-null value in that column.
    """
    corrupt_file_rows = (
        raw_dataframe
        .filter(F.col(_CORRUPT_RECORD_COL).isNotNull())
        .select("_file_path")
        .collect()
    )

    corrupt_tag_ids = set()
    for row in corrupt_file_rows:
        if row._file_path:
            filename = os.path.basename(row._file_path)
            tag_id = filename.replace(".json", "")
            corrupt_tag_ids.add(tag_id)

    return corrupt_tag_ids


# ---------------------------------------------------------------------------
# Per-table extraction
# ---------------------------------------------------------------------------


def _extract_table(raw_dataframe: DataFrame, table_name: str) -> DataFrame:
    """Extract one domain table from the already-loaded raw DataFrame.

    Accepts a pre-read DataFrame so the caller can read S3 once and reuse it
    across all tables. Returns a DataFrame with the domain table's columns cast
    to their target types.
    """
    target_schema = TABLE_SCHEMAS[table_name]

    table_column_is_missing = table_name not in raw_dataframe.columns
    if table_column_is_missing:
        logger.warning(
            "Column not found in raw data — returning empty DataFrame",
            extra={"run_id": None, "table": table_name},
        )
        return raw_dataframe.sparkSession.createDataFrame([], target_schema)

    if table_name in LIST_TABLES:
        # The table column holds an array of structs — one element per record.
        # explode() turns each array element into its own row, then .* unpacks
        # the struct fields into top-level columns.
        exploded_records = raw_dataframe.select(
            F.explode(F.col(table_name)).alias("_record")
        )
        flat_dataframe = exploded_records.select("_record.*")
    else:
        # The table column holds a single struct — expand its fields directly.
        flat_dataframe = raw_dataframe.select(f"{table_name}.*")

    cast_column_expressions = []
    for field in target_schema.fields:
        column_exists_in_data = field.name in flat_dataframe.columns
        if column_exists_in_data:
            cast_expression = F.col(field.name).cast(field.dataType).alias(field.name)
        else:
            cast_expression = F.lit(None).cast(field.dataType).alias(field.name)
        cast_column_expressions.append(cast_expression)

    return flat_dataframe.select(cast_column_expressions)


# ---------------------------------------------------------------------------
# Transformation pipeline
# ---------------------------------------------------------------------------


def _apply_transformations(dataframe: DataFrame, table_name: str, run_id: str, ingested_at: str) -> DataFrame:
    """Apply all standard transformations to a domain table DataFrame."""
    target_schema = TABLE_SCHEMAS[table_name]
    primary_key_column = PRIMARY_KEYS[table_name]

    # Drop rows where the primary key is null or empty — they are unidentifiable.
    primary_key_is_present = (
        F.col(primary_key_column).isNotNull()
        & (F.trim(F.col(primary_key_column)) != "")
    )
    dataframe = dataframe.filter(primary_key_is_present)

    # Keep only the first occurrence of each primary key value.
    dataframe = dataframe.dropDuplicates([primary_key_column])

    # Build all column expressions in one pass and emit a single select so Spark
    column_expressions = []
    for field in target_schema.fields:
        col_expr = F.col(field.name)

        if isinstance(field.dataType, StringType):
            if field.name in _ENUM_COLUMNS:
                col_expr = F.upper(F.trim(col_expr))
            else:
                col_expr = F.trim(col_expr)

            if field.nullable and field.name != primary_key_column:
                col_expr = F.when(
                    col_expr.isNull() | (col_expr == ""), F.lit("UNKNOWN")
                ).otherwise(col_expr)

        column_expressions.append(col_expr.alias(field.name))

    dataframe = dataframe.select(column_expressions)
    return add_audit_columns(dataframe, run_id, table_name, ingested_at)


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------


def main() -> None:
    """Glue job entry point."""
    args = getResolvedOptions(sys.argv, ["JOB_NAME", "run_id", "ENVIRONMENT"])

    run_id = args["run_id"]
    ssm_config = load_ssm_config(args["ENVIRONMENT"])
    raw_bucket = ssm_config["raw-bucket-name"]
    cleaned_bucket = ssm_config["cleaned-bucket-name"]
    dynamo_table_name = ssm_config["pipeline-state-table"]

    run_id_ctx.set(run_id)
    job_start_time = time.perf_counter()
    ingested_at = datetime.now(timezone.utc).isoformat()

    glue_ctx, spark, job = create_glue_context(args["JOB_NAME"], args)

    logger.info(
        "Transform job started",
        extra={"run_id": run_id, "raw_bucket": raw_bucket, "cleaned_bucket": cleaned_bucket},
    )

    update_run_transform_status(dynamo_table_name, run_id, STATUS_RUNNING)

    raw_s3_path = f"s3://{raw_bucket}/raw/{run_id}/"

    all_tag_ids: set[str] = set(_discover_tag_ids(raw_bucket, run_id))

    tag_stats: dict[str, dict] = {}
    total_records_written = 0

    # Single S3 read with PERMISSIVE mode — _corrupt_record captures unparseable rows
    # and _file_path maps each row back to its source file for corrupt tag detection.
    # Both columns are dropped before the table loop so they don't leak into extractions.
    triage_dataframe = (
        spark.read
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", _CORRUPT_RECORD_COL)
        .json(raw_s3_path)
        .withColumn("_file_path", F.col("_metadata.file_path"))
        .cache()
    )

    corrupt_tag_ids: set[str] = _extract_corrupt_tag_ids(triage_dataframe)
    if corrupt_tag_ids:
        logger.warning(
            "Corrupt JSON envelopes detected — affected tags will be marked FAILED",
            extra={"run_id": run_id, "corrupt_tag_count": len(corrupt_tag_ids)},
        )

    # Drop the triage columns and cache the result — this is the DataFrame reused
    # across all 13 table extractions. Caching here (after drop) ensures every
    # table reads from the same cached plan node rather than recomputing from S3.
    raw_dataframe = triage_dataframe.drop(_CORRUPT_RECORD_COL, "_file_path").cache()
    triage_dataframe.unpersist()

    for table_name in TABLE_SCHEMAS:
        table_start_time = time.perf_counter()
        try:
            extracted_dataframe = _extract_table(raw_dataframe, table_name)
            transformed_dataframe = _apply_transformations(extracted_dataframe, table_name, run_id, ingested_at).cache()

            cleaned_s3_path = f"s3://{cleaned_bucket}/cleaned/{run_id}/{table_name}/"

            write_parquet_to_s3(transformed_dataframe, cleaned_s3_path)

            if "TagID" in transformed_dataframe.columns:
                # groupBy reads from the in-memory cache materialized by the write
                # above — no S3 re-read. One shuffle per table but over cached data.
                for row in transformed_dataframe.groupBy("TagID").count().collect():
                    if row.TagID:
                        entry = tag_stats.setdefault(row.TagID, {"extracted": 0, "transformed": 0})
                        entry["extracted"] += row["count"]
                        entry["transformed"] += row["count"]
                record_count = sum(s["transformed"] for s in tag_stats.values())
            else:
                record_count = transformed_dataframe.count()
            total_records_written += record_count

            transformed_dataframe.unpersist()

            table_duration_ms = round((time.perf_counter() - table_start_time) * 1_000, 2)
            logger.info(
                "Table transformed",
                extra={
                    "run_id": run_id,
                    "table": table_name,
                    "records": record_count,
                    "duration_ms": table_duration_ms,
                },
            )
        except Exception:
            logger.exception(
                "Failed to transform table",
                extra={"run_id": run_id, "table": table_name},
            )
            raise

    # All table extractions are done — release the cached raw DataFrame from memory.
    raw_dataframe.unpersist()

    # Classify every tag into one of three outcomes:
    # 1. tags_with_output   — produced at least one transformed row
    # 2. corrupt_tag_ids    — JSON file could not be parsed at all
    # 3. zero_record_tags   — valid JSON but all rows were dropped during transformation
    tags_with_output = {tag_id for tag_id, stats in tag_stats.items() if stats["transformed"] > 0}
    zero_record_tags = all_tag_ids - corrupt_tag_ids - tags_with_output

    tags_success: set[str] = set()
    tags_failed: set[str] = corrupt_tag_ids | zero_record_tags

    # Log each failure outcome individually — logging is cheap and preserves
    # per-tag visibility in CloudWatch without adding DynamoDB call overhead.
    for tag_id in corrupt_tag_ids:
        logger.error(
            "Tag transform failed — corrupt JSON envelope",
            extra={
                "run_id": run_id,
                "tag_id": tag_id,
                "failure_reason": "corrupt_json_envelope",
                "records_extracted": 0,
                "records_transformed": 0,
            },
        )

    for tag_id in zero_record_tags:
        logger.error(
            "Tag transform failed — zero usable records after cast",
            extra={
                "run_id": run_id,
                "tag_id": tag_id,
                "failure_reason": "zero_usable_records",
                "records_transformed": 0,
            },
        )

    # Build the DynamoDB update payloads for all three outcome groups.
    failed_tag_updates = [
        {"tag_id": tag_id, "status": STATUS_FAILED,
         "records_extracted": 0, "records_dropped": 0, "records_transformed": 0}
        for tag_id in corrupt_tag_ids
    ] + [
        {"tag_id": tag_id, "status": STATUS_FAILED,
         "records_extracted": 0, "records_dropped": 0, "records_transformed": 0}
        for tag_id in zero_record_tags
    ]

    success_tag_updates = []
    for tag_id in tags_with_output:
        stats = tag_stats.get(tag_id, {"extracted": 0, "transformed": 0})
        tags_success.add(tag_id)
        success_tag_updates.append({
            "tag_id": tag_id,
            "status": STATUS_SUCCESS,
            "records_extracted": stats["extracted"],
            "records_dropped": 0,
            "records_transformed": stats["transformed"],
        })

    bulk_update_tag_transform_status(dynamo_table_name, run_id, failed_tag_updates)
    bulk_update_tag_transform_status(dynamo_table_name, run_id, success_tag_updates)

    total_duration_ms = round((time.perf_counter() - job_start_time) * 1_000, 2)

    update_run_transform_status(
        dynamo_table_name,
        run_id,
        STATUS_SUCCESS,
        transform_tags_success=len(tags_success),
        transform_tags_failed=len(tags_failed),
        records_transformed=total_records_written,
        records_dropped=0,
        duration_ms=int(total_duration_ms),
    )

    logger.info(
        "Transform job completed",
        extra={
            "run_id": run_id,
            "total_records": total_records_written,
            "tags_success": len(tags_success),
            "tags_failed": len(tags_failed),
            "total_duration_ms": total_duration_ms,
        },
    )

    job.commit()


if __name__ == "__main__":
    main()
