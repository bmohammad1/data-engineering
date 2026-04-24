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

import boto3
from awsglue.utils import getResolvedOptions
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

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

# Name of the derived date column added before writing — kept consistent so
# Athena partition projection and downstream Glue jobs can rely on a single name.
_PARTITION_DATE_COLUMN = "partition_date"


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

# Spark 2.3+ requires at least one real field alongside _corrupt_record.
# "tag" is always present in every valid raw envelope so it anchors the schema.
_TRIAGE_SCHEMA = StructType([
    StructField("tag", StringType(), nullable=True),
    StructField(_CORRUPT_RECORD_COL, StringType(), nullable=True),
])


def _detect_corrupt_tag_ids(spark, raw_s3_path: str) -> set[str]:
    """Return the set of tag IDs whose JSON file cannot be parsed at all.

    Reads every file with a minimal two-field schema so Spark populates
    _corrupt_record for any unparseable file. The DataFrame is cached before
    filtering because Spark forbids filtering on _corrupt_record alone without
    materialising the plan first.
    """
    triage_dataframe = (
        spark.read
        .schema(_TRIAGE_SCHEMA)
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", _CORRUPT_RECORD_COL)
        .json(raw_s3_path)
        .withColumn("_file_path", F.col("_metadata.file_path"))
        .cache()
    )

    corrupt_file_rows = (
        triage_dataframe
        .filter(F.col(_CORRUPT_RECORD_COL).isNotNull())
        .select("_file_path")
        .distinct()
        .collect()
    )

    triage_dataframe.unpersist()

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


def _apply_transformations(dataframe: DataFrame, table_name: str, run_id: str) -> DataFrame:
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

    for field in target_schema.fields:
        column_name = field.name
        column_is_a_string = isinstance(field.dataType, StringType)

        if not column_is_a_string:
            continue

        if column_name in _ENUM_COLUMNS:
            # Enum columns must be uppercased for consistent comparisons downstream.
            dataframe = dataframe.withColumn(
                column_name,
                F.upper(F.trim(F.col(column_name))),
            )
        else:
            dataframe = dataframe.withColumn(
                column_name,
                F.trim(F.col(column_name)),
            )

        column_is_nullable = field.nullable
        column_is_not_primary_key = column_name != primary_key_column

        if column_is_nullable and column_is_not_primary_key:
            # Replace empty or null strings with "UNKNOWN" so downstream queries
            # never encounter silent nulls in nullable string columns.
            value_is_empty_or_null = (
                F.col(column_name).isNull() | (F.col(column_name) == "")
            )
            dataframe = dataframe.withColumn(
                column_name,
                F.when(value_is_empty_or_null, F.lit("UNKNOWN")).otherwise(F.col(column_name)),
            )

    return add_audit_columns(dataframe, run_id, table_name)


# ---------------------------------------------------------------------------
# Per-tag count accumulation
# ---------------------------------------------------------------------------


def _accumulate_tag_counts(
    dataframe: DataFrame,
    count_key: str,
    tag_stats: dict[str, dict],
) -> None:
    """Add per-tag row counts from dataframe into tag_stats[tag_id][count_key]."""
    if "TagID" not in dataframe.columns:
        return

    rows_per_tag = dataframe.groupBy("TagID").count().collect()

    for row in rows_per_tag:
        tag_id = row.TagID
        if tag_id:
            tag_stats.setdefault(tag_id, {"extracted": 0, "transformed": 0})
            tag_stats[tag_id][count_key] += row["count"]


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

    glue_ctx, spark, job = create_glue_context(args["JOB_NAME"], args)

    logger.info(
        "Transform job started",
        extra={"run_id": run_id, "raw_bucket": raw_bucket, "cleaned_bucket": cleaned_bucket},
    )

    update_run_transform_status(dynamo_table_name, run_id, STATUS_RUNNING)

    raw_s3_path = f"s3://{raw_bucket}/raw/{run_id}/"

    all_tag_ids: set[str] = set(_discover_tag_ids(raw_bucket, run_id))

    # Detect corrupt JSON envelopes once before the table loop.
    corrupt_tag_ids: set[str] = _detect_corrupt_tag_ids(spark, raw_s3_path)
    if corrupt_tag_ids:
        logger.warning(
            "Corrupt JSON envelopes detected — affected tags will be marked FAILED",
            extra={"run_id": run_id, "corrupt_tag_count": len(corrupt_tag_ids)},
        )

    # tag_stats tracks extracted and transformed row counts per tag across all tables.
    # Structure: { tag_id: { "extracted": int, "transformed": int } }
    tag_stats: dict[str, dict] = {}
    total_records_written = 0

    # Read all raw files once and cache so the per-table loop does not re-scan S3.
    raw_dataframe = spark.read.option("mode", "PERMISSIVE").json(raw_s3_path).cache()

    for table_name in TABLE_SCHEMAS:
        table_start_time = time.perf_counter()
        try:
            extracted_dataframe = _extract_table(raw_dataframe, table_name)

            # Snapshot row counts before null-PK rows are dropped.
            _accumulate_tag_counts(extracted_dataframe, "extracted", tag_stats)

            transformed_dataframe = _apply_transformations(extracted_dataframe, table_name, run_id)

            # Snapshot row counts after null-PK rows are dropped.
            _accumulate_tag_counts(transformed_dataframe, "transformed", tag_stats)

            record_count = transformed_dataframe.count()
            total_records_written += record_count

            cleaned_s3_path = f"s3://{cleaned_bucket}/cleaned/{run_id}/{table_name}/"

            source_column = _PARTITION_SOURCE_COLUMN.get(table_name)
            if source_column is not None:
                # Spark's partitionBy automatically excludes the partition column
                # from the row data inside each Parquet file — it is stored only
                # in the directory name (partition_date=2024-01-15/). The column
                # must remain in the DataFrame so partitionBy can read its value,
                # but it will not appear as an extra column when the file is read back.
                dataframe_with_partition_date = transformed_dataframe.withColumn(
                    _PARTITION_DATE_COLUMN,
                    F.to_date(F.col(source_column)),
                )
                write_parquet_to_s3(
                    dataframe_with_partition_date,
                    cleaned_s3_path,
                    partition_cols=[_PARTITION_DATE_COLUMN],
                )
            else:
                write_parquet_to_s3(transformed_dataframe, cleaned_s3_path)

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
    total_records_dropped = 0

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
        records_extracted = tag_stats.get(tag_id, {}).get("extracted", 0)
        logger.error(
            "Tag transform failed — zero usable records after cast",
            extra={
                "run_id": run_id,
                "tag_id": tag_id,
                "failure_reason": "zero_usable_records",
                "records_extracted": records_extracted,
                "records_transformed": 0,
            },
        )

    # Build the DynamoDB update payloads for all three outcome groups    failed_tag_updates = [
        {"tag_id": tag_id, "status": STATUS_FAILED, "records_extracted": 0,
         "records_dropped": 0, "records_transformed": 0}
        for tag_id in corrupt_tag_ids
    ] + [
        {"tag_id": tag_id, "status": STATUS_FAILED,
         "records_extracted": tag_stats.get(tag_id, {}).get("extracted", 0),
         "records_dropped": tag_stats.get(tag_id, {}).get("extracted", 0),
         "records_transformed": 0}
        for tag_id in zero_record_tags
    ]

    success_tag_updates = []
    for tag_id in tags_with_output:
        tag_stats_entry = tag_stats.get(tag_id, {"extracted": 0, "transformed": 0})
        records_extracted = tag_stats_entry["extracted"]
        records_transformed = tag_stats_entry["transformed"]
        records_dropped = records_extracted - records_transformed
        total_records_dropped += records_dropped
        tags_success.add(tag_id)
        success_tag_updates.append({
            "tag_id": tag_id,
            "status": STATUS_SUCCESS,
            "records_extracted": records_extracted,
            "records_dropped": records_dropped,
            "records_transformed": records_transformed,
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
        records_dropped=total_records_dropped,
        duration_ms=int(total_duration_ms),
    )

    logger.info(
        "Transform job completed",
        extra={
            "run_id": run_id,
            "total_records": total_records_written,
            "total_dropped": total_records_dropped,
            "tags_success": len(tags_success),
            "tags_failed": len(tags_failed),
            "total_duration_ms": total_duration_ms,
        },
    )

    job.commit()


if __name__ == "__main__":
    main()
