"""Glue transformation job — Child3, step 1 of 2.

Reads raw TagResponse JSON files from the raw S3 bucket (one file per tag),
flattens each nested domain table into a separate DataFrame, applies data
engineering best-practice transformations, and writes the cleaned output to
the cleaned S3 bucket as newline-delimited JSON (one folder per table).

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
    update_run_transform_status,
    update_tag_transform_status,
)

from shared.constants import STATUS_FAILED, STATUS_RUNNING, STATUS_SUCCESS, load_ssm_config
from shared.logger import configure_logging, run_id_ctx

configure_logging()
logger = logging.getLogger(__name__)

_ENUM_COLUMNS = {"QualityFlag", "Status", "PaymentStatus"}


# ---------------------------------------------------------------------------
# Raw data discovery
# ---------------------------------------------------------------------------


def _discover_tag_ids(raw_bucket: str, run_id: str) -> list[str]:
    """List all tag IDs present in the raw S3 prefix by reading object keys."""
    s3 = boto3.client("s3")
    prefix = f"raw/{run_id}/"
    paginator = s3.get_paginator("list_objects_v2")

    tag_ids = []
    for page in paginator.paginate(Bucket=raw_bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            filename = obj["Key"].split("/")[-1]
            if filename.endswith(".json"):
                tag_ids.append(filename[:-5])

    return tag_ids


# ---------------------------------------------------------------------------
# Per-table extraction with corrupt envelope detection
# ---------------------------------------------------------------------------


_CORRUPT_RECORD_COL = "_corrupt_record"
# Must include at least one real field alongside _corrupt_record — Spark 2.3+
# disallows queries whose schema contains only the corrupt-record column.
# "tag" is always present in every valid raw envelope so it serves as the anchor.
_TRIAGE_SCHEMA = StructType([
    StructField("tag", StringType(), nullable=True),
    StructField(_CORRUPT_RECORD_COL, StringType(), nullable=True),
])


def _detect_corrupt_tag_ids(spark, raw_s3_path: str) -> set[str]:
    """Return the set of tag IDs whose JSON file cannot be parsed at all.

    Reads every file in raw_s3_path with a minimal two-field schema so that
    Spark populates _corrupt_record for any unparseable file. The DataFrame is
    cached before filtering because Spark forbids filtering on _corrupt_record
    alone without materialising the plan first.
    """
    triage_df = (
        spark.read
        .schema(_TRIAGE_SCHEMA)
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", _CORRUPT_RECORD_COL)
        .json(raw_s3_path)
        .withColumn("_file_path", F.col("_metadata.file_path"))
        .cache()
    )
    corrupt_rows = (
        triage_df
        .filter(F.col(_CORRUPT_RECORD_COL).isNotNull())
        .select("_file_path")
        .distinct()
        .collect()
    )
    triage_df.unpersist()
    return {
        os.path.basename(row._file_path).replace(".json", "")
        for row in corrupt_rows
        if row._file_path
    }


def _extract_table(raw_df: DataFrame, table_name: str) -> DataFrame:
    """Extract one domain table from the already-loaded raw DataFrame.

    Accepts a pre-read DataFrame so the caller can read S3 once and reuse it
    across all tables. Returns a DataFrame with the domain table's columns cast
    to their target types.
    """
    schema = TABLE_SCHEMAS[table_name]

    if table_name not in raw_df.columns:
        logger.warning(
            "Column not found in raw data — returning empty DataFrame",
            extra={"run_id": None, "table": table_name},
        )
        return raw_df.sparkSession.createDataFrame([], schema)

    if table_name in LIST_TABLES:
        flat_df = raw_df.select(F.explode(F.col(table_name)).alias("_record")).select("_record.*")
    else:
        flat_df = raw_df.select(f"{table_name}.*")

    select_exprs = [
        F.col(f.name).cast(f.dataType).alias(f.name) if f.name in flat_df.columns
        else F.lit(None).cast(f.dataType).alias(f.name)
        for f in schema.fields
    ]
    return flat_df.select(select_exprs)


# ---------------------------------------------------------------------------
# Transformation pipeline
# ---------------------------------------------------------------------------


def _apply_transformations(df: DataFrame, table_name: str, run_id: str) -> DataFrame:
    """Apply all standard transformations to a domain table DataFrame."""
    schema = TABLE_SCHEMAS[table_name]
    primary_key = PRIMARY_KEYS[table_name]

    df = df.filter(F.col(primary_key).isNotNull() & (F.trim(F.col(primary_key)) != ""))
    df = df.dropDuplicates([primary_key])

    for field in schema.fields:
        col_name = field.name
        if isinstance(field.dataType, StringType):
            if col_name in _ENUM_COLUMNS:
                df = df.withColumn(col_name, F.upper(F.trim(F.col(col_name))))
            else:
                df = df.withColumn(col_name, F.trim(F.col(col_name)))
            if field.nullable and col_name != primary_key:
                df = df.withColumn(
                    col_name,
                    F.when(
                        F.col(col_name).isNull() | (F.col(col_name) == ""),
                        F.lit("UNKNOWN"),
                    ).otherwise(F.col(col_name)),
                )

    return add_audit_columns(df, run_id, table_name)


# ---------------------------------------------------------------------------
# Per-tag count accumulation helpers
# ---------------------------------------------------------------------------


def _accumulate_tag_counts(
    df: DataFrame,
    count_key: str,
    tag_stats: dict[str, dict],
) -> None:
    """Add per-tag row counts from df into tag_stats[tag_id][count_key]."""
    if "TagID" not in df.columns:
        return
    for row in df.groupBy("TagID").count().collect():
        tid = row.TagID
        if tid:
            tag_stats.setdefault(tid, {"extracted": 0, "transformed": 0})
            tag_stats[tid][count_key] += row["count"]


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------


def main() -> None:
    """Glue job entry point."""
    args = getResolvedOptions(sys.argv, ["JOB_NAME", "run_id", "ENVIRONMENT"])

    run_id = args["run_id"]
    ssm = load_ssm_config(args["ENVIRONMENT"])
    raw_bucket = ssm["raw-bucket-name"]
    cleaned_bucket = ssm["cleaned-bucket-name"]
    table_name_dynamo = ssm["pipeline-state-table"]

    run_id_ctx.set(run_id)
    job_start = time.perf_counter()

    glue_ctx, spark, job = create_glue_context(args["JOB_NAME"], args)

    logger.info(
        "Transform job started",
        extra={"run_id": run_id, "raw_bucket": raw_bucket, "cleaned_bucket": cleaned_bucket},
    )

    update_run_transform_status(table_name_dynamo, run_id, STATUS_RUNNING)

    raw_s3_path = f"s3://{raw_bucket}/raw/{run_id}/"
    all_tag_ids: set[str] = set(_discover_tag_ids(raw_bucket, run_id))

    # Single triage pass — detect corrupt JSON envelopes across all files once.
    corrupt_tag_ids: set[str] = _detect_corrupt_tag_ids(spark, raw_s3_path)
    if corrupt_tag_ids:
        logger.warning(
            "Corrupt JSON envelopes detected — affected tags will be marked FAILED",
            extra={"run_id": run_id, "corrupt_tag_count": len(corrupt_tag_ids)},
        )

    # Accumulated across all tables:
    # { tag_id: { "extracted": int, "transformed": int } }
    tag_stats: dict[str, dict] = {}
    total_records = 0

    raw_df = spark.read.option("mode", "PERMISSIVE").json(raw_s3_path).cache()

    for table_name in TABLE_SCHEMAS:
        table_start = time.perf_counter()
        try:
            pre_df = _extract_table(raw_df, table_name)

            # Count extracted rows per tag before null-PK drop
            _accumulate_tag_counts(pre_df, "extracted", tag_stats)

            transformed_df = _apply_transformations(pre_df, table_name, run_id)

            # Count transformed rows per tag after null-PK drop
            _accumulate_tag_counts(transformed_df, "transformed", tag_stats)

            count = transformed_df.count()
            total_records += count

            cleaned_path = f"s3://{cleaned_bucket}/cleaned/{run_id}/{table_name}/"
            write_parquet_to_s3(transformed_df, cleaned_path)

            duration_ms = round((time.perf_counter() - table_start) * 1_000, 2)
            logger.info(
                "Table transformed",
                extra={
                    "run_id": run_id,
                    "table": table_name,
                    "records": count,
                    "duration_ms": duration_ms,
                },
            )
        except Exception:
            logger.exception(
                "Failed to transform table",
                extra={"run_id": run_id, "table": table_name},
            )
            raise

    raw_df.unpersist()

    # Tags that had no corrupt envelope but still produced zero transformed rows
    tags_with_output = {tid for tid, s in tag_stats.items() if s["transformed"] > 0}
    zero_record_tags = all_tag_ids - corrupt_tag_ids - tags_with_output

    # Per-tag DynamoDB updates
    tags_success: set[str] = set()
    tags_failed: set[str] = corrupt_tag_ids | zero_record_tags
    total_dropped = 0

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
        try:
            update_tag_transform_status(table_name_dynamo, run_id, tag_id, STATUS_FAILED)
        except Exception:
            logger.warning(
                "Failed to update DynamoDB for corrupt tag — continuing",
                extra={"run_id": run_id, "tag_id": tag_id},
            )

    for tag_id in zero_record_tags:
        extracted = tag_stats.get(tag_id, {}).get("extracted", 0)
        logger.error(
            "Tag transform failed — zero usable records after cast",
            extra={
                "run_id": run_id,
                "tag_id": tag_id,
                "failure_reason": "zero_usable_records",
                "records_extracted": extracted,
                "records_transformed": 0,
            },
        )
        try:
            update_tag_transform_status(
                table_name_dynamo,
                run_id,
                tag_id,
                STATUS_FAILED,
                records_extracted=extracted,
                records_dropped=extracted,
                records_transformed=0,
            )
        except Exception:
            logger.warning(
                "Failed to update DynamoDB for zero-record tag — continuing",
                extra={"run_id": run_id, "tag_id": tag_id},
            )

    for tag_id in tags_with_output:
        stats = tag_stats.get(tag_id, {"extracted": 0, "transformed": 0})
        extracted = stats["extracted"]
        transformed = stats["transformed"]
        dropped = extracted - transformed
        total_dropped += dropped
        tags_success.add(tag_id)
        try:
            update_tag_transform_status(
                table_name_dynamo,
                run_id,
                tag_id,
                STATUS_SUCCESS,
                records_extracted=extracted,
                records_dropped=dropped,
                records_transformed=transformed,
            )
        except Exception:
            logger.warning(
                "Failed to update DynamoDB for successful tag — continuing",
                extra={"run_id": run_id, "tag_id": tag_id},
            )

    total_duration_ms = round((time.perf_counter() - job_start) * 1_000, 2)

    update_run_transform_status(
        table_name_dynamo,
        run_id,
        STATUS_SUCCESS,
        transform_tags_success=len(tags_success),
        transform_tags_failed=len(tags_failed),
        records_transformed=total_records,
        records_dropped=total_dropped,
        duration_ms=int(total_duration_ms),
    )

    logger.info(
        "Transform job completed",
        extra={
            "run_id": run_id,
            "total_records": total_records,
            "total_dropped": total_dropped,
            "tags_success": len(tags_success),
            "tags_failed": len(tags_failed),
            "total_duration_ms": total_duration_ms,
        },
    )

    job.commit()


if __name__ == "__main__":
    main()
