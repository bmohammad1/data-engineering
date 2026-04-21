"""Glue transformation job — Child3, step 1 of 2.

Reads raw TagResponse JSON files from the raw S3 bucket (one file per tag),
flattens each nested domain table into a separate DataFrame, applies data
engineering best-practice transformations, and writes the cleaned output to
the cleaned S3 bucket as newline-delimited JSON (one folder per table).

Invoked by the transformation Step Function state machine. The run_id
argument scopes all reads and writes to the current pipeline run.

Input  → s3://<RAW_BUCKET>/raw/<run_id>/          (one JSON file per tag)
Output → s3://<CLEANED_BUCKET>/cleaned/<run_id>/<table>/
"""

import logging
import os
import sys
import time

from awsglue.utils import getResolvedOptions
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from utils.schema_definitions import LIST_TABLES, PRIMARY_KEYS, TABLE_SCHEMAS
from utils.spark_helpers import add_audit_columns, create_glue_context, write_json_to_s3
from utils.dynamodb_updater import (
    update_run_transform_status,
    update_tag_transform_status,
)

from shared.constants import STATUS_FAILED, STATUS_RUNNING, STATUS_SUCCESS
from shared.logger import configure_logging, run_id_ctx

configure_logging()
logger = logging.getLogger(__name__)

# Enum-like columns that should be upper-cased for consistency
_ENUM_COLUMNS = {"QualityFlag", "Status", "PaymentStatus"}


# ---------------------------------------------------------------------------
# Raw data extraction helpers
# ---------------------------------------------------------------------------


def _discover_tag_ids(raw_bucket: str, run_id: str) -> list[str]:
    """List all tag IDs present in the raw S3 prefix by reading object keys."""
    import boto3

    s3 = boto3.client("s3")
    prefix = f"raw/{run_id}/"
    paginator = s3.get_paginator("list_objects_v2")

    tag_ids = []
    for page in paginator.paginate(Bucket=raw_bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            # Key format: raw/<run_id>/<tag_id>.json
            filename = obj["Key"].split("/")[-1]
            if filename.endswith(".json"):
                tag_ids.append(filename[:-5])  # strip .json

    return tag_ids


def _read_table_from_raw(spark, raw_s3_path: str, table_name: str) -> DataFrame:
    """Extract one domain table from all raw TagResponse JSON files.

    spark.read.json reads every file in the prefix as one row (one row = one
    TagResponse = one tag). We then pull out the column for this table and
    explode list-type tables so that one row = one domain record.
    """
    schema = TABLE_SCHEMAS[table_name]
    is_list = table_name in LIST_TABLES

    # Read the full raw envelope — infer the top-level structure, no strict schema yet.
    # We apply the per-table schema after exploding so Spark can cast correctly.
    raw_df = spark.read.option("mode", "PERMISSIVE").json(raw_s3_path)

    if table_name not in raw_df.columns:
        logger.warning("Column '%s' not found in raw data — returning empty DataFrame", table_name)
        return spark.createDataFrame([], schema)

    if is_list:
        # Explode the array: one row per domain record across all tags
        exploded = (
            raw_df
            .select(F.explode(F.col(table_name)).alias("_record"))
            .select("_record.*")
        )
    else:
        # Single-object tables (tag, equipment, location, customer)
        exploded = raw_df.select(f"{table_name}.*")

    # Select only the columns defined in the schema, casting to declared types.
    # Any injected extra columns from dirty data are silently dropped here.
    select_exprs = []
    for field in schema.fields:
        if field.name in exploded.columns:
            select_exprs.append(F.col(field.name).cast(field.dataType).alias(field.name))
        else:
            select_exprs.append(F.lit(None).cast(field.dataType).alias(field.name))

    return exploded.select(select_exprs)


# ---------------------------------------------------------------------------
# Transformation pipeline
# ---------------------------------------------------------------------------


def _apply_transformations(df: DataFrame, table_name: str, run_id: str) -> DataFrame:
    """Apply all standard transformations to a domain table DataFrame."""
    schema = TABLE_SCHEMAS[table_name]
    primary_key = PRIMARY_KEYS[table_name]

    # 1. Drop rows where the primary key is null — these are unsalvageable
    df = df.filter(F.col(primary_key).isNotNull() & (F.trim(F.col(primary_key)) != ""))

    # 2. Deduplicate by primary key — keep the first occurrence
    df = df.dropDuplicates([primary_key])

    # 3. Normalize strings: trim whitespace on all string columns,
    #    upper-case known enum columns
    for field in schema.fields:
        col_name = field.name
        from pyspark.sql.types import StringType
        if isinstance(field.dataType, StringType):
            if col_name in _ENUM_COLUMNS:
                df = df.withColumn(col_name, F.upper(F.trim(F.col(col_name))))
            else:
                df = df.withColumn(col_name, F.trim(F.col(col_name)))
            # Fill nulls in optional string columns with UNKNOWN sentinel
            if field.nullable and col_name != primary_key:
                df = df.withColumn(
                    col_name,
                    F.when(
                        F.col(col_name).isNull() | (F.col(col_name) == ""),
                        F.lit("UNKNOWN")
                    ).otherwise(F.col(col_name))
                )

    # 4. Audit columns
    df = add_audit_columns(df, run_id, table_name)

    return df


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------


def main() -> None:
    """Glue job entry point."""
    args = getResolvedOptions(sys.argv, [
        "JOB_NAME",
        "run_id",
        "RAW_BUCKET",
        "CLEANED_BUCKET",
        "PIPELINE_STATE_TABLE",
    ])

    run_id = args["run_id"]
    raw_bucket = args["RAW_BUCKET"]
    cleaned_bucket = args["CLEANED_BUCKET"]
    table_name_dynamo = args["PIPELINE_STATE_TABLE"]

    run_id_ctx.set(run_id)
    job_start = time.perf_counter()

    glue_ctx, spark, job = create_glue_context(args["JOB_NAME"], args)

    logger.info(
        "Transform job started",
        extra={"run_id": run_id, "raw_bucket": raw_bucket, "cleaned_bucket": cleaned_bucket},
    )

    update_run_transform_status(table_name_dynamo, run_id, STATUS_RUNNING)

    raw_s3_path = f"s3://{raw_bucket}/raw/{run_id}/"
    total_records = 0

    for table_name in TABLE_SCHEMAS:
        table_start = time.perf_counter()
        try:
            df = _read_table_from_raw(spark, raw_s3_path, table_name)
            df = _apply_transformations(df, table_name, run_id)

            count = df.count()
            cleaned_path = f"s3://{cleaned_bucket}/cleaned/{run_id}/{table_name}/"
            write_json_to_s3(df, cleaned_path)
            total_records += count

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

    # Per-tag DynamoDB updates — discover tag IDs from the raw prefix
    tag_ids = _discover_tag_ids(raw_bucket, run_id)
    for tag_id in tag_ids:
        try:
            update_tag_transform_status(table_name_dynamo, run_id, tag_id, STATUS_SUCCESS)
        except Exception:
            logger.warning(
                "Failed to update DynamoDB for tag — continuing",
                extra={"run_id": run_id, "tag_id": tag_id},
            )

    total_duration_ms = round((time.perf_counter() - job_start) * 1_000, 2)
    update_run_transform_status(
        table_name_dynamo,
        run_id,
        STATUS_SUCCESS,
        records_transformed=total_records,
    )

    logger.info(
        "Transform job completed",
        extra={
            "run_id": run_id,
            "total_records": total_records,
            "total_duration_ms": total_duration_ms,
            "tags_processed": len(tag_ids),
        },
    )

    job.commit()


if __name__ == "__main__":
    main()
