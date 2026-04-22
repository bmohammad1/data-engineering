"""Common Spark / Glue utilities shared by transform and validation jobs."""

import logging

from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

logger = logging.getLogger(__name__)


def create_glue_context(job_name: str, args: dict) -> tuple[GlueContext, SparkSession, Job]:
    """Initialise GlueContext, SparkSession, and commit job bookmark.

    Returns the GlueContext (needed for Data Catalog sink writes),
    the SparkSession, and the Job handle.
    """
    sc = SparkContext.getOrCreate()
    glue_ctx = GlueContext(sc)
    spark = glue_ctx.spark_session

    job = Job(glue_ctx)
    job.init(job_name, args)

    return glue_ctx, spark, job


def read_json_from_s3(
    glue_ctx: GlueContext,
    s3_path: str,
    schema: StructType,
) -> DataFrame:
    """Read JSON files from an S3 prefix via the Glue DynamicFrame API.

    Using create_dynamic_frame.from_options avoids the Spark FileSystem cache
    stale-listing issue that causes 'No such file' errors when reading S3
    paths written moments earlier by the same job session.
    The DynamicFrame is converted to a Spark DataFrame and columns are cast
    to the target schema types so downstream validation rules get correct types.
    """
    dyf = glue_ctx.create_dynamic_frame.from_options(
        connection_type="s3",
        connection_options={"paths": [s3_path], "recurse": True},
        format="json",
    )
    df = dyf.toDF()

    # Cast each column to the declared schema type; add as null if missing.
    select_exprs = []
    for field in schema.fields:
        if field.name in df.columns:
            select_exprs.append(
                F.col(field.name).cast(field.dataType).alias(field.name)
            )
        else:
            select_exprs.append(
                F.lit(None).cast(field.dataType).alias(field.name)
            )
    return df.select(select_exprs)


def write_json_to_s3(df: DataFrame, s3_path: str) -> None:
    """Write a DataFrame to S3 as newline-delimited JSON, overwriting any existing data."""
    (
        df.write
        .mode("overwrite")
        .json(s3_path)
    )


def write_parquet_to_catalog(
    glue_ctx: GlueContext,
    df: DataFrame,
    database: str,
    table_name: str,
    s3_path: str,
) -> None:
    """Write a DataFrame as snappy Parquet and register/update the Glue Data Catalog.

    Using enableUpdateCatalog=True means Athena can query the table
    immediately after the job finishes — no MSCK REPAIR TABLE needed.
    """
    dynamic_frame = glue_ctx.create_dynamic_frame.from_options(
        connection_type="s3",
        connection_options={"paths": []},
        format="parquet",
    )
    # Convert back from Spark DataFrame to DynamicFrame for catalog sink
    from awsglue.dynamicframe import DynamicFrame
    dynamic_frame = DynamicFrame.fromDF(df, glue_ctx, table_name)

    sink = glue_ctx.getSink(
        connection_type="s3",
        path=s3_path,
        enableUpdateCatalog=True,
        updateBehavior="UPDATE_IN_DATABASE",
        partitionKeys=[],
    )
    sink.setFormat("glueparquet", formatOptions={"compression": "snappy"})
    sink.setCatalogInfo(catalogDatabase=database, catalogTableName=table_name)
    sink.writeFrame(dynamic_frame)


def add_audit_columns(df: DataFrame, run_id: str, source_table: str) -> DataFrame:
    """Append pipeline audit columns to a DataFrame."""
    return (
        df
        .withColumn("_run_id",       F.lit(run_id))
        .withColumn("_source_table", F.lit(source_table))
        .withColumn("_ingested_at",  F.current_timestamp())
    )
