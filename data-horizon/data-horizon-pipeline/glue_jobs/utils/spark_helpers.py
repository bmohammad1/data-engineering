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
    # Prevent Hadoop FileOutputCommitter from writing empty _$folder$ marker
    # objects into S3 output prefixes on every write.
    sc._jsc.hadoopConfiguration().set(
        "mapreduce.fileoutputcommitter.marksuccessfuljobs", "false"
    )
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
    dynamic_frame = glue_ctx.create_dynamic_frame.from_options(
        connection_type="s3",
        connection_options={"paths": [s3_path], "recurse": True},
        format="json",
    )
    dataframe = dynamic_frame.toDF()

    cast_column_expressions = []
    for field in schema.fields:
        column_exists_in_data = field.name in dataframe.columns
        if column_exists_in_data:
            cast_expression = F.col(field.name).cast(field.dataType).alias(field.name)
        else:
            cast_expression = F.lit(None).cast(field.dataType).alias(field.name)
        cast_column_expressions.append(cast_expression)

    return dataframe.select(cast_column_expressions)


def read_parquet_from_s3(
    glue_ctx: GlueContext,
    s3_path: str,
    schema: StructType,
) -> DataFrame:
    """Read Parquet files from an S3 prefix via the Glue DynamicFrame API.

    Using create_dynamic_frame.from_options avoids the Spark FileSystem cache
    stale-listing issue that causes 'No such file' errors when reading S3
    paths written moments earlier by the same job session.
    The DynamicFrame is converted to a Spark DataFrame and columns are cast
    to the target schema types so downstream validation rules get correct types.
    """
    dynamic_frame = glue_ctx.create_dynamic_frame.from_options(
        connection_type="s3",
        connection_options={"paths": [s3_path], "recurse": True},
        format="parquet",
    )
    dataframe = dynamic_frame.toDF()

    cast_column_expressions = []
    for field in schema.fields:
        column_exists_in_data = field.name in dataframe.columns
        if column_exists_in_data:
            cast_expression = F.col(field.name).cast(field.dataType).alias(field.name)
        else:
            cast_expression = F.lit(None).cast(field.dataType).alias(field.name)
        cast_column_expressions.append(cast_expression)

    return dataframe.select(cast_column_expressions)


def write_json_to_s3(dataframe: DataFrame, s3_path: str) -> None:
    """Write a DataFrame to S3 as newline-delimited JSON, overwriting any existing data."""
    dataframe.write.mode("overwrite").json(s3_path)


def write_parquet_to_s3(
    dataframe: DataFrame,
    s3_path: str,
    partition_cols: list[str] | None = None,
) -> None:
    """Write a DataFrame to S3 as snappy-compressed Parquet, overwriting any existing data.

    When partition_cols is provided, Spark creates one subdirectory per unique
    combination of partition column values (e.g., partition_date=2024-01-15/).
    Athena and downstream Glue jobs skip irrelevant partitions entirely, so
    queries that filter on those columns only scan the matching subdirectories.
    """
    writer = dataframe.write.mode("overwrite").option("compression", "snappy")

    partition_columns_were_provided = partition_cols is not None and len(partition_cols) > 0
    if partition_columns_were_provided:
        writer = writer.partitionBy(*partition_cols)

    writer.parquet(s3_path)


def write_parquet_to_catalog(
    glue_ctx: GlueContext,
    dataframe: DataFrame,
    database: str,
    table_name: str,
    s3_path: str,
) -> None:
    """Write a DataFrame as snappy Parquet and register/update the Glue Data Catalog.

    Using enableUpdateCatalog=True means Athena can query the table
    immediately after the job finishes — no MSCK REPAIR TABLE needed.
    """
    dynamic_frame = DynamicFrame.fromDF(dataframe, glue_ctx, table_name)

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


def add_audit_columns(dataframe: DataFrame, run_id: str, source_table: str) -> DataFrame:
    """Append pipeline audit columns to a DataFrame."""
    dataframe_with_run_id = dataframe.withColumn("_run_id", F.lit(run_id))
    dataframe_with_source = dataframe_with_run_id.withColumn("_source_table", F.lit(source_table))
    dataframe_with_timestamp = dataframe_with_source.withColumn("_ingested_at", F.current_timestamp())
    return dataframe_with_timestamp
