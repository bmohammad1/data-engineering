"""Tests for transform_job.py — fault isolation and per-tag record counts."""

import json
from unittest.mock import MagicMock, patch

import boto3
import pytest

from glue_jobs.tests.conftest import (
    CLEANED_BUCKET,
    RAW_BUCKET,
    RUN_ID,
    TABLE_NAME,
)
from shared.constants import PK_RUN_PREFIX, SK_META, SK_TAG_PREFIX, STATUS_FAILED, STATUS_SUCCESS


# ---------------------------------------------------------------------------
# Sample raw TagResponse payloads
# ---------------------------------------------------------------------------

def _valid_tag_payload(tag_id: str, measurement_count: int = 2) -> bytes:
    """Minimal valid TagResponse JSON for a single tag."""
    payload = {
        "tag": {
            "TagID": tag_id,
            "TagName": f"Tag {tag_id}",
            "Description": None,
            "UnitOfMeasure": "kPa",
            "EquipmentID": "EQ-001",
            "LocationID": "LOC-001",
        },
        "equipment": {
            "EquipmentID": "EQ-001",
            "EquipmentName": "Pump A",
            "EquipmentType": "Pump",
            "Manufacturer": "Acme",
            "InstallDate": None,
        },
        "location": {
            "LocationID": "LOC-001",
            "SiteName": "Site A",
            "Area": "Zone 1",
            "GPSCoordinates": "51.5074,-0.1278",
        },
        "customer": {
            "CustomerID": "CUST-001",
            "CustomerName": "Acme Corp",
            "Industry": "Oil & Gas",
            "ContactInfo": "contact@acme.com",
            "Region": "EU",
        },
        "measurements": [
            {
                "MeasurementID": f"M-{tag_id}-{i:04d}",
                "TagID": tag_id,
                "Timestamp": "2024-01-01T12:00:00",
                "Value": 42.5 + i,
                "QualityFlag": "GOOD",
            }
            for i in range(measurement_count)
        ],
        "alarms": [],
        "maintenance": [],
        "events": [],
        "contracts": [],
        "billing": [],
        "inventory": [],
        "regulatory_compliance": [],
        "financial_forecasts": [],
    }
    return json.dumps(payload).encode("utf-8")


def _null_pk_tag_payload(tag_id: str) -> bytes:
    """Tag payload where all primary keys are null across every domain table."""
    payload = {
        "tag": {"TagID": None, "TagName": None, "Description": None, "UnitOfMeasure": None, "EquipmentID": None, "LocationID": None},
        "equipment": {"EquipmentID": None, "EquipmentName": None, "EquipmentType": None, "Manufacturer": None, "InstallDate": None},
        "location": {"LocationID": None, "SiteName": None, "Area": None, "GPSCoordinates": None},
        "customer": {"CustomerID": None, "CustomerName": None, "Industry": None, "ContactInfo": None, "Region": None},
        "measurements": [
            {"MeasurementID": None, "TagID": tag_id, "Timestamp": "2024-01-01T12:00:00", "Value": 10.0, "QualityFlag": "GOOD"}
        ],
        "alarms": [],
        "maintenance": [],
        "events": [],
        "contracts": [],
        "billing": [],
        "inventory": [],
        "regulatory_compliance": [],
        "financial_forecasts": [],
    }
    return json.dumps(payload).encode("utf-8")


# ---------------------------------------------------------------------------
# Helper: upload tag files to moto S3
# ---------------------------------------------------------------------------

def _upload_tag(s3_client, tag_id: str, body: bytes) -> None:
    s3_client.put_object(
        Bucket=RAW_BUCKET,
        Key=f"raw/{RUN_ID}/{tag_id}.json",
        Body=body,
    )


def _seed_tag_item(dynamodb_client, tag_id: str) -> None:
    """Pre-create a TAG item with an empty stage_status map.

    DynamoDB cannot SET a nested attribute path (stage_status.#TRANSFORM)
    if the parent map does not already exist in the item. Seeding avoids
    a ValidationException from the updater during tests.
    """
    dynamodb_client.put_item(
        TableName=TABLE_NAME,
        Item={
            "PK": {"S": f"{PK_RUN_PREFIX}{RUN_ID}"},
            "SK": {"S": f"{SK_TAG_PREFIX}{tag_id}"},
            "stage_status": {"M": {}},
        },
    )


def _seed_meta_item(dynamodb_client) -> None:
    """Pre-create the RUN META item so update_run_transform_status can write to it."""
    dynamodb_client.put_item(
        TableName=TABLE_NAME,
        Item={
            "PK": {"S": f"{PK_RUN_PREFIX}{RUN_ID}"},
            "SK": {"S": SK_META},
        },
    )


def _get_tag_item(dynamodb_client, tag_id: str) -> dict:
    resp = dynamodb_client.get_item(
        TableName=TABLE_NAME,
        Key={
            "PK": {"S": f"{PK_RUN_PREFIX}{RUN_ID}"},
            "SK": {"S": f"{SK_TAG_PREFIX}{tag_id}"},
        },
    )
    return resp.get("Item", {})


# ---------------------------------------------------------------------------
# Glue / S3 mocking helpers
# ---------------------------------------------------------------------------

GLUE_ARGS = {
    "JOB_NAME": "test-transform-job",
    "run_id": RUN_ID,
    "ENVIRONMENT": "test",
}

SSM_CONFIG = {
    "raw-bucket-name": RAW_BUCKET,
    "cleaned-bucket-name": CLEANED_BUCKET,
    "pipeline-state-table": TABLE_NAME,
}

# Full TagResponse schema for from_json — mirrors the raw API structure.
# All array tables use a minimal struct so unexploded columns don't need full
# field definitions (only measurements is exercised in these tests).
def _tag_response_schema():
    from pyspark.sql.types import (
        ArrayType, DoubleType, StringType, StructField, StructType,
    )
    _stub_array = ArrayType(StructType([StructField("_stub", StringType(), True)]))
    return StructType([
        StructField("tag", StructType([
            StructField("TagID",         StringType(), True),
            StructField("TagName",       StringType(), True),
            StructField("Description",   StringType(), True),
            StructField("UnitOfMeasure", StringType(), True),
            StructField("EquipmentID",   StringType(), True),
            StructField("LocationID",    StringType(), True),
        ]), True),
        StructField("equipment", StructType([
            StructField("EquipmentID",   StringType(), True),
            StructField("EquipmentName", StringType(), True),
            StructField("EquipmentType", StringType(), True),
            StructField("Manufacturer",  StringType(), True),
            StructField("InstallDate",   StringType(), True),
        ]), True),
        StructField("location", StructType([
            StructField("LocationID",     StringType(), True),
            StructField("SiteName",       StringType(), True),
            StructField("Area",           StringType(), True),
            StructField("GPSCoordinates", StringType(), True),
        ]), True),
        StructField("customer", StructType([
            StructField("CustomerID",   StringType(), True),
            StructField("CustomerName", StringType(), True),
            StructField("Industry",     StringType(), True),
            StructField("ContactInfo",  StringType(), True),
            StructField("Region",       StringType(), True),
        ]), True),
        StructField("measurements", ArrayType(StructType([
            StructField("MeasurementID", StringType(), True),
            StructField("TagID",         StringType(), True),
            StructField("Timestamp",     StringType(), True),
            StructField("Value",         DoubleType(), True),
            StructField("QualityFlag",   StringType(), True),
        ])), True),
        StructField("alarms",                _stub_array, True),
        StructField("maintenance",           _stub_array, True),
        StructField("events",                _stub_array, True),
        StructField("contracts",             _stub_array, True),
        StructField("billing",               _stub_array, True),
        StructField("inventory",             _stub_array, True),
        StructField("regulatory_compliance", _stub_array, True),
        StructField("financial_forecasts",   _stub_array, True),
    ])


def _build_raw_df(spark, tag_payloads: dict[str, bytes]):
    """Build a triage DataFrame from tag payloads without touching the filesystem.

    Uses spark.sql() + from_json() so no s3:// URI or local path ever reaches
    Spark's Hadoop FileSystem layer. Each tag becomes one row; corrupt payloads
    produce a row where _corrupt_record is non-null (mirroring PERMISSIVE mode).

    The returned DataFrame includes _corrupt_record, _file_path, and a synthetic
    _metadata struct so the transform_job's .withColumn("_file_path", ...) works.
    """
    from pyspark.sql import functions as F

    schema = _tag_response_schema()
    parts = []

    for tag_id, body in tag_payloads.items():
        raw_str = body.decode("utf-8", errors="replace")
        file_path = f"{tag_id}.json"

        try:
            json.loads(raw_str)
            is_corrupt = False
        except (json.JSONDecodeError, ValueError):
            is_corrupt = True

        safe_fp = file_path.replace("'", "''")

        if is_corrupt:
            safe_corrupt = raw_str[:200].replace("'", "''")
            row_sql = (
                f"SELECT CAST(NULL AS STRING) AS _raw,"
                f" '{safe_fp}' AS _fp,"
                f" '{safe_corrupt}' AS _corrupt_record"
            )
        else:
            safe_json = raw_str.replace("'", "''")
            row_sql = (
                f"SELECT '{safe_json}' AS _raw,"
                f" '{safe_fp}' AS _fp,"
                f" CAST(NULL AS STRING) AS _corrupt_record"
            )

        df = (
            spark.sql(row_sql)
            .select(
                F.from_json(F.col("_raw"), schema).alias("_d"),
                F.col("_fp"),
                F.col("_corrupt_record"),
            )
            .select("_d.*", "_fp", "_corrupt_record")
            # Add _metadata struct so .withColumn("_file_path", F.col("_metadata.file_path"))
            # in transform_job.main() resolves without error.
            .withColumn("_metadata", F.struct(F.col("_fp").alias("file_path")))
            .drop("_fp")
        )
        parts.append(df)

    result = parts[0]
    for df in parts[1:]:
        result = result.unionByName(df)
    return result


def _patch_glue(spark, mock_glue_job, tag_payloads: dict[str, bytes]):
    """Return patches for all Glue-specific entry points.

    spark.read is replaced with a mock whose .json() returns an in-memory
    DataFrame built from tag_payloads — no filesystem access occurs.
    write_parquet_to_s3 is mocked to a no-op (S3 writes not under test).
    """
    glue_ctx, job = mock_glue_job

    raw_df = _build_raw_df(spark, tag_payloads)

    class _MockReader:
        def option(self, key, value):
            return self

        def json(self, path):
            return raw_df

    mock_spark = MagicMock(wraps=spark)
    mock_spark.read = _MockReader()

    return [
        patch("glue_jobs.scripts.transform_job.getResolvedOptions", return_value=GLUE_ARGS),
        patch("glue_jobs.scripts.transform_job.create_glue_context", return_value=(glue_ctx, mock_spark, job)),
        patch("glue_jobs.scripts.transform_job.load_ssm_config", return_value=SSM_CONFIG),
        patch("glue_jobs.scripts.transform_job.write_parquet_to_s3", return_value=None),
    ]


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestTransformFaultIsolation:
    def test_all_tags_succeed(self, s3, dynamodb_table, spark, mock_glue_job):
        for tag_id in ("TAG-001", "TAG-002"):
            _seed_tag_item(dynamodb_table, tag_id)
        _seed_meta_item(dynamodb_table)

        payloads = {
            "TAG-001": _valid_tag_payload("TAG-001", measurement_count=3),
            "TAG-002": _valid_tag_payload("TAG-002", measurement_count=2),
        }
        for tag_id, body in payloads.items():
            _upload_tag(s3, tag_id, body)

        patches = _patch_glue(spark, mock_glue_job, payloads)
        with patches[0], patches[1], patches[2], patches[3]:
            from glue_jobs.scripts import transform_job
            transform_job.main()

        item_001 = _get_tag_item(dynamodb_table, "TAG-001")
        item_002 = _get_tag_item(dynamodb_table, "TAG-002")

        assert item_001["stage_status"]["M"]["TRANSFORM"]["S"] == STATUS_SUCCESS
        assert item_002["stage_status"]["M"]["TRANSFORM"]["S"] == STATUS_SUCCESS
        assert int(item_001["transform_records_written"]["N"]) > 0
        assert int(item_002["transform_records_written"]["N"]) > 0

    def test_corrupt_tag_isolated_others_succeed(self, s3, dynamodb_table, spark, mock_glue_job):
        for tag_id in ("TAG-001", "TAG-BAD"):
            _seed_tag_item(dynamodb_table, tag_id)
        _seed_meta_item(dynamodb_table)

        payloads = {
            "TAG-001": _valid_tag_payload("TAG-001", measurement_count=2),
            "TAG-BAD": b"{ this is not valid json !!!",
        }
        for tag_id, body in payloads.items():
            _upload_tag(s3, tag_id, body)

        patches = _patch_glue(spark, mock_glue_job, payloads)
        with patches[0], patches[1], patches[2], patches[3]:
            from glue_jobs.scripts import transform_job
            transform_job.main()  # must NOT raise even though TAG-BAD is corrupt

        bad_item = _get_tag_item(dynamodb_table, "TAG-BAD")
        good_item = _get_tag_item(dynamodb_table, "TAG-001")

        assert bad_item["stage_status"]["M"]["TRANSFORM"]["S"] == STATUS_FAILED
        assert int(bad_item.get("transform_records_written", {}).get("N", "0")) == 0
        assert good_item["stage_status"]["M"]["TRANSFORM"]["S"] == STATUS_SUCCESS

    def test_zero_usable_records_tag_marked_failed(self, s3, dynamodb_table, spark, mock_glue_job):
        _seed_tag_item(dynamodb_table, "TAG-ZERO")
        _seed_meta_item(dynamodb_table)

        payloads = {"TAG-ZERO": _null_pk_tag_payload("TAG-ZERO")}
        _upload_tag(s3, "TAG-ZERO", payloads["TAG-ZERO"])

        patches = _patch_glue(spark, mock_glue_job, payloads)
        with patches[0], patches[1], patches[2], patches[3]:
            from glue_jobs.scripts import transform_job
            transform_job.main()

        item = _get_tag_item(dynamodb_table, "TAG-ZERO")
        assert item["stage_status"]["M"]["TRANSFORM"]["S"] == STATUS_FAILED
        assert int(item["transform_records_written"]["N"]) == 0
