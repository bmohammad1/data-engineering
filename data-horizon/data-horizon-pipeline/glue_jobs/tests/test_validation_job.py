"""Tests for validation_job.py — routing and per-tag DynamoDB status updates."""

import json
from unittest.mock import ANY, MagicMock, patch

import boto3
import pytest

from glue_jobs.tests.conftest import (
    CLEANED_BUCKET,
    GLUE_DATABASE,
    QUARANTINE_BUCKET,
    RUN_ID,
    TABLE_NAME,
    VALIDATED_BUCKET,
)
from shared.constants import PK_RUN_PREFIX, SK_META, SK_TAG_PREFIX, STATUS_FAILED, STATUS_SUCCESS


# ---------------------------------------------------------------------------
# Cleaned record helpers
# ---------------------------------------------------------------------------

def _valid_measurement(mid: str, tag_id: str) -> dict:
    return {
        "MeasurementID": mid,
        "TagID": tag_id,
        "Timestamp": "2024-01-01T12:00:00.000Z",
        "Value": 42.5,
        "QualityFlag": "GOOD",
    }


def _invalid_measurement(mid: str, tag_id: str) -> dict:
    return {
        "MeasurementID": mid,
        "TagID": tag_id,
        "Timestamp": "2024-01-01T12:00:00.000Z",
        "Value": None,          # violates value_not_null
        "QualityFlag": "CORRUPTED",  # violates quality_flag_valid
    }


def _seed_tag_item(dynamodb_client, tag_id: str, overall_status: str = STATUS_SUCCESS) -> None:
    """Pre-create a TAG item with an empty stage_status map and the given overall_status.

    fetch_transform_succeeded_tags filters on overall_status = 'SUCCESS', so
    each tag the validation job should process must be seeded with SUCCESS here.
    The stage_status map must also be initialised so SET stage_status.#VALIDATE
    does not raise a ValidationException.
    """
    dynamodb_client.put_item(
        TableName=TABLE_NAME,
        Item={
            "PK": {"S": f"{PK_RUN_PREFIX}{RUN_ID}"},
            "SK": {"S": f"{SK_TAG_PREFIX}{tag_id}"},
            "overall_status": {"S": overall_status},
            "stage_status": {"M": {}},
        },
    )


def _seed_meta_item(dynamodb_client) -> None:
    """Pre-create the RUN META item so update_run_validate_status can write to it."""
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
# Glue mocking helpers
# ---------------------------------------------------------------------------

GLUE_ARGS = {
    "JOB_NAME": "test-validation-job",
    "run_id": RUN_ID,
    "ENVIRONMENT": "test",
}

SSM_CONFIG = {
    "cleaned-bucket-name": CLEANED_BUCKET,
    "validated-bucket-name": VALIDATED_BUCKET,
    "quarantine-bucket-name": QUARANTINE_BUCKET,
    "pipeline-state-table": TABLE_NAME,
    "glue-database": GLUE_DATABASE,
}


def _build_measurements_df(spark, records: list[dict]):
    """Build a measurements DataFrame in-memory using spark.sql() + from_json().

    Avoids any filesystem access — no s3:// URI or local path is passed to Spark.
    Each record is serialised to a JSON string and parsed via from_json() so
    Spark sees typed structs rather than raw Python data (which would require
    cloudpickle serialisation, unsupported on Python 3.14).
    """
    from pyspark.sql import functions as F
    from pyspark.sql.types import DoubleType, StringType, StructField, StructType

    measurements_schema = StructType([
        StructField("MeasurementID", StringType(), True),
        StructField("TagID",         StringType(), True),
        StructField("Timestamp",     StringType(), True),
        StructField("Value",         DoubleType(),  True),
        StructField("QualityFlag",   StringType(), True),
    ])

    if not records:
        return spark.sql(
            "SELECT CAST(NULL AS STRING) AS MeasurementID,"
            " CAST(NULL AS STRING) AS TagID,"
            " CAST(NULL AS STRING) AS Timestamp,"
            " CAST(NULL AS DOUBLE) AS Value,"
            " CAST(NULL AS STRING) AS QualityFlag"
            " WHERE 1=0"
        )

    parts = []
    for record in records:
        safe_json = json.dumps(record).replace("'", "''")
        df = (
            spark.sql(f"SELECT '{safe_json}' AS _raw")
            .select(F.from_json(F.col("_raw"), measurements_schema).alias("_d"))
            .select("_d.*")
        )
        parts.append(df)

    result = parts[0]
    for df in parts[1:]:
        result = result.unionByName(df)
    return result


def _make_in_memory_reader(spark, table_records: dict[str, list[dict]]):
    """Return a replacement for read_parquet_from_s3 that builds DataFrames in-memory.

    table_records maps table_name → list of record dicts. Tables not present in
    the dict return an empty DataFrame matching the requested schema. The s3_path
    argument encodes the table name as the last non-empty path segment, which is
    used to look up records.

    No s3:// URI or local filesystem path ever reaches Spark.
    """
    def _read(glue_ctx, s3_path: str, tbl_schema):
        segments = [p for p in s3_path.rstrip("/").split("/") if p]
        table_name = segments[-1] if segments else ""

        if table_name == "measurements" and table_name in table_records:
            return _build_measurements_df(spark, table_records[table_name])

        # All other tables return an empty DataFrame — they are not exercised
        # in these tests so returning empty is correct and safe.
        return spark.sql(
            " UNION ALL ".join(
                f"SELECT {', '.join(f'CAST(NULL AS STRING) AS {f.name}' for f in tbl_schema.fields)}"
                " WHERE 1=0"
                for _ in [1]  # single-element loop to build one SELECT … WHERE 1=0
            )
        )

    return _read


def _patch_glue(spark, mock_glue_job, table_records: dict[str, list[dict]]):
    """Return a list of context managers patching all Glue-specific entry points.

    read_parquet_from_s3 is replaced with an in-memory reader so no s3:// URI
    ever reaches Spark. write_parquet_to_catalog and write_json_to_s3 are mocked
    to no-ops — S3/catalog writes are not under test here.
    """
    glue_ctx, job = mock_glue_job

    return [
        patch("glue_jobs.scripts.validation_job.getResolvedOptions", return_value=GLUE_ARGS),
        patch("glue_jobs.scripts.validation_job.create_glue_context", return_value=(glue_ctx, spark, job)),
        patch("glue_jobs.scripts.validation_job.load_ssm_config", return_value=SSM_CONFIG),
        patch(
            "glue_jobs.scripts.validation_job.read_parquet_from_s3",
            side_effect=_make_in_memory_reader(spark, table_records),
        ),
        patch("glue_jobs.scripts.validation_job.write_parquet_to_catalog", MagicMock()),
        patch("glue_jobs.scripts.validation_job.write_json_to_s3", MagicMock()),
    ]


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestValidationJobRouting:
    def test_valid_records_written_to_catalog_and_tag_marked_success(
        self, s3, dynamodb_table, spark, mock_glue_job
    ):
        _seed_tag_item(dynamodb_table, "TAG-001")
        _seed_meta_item(dynamodb_table)

        records = {
            "measurements": [
                _valid_measurement("M-001", "TAG-001"),
                _valid_measurement("M-002", "TAG-001"),
            ]
        }

        mock_write_catalog = MagicMock()
        patches = _patch_glue(spark, mock_glue_job, records)
        patches[4] = patch(
            "glue_jobs.scripts.validation_job.write_parquet_to_catalog",
            mock_write_catalog,
        )
        with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5]:
            from glue_jobs.scripts import validation_job
            validation_job.main()

        # write_parquet_to_catalog(df, glue_database, catalog_table_name, validated_path)
        mock_write_catalog.assert_any_call(
            ANY,
            GLUE_DATABASE,
            "validated_measurements",
            ANY,
        )

        item = _get_tag_item(dynamodb_table, "TAG-001")
        assert item["stage_status"]["M"]["VALIDATE"]["S"] == STATUS_SUCCESS
        assert int(item["validate_records_passed"]["N"]) == 2
        assert int(item["validate_records_quarantined"]["N"]) == 0

    def test_invalid_records_routed_to_quarantine_and_tag_marked_success(
        self, s3, dynamodb_table, spark, mock_glue_job
    ):
        """A tag with mixed valid/invalid records is SUCCESS — partial quarantine is acceptable."""
        _seed_tag_item(dynamodb_table, "TAG-001")
        _seed_meta_item(dynamodb_table)

        records = {
            "measurements": [
                _valid_measurement("M-001", "TAG-001"),
                _invalid_measurement("M-002", "TAG-001"),
            ]
        }

        quarantine_calls = []
        patches = _patch_glue(spark, mock_glue_job, records)
        patches[5] = patch(
            "glue_jobs.scripts.validation_job.write_json_to_s3",
            side_effect=lambda df, path: quarantine_calls.append(path),
        )

        with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5]:
            from glue_jobs.scripts import validation_job
            validation_job.main()

        assert any(f"quarantine/{RUN_ID}/measurements" in p for p in quarantine_calls), (
            "Invalid record was not routed to the quarantine path"
        )

        item = _get_tag_item(dynamodb_table, "TAG-001")
        assert item["stage_status"]["M"]["VALIDATE"]["S"] == STATUS_SUCCESS
        assert int(item["validate_records_passed"]["N"]) == 1
        assert int(item["validate_records_quarantined"]["N"]) == 1

    def test_all_records_invalid_raises_and_tag_marked_failed(
        self, s3, dynamodb_table, spark, mock_glue_job
    ):
        _seed_tag_item(dynamodb_table, "TAG-FAIL")
        _seed_meta_item(dynamodb_table)

        records = {
            "measurements": [
                _invalid_measurement("M-001", "TAG-FAIL"),
                _invalid_measurement("M-002", "TAG-FAIL"),
            ]
        }

        patches = _patch_glue(spark, mock_glue_job, records)
        with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5]:
            from glue_jobs.scripts import validation_job
            with pytest.raises(RuntimeError, match="All.*records failed validation"):
                validation_job.main()

        item = _get_tag_item(dynamodb_table, "TAG-FAIL")
        assert item["stage_status"]["M"]["VALIDATE"]["S"] == STATUS_FAILED
        assert int(item["validate_records_passed"]["N"]) == 0
        assert int(item["validate_records_quarantined"]["N"]) == 2
