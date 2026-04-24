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
# Cleaned JSON helpers (written to the cleaned bucket as input)
# ---------------------------------------------------------------------------

def _cleaned_measurements_json(records: list[dict]) -> bytes:
    """Newline-delimited JSON — the format write_json_to_s3 produces."""
    return b"\n".join(json.dumps(r).encode("utf-8") for r in records)


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
        "Value": None,       # violates value_not_null
        "QualityFlag": "CORRUPTED",  # violates quality_flag_valid
    }


def _upload_cleaned(s3_client, table: str, body: bytes) -> None:
    s3_client.put_object(
        Bucket=CLEANED_BUCKET,
        Key=f"cleaned/{RUN_ID}/{table}/part-00000.json",
        Body=body,
    )


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


def _make_spark_parquet_reader(spark, schema):
    """Return a function that reads from moto S3 via Spark instead of GlueContext."""
    def _read(glue_ctx, s3_path, tbl_schema):
        try:
            return spark.read.schema(tbl_schema).parquet(s3_path)
        except Exception:
            return spark.createDataFrame([], tbl_schema)
    return _read


def _make_spark_json_reader(spark):
    """Return a patched read_parquet_from_s3 that falls back to JSON for test uploads."""
    def _read(glue_ctx, s3_path, tbl_schema):
        try:
            return spark.read.schema(tbl_schema).json(s3_path)
        except Exception:
            return spark.createDataFrame([], tbl_schema)
    return _read


def _patch_glue(spark, mock_glue_job):
    """Return a list of context managers patching all Glue-specific entry points."""
    glue_ctx, job = mock_glue_job
    mock_write_catalog = MagicMock()
    mock_write_json = MagicMock()

    return [
        patch("glue_jobs.scripts.validation_job.getResolvedOptions", return_value=GLUE_ARGS),
        patch("glue_jobs.scripts.validation_job.create_glue_context", return_value=(glue_ctx, spark, job)),
        patch("glue_jobs.scripts.validation_job.load_ssm_config", return_value=SSM_CONFIG),
        patch("glue_jobs.scripts.validation_job.read_parquet_from_s3", side_effect=_make_spark_json_reader(spark)),
        patch("glue_jobs.scripts.validation_job.write_parquet_to_catalog", mock_write_catalog),
        patch("glue_jobs.scripts.validation_job.write_json_to_s3", mock_write_json),
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

        records = [
            _valid_measurement("M-001", "TAG-001"),
            _valid_measurement("M-002", "TAG-001"),
        ]
        _upload_cleaned(s3, "measurements", _cleaned_measurements_json(records))

        mock_write_catalog = MagicMock()
        patches = _patch_glue(spark, mock_glue_job)
        patches[4] = patch(
            "glue_jobs.scripts.validation_job.write_parquet_to_catalog",
            mock_write_catalog,
        )
        with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5]:
            from glue_jobs.scripts import validation_job
            validation_job.main()

        mock_write_catalog.assert_any_call(
            ANY,
            ANY,
            GLUE_DATABASE,
            "validated_measurements",
            ANY,
            partition_cols=["partition_date"],
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

        records = [
            _valid_measurement("M-001", "TAG-001"),
            _invalid_measurement("M-002", "TAG-001"),
        ]
        _upload_cleaned(s3, "measurements", _cleaned_measurements_json(records))

        mock_write_json = MagicMock()
        patches = _patch_glue(spark, mock_glue_job)
        # Replace the write_json_to_s3 mock with a capturing one so we can verify quarantine routing.
        quarantine_calls = []
        original_patches = patches[:]

        with patches[0], patches[1], patches[2], patches[3], patches[4]:
            with patch(
                "glue_jobs.scripts.validation_job.write_json_to_s3",
                side_effect=lambda df, path: quarantine_calls.append(path),
            ):
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

        records = [
            _invalid_measurement("M-001", "TAG-FAIL"),
            _invalid_measurement("M-002", "TAG-FAIL"),
        ]
        _upload_cleaned(s3, "measurements", _cleaned_measurements_json(records))

        patches = _patch_glue(spark, mock_glue_job)
        with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5]:
            from glue_jobs.scripts import validation_job
            with pytest.raises(RuntimeError, match="All.*records failed validation"):
                validation_job.main()

        item = _get_tag_item(dynamodb_table, "TAG-FAIL")
        assert item["stage_status"]["M"]["VALIDATE"]["S"] == STATUS_FAILED
        assert int(item["validate_records_passed"]["N"]) == 0
        assert int(item["validate_records_quarantined"]["N"]) == 2
