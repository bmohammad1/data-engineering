"""Tests for validation_job.py — routing and per-tag DynamoDB status updates."""

import datetime
import json
from unittest.mock import MagicMock, patch

import boto3
import pytest
from moto import mock_aws
from pyspark.sql import Row

from glue_jobs.tests.conftest import (
    CLEANED_BUCKET,
    QUARANTINE_BUCKET,
    RAW_BUCKET,
    RUN_ID,
    TABLE_NAME,
    VALIDATED_BUCKET,
    GLUE_DATABASE,
)
from shared.constants import PK_RUN_PREFIX, SK_TAG_PREFIX, STATUS_FAILED, STATUS_SUCCESS


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
        "Value": None,      # violates value_not_null
        "QualityFlag": "CORRUPTED",  # violates quality_flag_valid
    }


def _upload_cleaned(s3_client, table: str, body: bytes) -> None:
    s3_client.put_object(
        Bucket=CLEANED_BUCKET,
        Key=f"cleaned/{RUN_ID}/{table}/part-00000.json",
        Body=body,
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
    "CLEANED_BUCKET": CLEANED_BUCKET,
    "VALIDATED_BUCKET": VALIDATED_BUCKET,
    "QUARANTINE_BUCKET": QUARANTINE_BUCKET,
    "PIPELINE_STATE_TABLE": TABLE_NAME,
    "GLUE_DATABASE": GLUE_DATABASE,
}


def _patch_glue(spark, mock_glue_job):
    glue_ctx, job = mock_glue_job
    # write_parquet_to_catalog uses the Glue catalog sink — replace with a
    # simple Parquet write so tests don't need a real Glue catalog.
    mock_write_catalog = MagicMock()
    return [
        patch("glue_jobs.scripts.validation_job.getResolvedOptions", return_value=GLUE_ARGS),
        patch("glue_jobs.scripts.validation_job.create_glue_context", return_value=(glue_ctx, spark, job)),
        patch("glue_jobs.scripts.validation_job.write_parquet_to_catalog", mock_write_catalog),
    ]


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestValidationJobRouting:
    def test_valid_records_written_to_catalog_and_tag_marked_success(
        self, s3, dynamodb_table, spark, mock_glue_job
    ):
        records = [
            _valid_measurement("M-001", "TAG-001"),
            _valid_measurement("M-002", "TAG-001"),
        ]
        _upload_cleaned(s3, "measurements", _cleaned_measurements_json(records))

        patches = _patch_glue(spark, mock_glue_job)
        with patches[0], patches[1], patches[2]:
            from glue_jobs.scripts import validation_job
            validation_job.main()

        item = _get_tag_item(dynamodb_table, "TAG-001")
        assert item["stage_status"]["M"]["VALIDATE"]["S"] == STATUS_SUCCESS
        assert int(item["validate_records_passed"]["N"]) == 2
        assert int(item["validate_records_quarantined"]["N"]) == 0

    def test_invalid_records_routed_to_quarantine_bucket(
        self, s3, dynamodb_table, spark, mock_glue_job
    ):
        records = [
            _valid_measurement("M-001", "TAG-001"),
            _invalid_measurement("M-002", "TAG-001"),  # should be quarantined
        ]
        _upload_cleaned(s3, "measurements", _cleaned_measurements_json(records))

        patches = _patch_glue(spark, mock_glue_job)
        with patches[0], patches[1], patches[2]:
            from glue_jobs.scripts import validation_job
            validation_job.main()

        # Quarantine bucket should have a file for measurements
        resp = s3.list_objects_v2(
            Bucket=QUARANTINE_BUCKET,
            Prefix=f"quarantine/{RUN_ID}/measurements/",
        )
        assert resp.get("KeyCount", 0) > 0

        item = _get_tag_item(dynamodb_table, "TAG-001")
        assert item["stage_status"]["M"]["VALIDATE"]["S"] == STATUS_SUCCESS
        assert int(item["validate_records_passed"]["N"]) == 1
        assert int(item["validate_records_quarantined"]["N"]) == 1

    def test_all_records_invalid_tag_marked_failed(
        self, s3, dynamodb_table, spark, mock_glue_job
    ):
        records = [
            _invalid_measurement("M-001", "TAG-FAIL"),
            _invalid_measurement("M-002", "TAG-FAIL"),
        ]
        _upload_cleaned(s3, "measurements", _cleaned_measurements_json(records))

        patches = _patch_glue(spark, mock_glue_job)
        with patches[0], patches[1], patches[2]:
            from glue_jobs.scripts import validation_job
            with pytest.raises(RuntimeError, match="All.*records failed validation"):
                validation_job.main()

        item = _get_tag_item(dynamodb_table, "TAG-FAIL")
        assert item["stage_status"]["M"]["VALIDATE"]["S"] == STATUS_FAILED
        assert int(item["validate_records_passed"]["N"]) == 0
        assert int(item["validate_records_quarantined"]["N"]) == 2
