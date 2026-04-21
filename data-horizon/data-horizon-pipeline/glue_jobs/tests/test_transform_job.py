"""Tests for transform_job.py — fault isolation and per-tag record counts."""

import json
import sys
from unittest.mock import MagicMock, patch

import boto3
import pytest
from moto import mock_aws

from glue_jobs.tests.conftest import (
    CLEANED_BUCKET,
    QUARANTINE_BUCKET,
    RAW_BUCKET,
    RUN_ID,
    TABLE_NAME,
    VALIDATED_BUCKET,
)
from shared.constants import PK_RUN_PREFIX, SK_TAG_PREFIX, STATUS_FAILED, STATUS_SUCCESS


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
    """Tag payload where all measurements have null MeasurementID (primary key)."""
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
# Glue / sys.argv mocking helpers
# ---------------------------------------------------------------------------

GLUE_ARGS = {
    "JOB_NAME": "test-transform-job",
    "run_id": RUN_ID,
    "RAW_BUCKET": RAW_BUCKET,
    "CLEANED_BUCKET": CLEANED_BUCKET,
    "PIPELINE_STATE_TABLE": TABLE_NAME,
}


def _patch_glue(spark, mock_glue_job):
    """Context manager stack — patches GlueContext/Job/args so main() can run."""
    glue_ctx, job = mock_glue_job
    return [
        patch("glue_jobs.scripts.transform_job.getResolvedOptions", return_value=GLUE_ARGS),
        patch("glue_jobs.scripts.transform_job.create_glue_context", return_value=(glue_ctx, spark, job)),
    ]


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestTransformFaultIsolation:
    def test_all_tags_succeed(self, s3, dynamodb_table, spark, mock_glue_job):
        _upload_tag(s3, "TAG-001", _valid_tag_payload("TAG-001", measurement_count=3))
        _upload_tag(s3, "TAG-002", _valid_tag_payload("TAG-002", measurement_count=2))

        patches = _patch_glue(spark, mock_glue_job)
        with patches[0], patches[1]:
            from glue_jobs.scripts import transform_job
            transform_job.main()

        item_001 = _get_tag_item(dynamodb_table, "TAG-001")
        item_002 = _get_tag_item(dynamodb_table, "TAG-002")

        assert item_001["stage_status"]["M"]["TRANSFORM"]["S"] == STATUS_SUCCESS
        assert item_002["stage_status"]["M"]["TRANSFORM"]["S"] == STATUS_SUCCESS

        # Records written should be > 0 for both tags
        assert int(item_001["transform_records_written"]["N"]) > 0
        assert int(item_002["transform_records_written"]["N"]) > 0

    def test_corrupt_tag_isolated_others_succeed(self, s3, dynamodb_table, spark, mock_glue_job):
        _upload_tag(s3, "TAG-001", _valid_tag_payload("TAG-001", measurement_count=2))
        _upload_tag(s3, "TAG-002", _valid_tag_payload("TAG-002", measurement_count=2))
        # Corrupt JSON — not valid JSON at all
        s3.put_object(
            Bucket=RAW_BUCKET,
            Key=f"raw/{RUN_ID}/TAG-BAD.json",
            Body=b"{ this is not valid json !!!",
        )

        patches = _patch_glue(spark, mock_glue_job)
        with patches[0], patches[1]:
            from glue_jobs.scripts import transform_job
            transform_job.main()  # must NOT raise even though TAG-BAD is corrupt

        bad_item = _get_tag_item(dynamodb_table, "TAG-BAD")
        good_item = _get_tag_item(dynamodb_table, "TAG-001")

        assert bad_item["stage_status"]["M"]["TRANSFORM"]["S"] == STATUS_FAILED
        assert int(bad_item.get("transform_records_written", {}).get("N", "0")) == 0
        assert good_item["stage_status"]["M"]["TRANSFORM"]["S"] == STATUS_SUCCESS

    def test_zero_usable_records_tag_marked_failed(self, s3, dynamodb_table, spark, mock_glue_job):
        # All tables will have null primary keys → zero surviving rows
        _upload_tag(s3, "TAG-ZERO", _null_pk_tag_payload("TAG-ZERO"))

        patches = _patch_glue(spark, mock_glue_job)
        with patches[0], patches[1]:
            from glue_jobs.scripts import transform_job
            transform_job.main()

        item = _get_tag_item(dynamodb_table, "TAG-ZERO")
        assert item["stage_status"]["M"]["TRANSFORM"]["S"] == STATUS_FAILED
        assert int(item["transform_records_written"]["N"]) == 0
