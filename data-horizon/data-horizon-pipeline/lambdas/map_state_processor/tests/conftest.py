"""Shared fixtures for map_state_processor Lambda tests."""

import json
from unittest.mock import MagicMock

import boto3
import pytest
from moto import mock_aws

REGION = "us-east-1"
ENVIRONMENT = "test"
SSM_PATH = f"/data-horizon/{ENVIRONMENT}"

TABLE_NAME = "PipelineAudit"
RAW_BUCKET = "test-raw-bucket"
API_TOKEN = "test-bearer-token-abc123"
API_BASE_URL = "https://mock-api.example.com/dev"

SSM_PARAMS = {
    "source-api-token":        API_TOKEN,
    "pipeline-state-table":    TABLE_NAME,
    "raw-bucket-name":         RAW_BUCKET,
    "source-api-base-url":     API_BASE_URL,
    "map-state-concurrency":   "5",
    "config-bucket-name":      "test-config-bucket",
    "orchestration-bucket-name": "test-orchestration-bucket",
    "cleaned-bucket-name":     "test-cleaned-bucket",
    "validated-bucket-name":   "test-validated-bucket",
    "quarantine-bucket-name":  "test-quarantine-bucket",
    "glue-database":           "test-glue-db",
}

SAMPLE_API_RESPONSE = {
    "tag": {"TagID": "TAG-00001", "TagName": "Flow Rate A"},
    "equipment": {"EquipmentID": "EQ-001", "EquipmentName": "Pump A"},
    "location": {"LocationID": "LOC-001", "SiteName": "Site Alpha"},
    "measurements": [
        {"MeasurementID": "M1", "TagID": "TAG-00001", "Value": 42.5, "QualityFlag": "GOOD"},
        {"MeasurementID": "M2", "TagID": "TAG-00001", "Value": 43.1, "QualityFlag": "GOOD"},
    ],
    "alarms": [],
}


@pytest.fixture(autouse=True)
def mock_env(monkeypatch):
    monkeypatch.setenv("AWS_DEFAULT_REGION", REGION)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("ENVIRONMENT", ENVIRONMENT)


@pytest.fixture()
def aws(mock_env):
    from shared.aws_clients import _client_cache
    _client_cache.clear()
    with mock_aws():
        yield
        _client_cache.clear()


@pytest.fixture()
def ssm_parameters(aws):
    client = boto3.client("ssm", region_name=REGION)
    for key, value in SSM_PARAMS.items():
        client.put_parameter(
            Name=f"{SSM_PATH}/{key}",
            Value=value,
            Type="String",
            Overwrite=True,
        )
    return client


@pytest.fixture()
def dynamodb_table(aws):
    client = boto3.client("dynamodb", region_name=REGION)
    client.create_table(
        TableName=TABLE_NAME,
        KeySchema=[
            {"AttributeName": "PK", "KeyType": "HASH"},
            {"AttributeName": "SK", "KeyType": "RANGE"},
        ],
        AttributeDefinitions=[
            {"AttributeName": "PK", "AttributeType": "S"},
            {"AttributeName": "SK", "AttributeType": "S"},
            {"AttributeName": "GSI1PK", "AttributeType": "S"},
            {"AttributeName": "GSI1SK", "AttributeType": "S"},
        ],
        GlobalSecondaryIndexes=[{
            "IndexName": "GSI1_RunByPipeline",
            "KeySchema": [
                {"AttributeName": "GSI1PK", "KeyType": "HASH"},
                {"AttributeName": "GSI1SK", "KeyType": "RANGE"},
            ],
            "Projection": {"ProjectionType": "ALL"},
        }],
        BillingMode="PAY_PER_REQUEST",
    )
    return client


@pytest.fixture()
def s3_buckets(aws):
    client = boto3.client("s3", region_name=REGION)
    client.create_bucket(Bucket=RAW_BUCKET)
    return client


@pytest.fixture()
def lambda_context():
    ctx = MagicMock()
    ctx.aws_request_id = "test-map-request-id-456"
    ctx.get_remaining_time_in_millis.return_value = 60000
    return ctx


@pytest.fixture()
def seed_tag_item(dynamodb_table):
    """Seed a PENDING TAG item so update_tag_status has a pre-existing item with stage_status."""
    from shared.constants import PK_RUN_PREFIX, SK_TAG_PREFIX

    def _seed(run_id: str, tag_id: str) -> None:
        dynamodb_table.put_item(
            TableName=TABLE_NAME,
            Item={
                "PK":             {"S": f"{PK_RUN_PREFIX}{run_id}"},
                "SK":             {"S": f"{SK_TAG_PREFIX}{tag_id}"},
                "run_id":         {"S": run_id},
                "tag_key":        {"S": tag_id},
                "overall_status": {"S": "PENDING"},
                "stage_status":   {"M": {
                    "EXTRACT":   {"S": "PENDING"},
                    "TRANSFORM": {"S": "PENDING"},
                    "VALIDATE":  {"S": "PENDING"},
                }},
                "attempts":       {"N": "0"},
            },
        )

    return _seed
