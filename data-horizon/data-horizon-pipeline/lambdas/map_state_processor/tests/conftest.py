"""Shared fixtures for map_state_processor Lambda tests."""

import json
from unittest.mock import MagicMock

import boto3
import pytest
from moto import mock_aws

REGION = "us-east-1"
TABLE_NAME = "test-pipeline-state"
RAW_BUCKET = "test-raw-bucket"
SECRET_NAME = "test-pipeline-config"
API_TOKEN = "test-bearer-token-abc123"
API_BASE_URL = "https://mock-api.example.com/dev"

TEST_CONFIG = {
    "raw_bucket_name": RAW_BUCKET,
    "pipeline_state_table": TABLE_NAME,
    "source_api_token": API_TOKEN,
    "source_api_base_url": API_BASE_URL,
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
    monkeypatch.setenv("SECRET_NAME", SECRET_NAME)
    monkeypatch.setenv("ENVIRONMENT", "test")


@pytest.fixture()
def aws(mock_env):
    from shared.aws_clients import _client_cache
    _client_cache.clear()
    with mock_aws():
        yield
        _client_cache.clear()


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
        ],
        BillingMode="PAY_PER_REQUEST",
    )
    return client


@pytest.fixture()
def s3_buckets(aws):
    client = boto3.client("s3", region_name=REGION)
    client.create_bucket(Bucket=RAW_BUCKET)
    return client


@pytest.fixture()
def secret(aws):
    client = boto3.client("secretsmanager", region_name=REGION)
    client.create_secret(
        Name=SECRET_NAME,
        SecretString=json.dumps(TEST_CONFIG),
    )
    return client


@pytest.fixture()
def lambda_context():
    ctx = MagicMock()
    ctx.aws_request_id = "test-map-request-id-456"
    ctx.get_remaining_time_in_millis.return_value = 60000
    return ctx
