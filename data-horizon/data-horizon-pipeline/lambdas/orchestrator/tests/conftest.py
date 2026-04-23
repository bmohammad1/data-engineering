"""Shared fixtures for orchestrator Lambda tests."""

import json
import os
from unittest.mock import MagicMock

import boto3
import pytest
from moto import mock_aws


REGION = "us-east-1"
ENVIRONMENT = "test"
SSM_PATH = f"/data-horizon/{ENVIRONMENT}"

TABLE_NAME = "PipelineAudit"
CONFIG_BUCKET = "test-config-bucket"
ORCHESTRATION_BUCKET = "test-orchestration-bucket"
RAW_BUCKET = "test-raw-bucket"
API_BASE_URL = "https://mock-api.example.com/dev"
API_TOKEN = "test-bearer-token-abc123"

SSM_PARAMS = {
    "source-api-token":        API_TOKEN,
    "pipeline-state-table":    TABLE_NAME,
    "config-bucket-name":      CONFIG_BUCKET,
    "orchestration-bucket-name": ORCHESTRATION_BUCKET,
    "raw-bucket-name":         RAW_BUCKET,
    "source-api-base-url":     API_BASE_URL,
    "map-state-concurrency":   "5",
    "cleaned-bucket-name":     "test-cleaned-bucket",
    "validated-bucket-name":   "test-validated-bucket",
    "quarantine-bucket-name":  "test-quarantine-bucket",
    "glue-database":           "test-glue-db",
}

# Kept for unit tests that call load_tags_from_s3 directly without going through
# the handler (they need config_bucket_name in the config dict).
TEST_CONFIG = {
    "source_api_token":   API_TOKEN,
    "config_bucket_name": CONFIG_BUCKET,
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
    client.create_bucket(Bucket=CONFIG_BUCKET)
    client.create_bucket(Bucket=ORCHESTRATION_BUCKET)
    return client


@pytest.fixture()
def sample_tags_csv(s3_buckets):
    csv_content = "TagID\n" + "\n".join(f"TAG-{i:05d}" for i in range(1, 11))
    s3_buckets.put_object(
        Bucket=CONFIG_BUCKET,
        Key="source_config/tags.csv",
        Body=csv_content.encode("utf-8"),
    )
    return [f"TAG-{i:05d}" for i in range(1, 11)]


@pytest.fixture()
def lambda_context():
    ctx = MagicMock()
    ctx.aws_request_id = "test-request-id-123"
    ctx.get_remaining_time_in_millis.return_value = 60000
    return ctx
