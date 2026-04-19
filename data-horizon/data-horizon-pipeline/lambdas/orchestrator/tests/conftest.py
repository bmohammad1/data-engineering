"""Shared fixtures for orchestrator Lambda tests."""

import json
import os
from unittest.mock import MagicMock

import boto3
import pytest
from moto import mock_aws


REGION = "us-east-1"
TABLE_NAME = "PipelineAudit"
CONFIG_BUCKET = "test-config-bucket"
ORCHESTRATION_BUCKET = "test-orchestration-bucket"
SECRET_NAME = "test-pipeline-config"
API_BASE_URL = "https://mock-api.example.com/dev"

# Only actual secrets stay in the Secrets Manager mock.
# Non-secret config is read from env vars by the Lambda handler.
# config_bucket_name is kept here so load_tags_from_s3 unit tests
# can pass TEST_CONFIG directly without going through the handler.
TEST_CONFIG = {
    "source_api_token":  "test-bearer-token-abc123",
    "config_bucket_name": CONFIG_BUCKET,
}


@pytest.fixture(autouse=True)
def mock_env(monkeypatch):
    monkeypatch.setenv("AWS_DEFAULT_REGION", REGION)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("SECRET_NAME", SECRET_NAME)
    monkeypatch.setenv("ENVIRONMENT", "test")
    monkeypatch.setenv("PIPELINE_STATE_TABLE", TABLE_NAME)
    monkeypatch.setenv("CONFIG_BUCKET_NAME", CONFIG_BUCKET)
    monkeypatch.setenv("ORCHESTRATION_BUCKET_NAME", ORCHESTRATION_BUCKET)
    monkeypatch.setenv("SOURCE_API_BASE_URL", API_BASE_URL)
    monkeypatch.setenv("MAP_STATE_CONCURRENCY", "5")


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
def secret(aws):
    client = boto3.client("secretsmanager", region_name=REGION)
    client.create_secret(
        Name=SECRET_NAME,
        SecretString=json.dumps(TEST_CONFIG),
    )
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
