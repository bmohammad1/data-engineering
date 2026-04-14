"""Shared fixtures for orchestrator Lambda tests."""

import json
import os
from unittest.mock import MagicMock

import boto3
import pytest
from moto import mock_aws


REGION = "us-east-1"
TABLE_NAME = "test-pipeline-state"
CONFIG_BUCKET = "test-config-bucket"
ORCHESTRATION_BUCKET = "test-orchestration-bucket"
SECRET_NAME = "test-pipeline-config"
API_BASE_URL = "https://mock-api.example.com/dev"

TEST_CONFIG = {
    "config_bucket_name": CONFIG_BUCKET,
    "orchestration_bucket_name": ORCHESTRATION_BUCKET,
    "pipeline_state_table": TABLE_NAME,
    "source_api_base_url": API_BASE_URL,
    "map_state_concurrency": 5,
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
