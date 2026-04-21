"""Shared fixtures for Glue job tests."""

import json
from unittest.mock import MagicMock

import boto3
import pytest
from moto import mock_aws
from pyspark.sql import SparkSession


REGION = "us-east-1"
RUN_ID = "RUN-TEST-001"
RAW_BUCKET = "test-raw-bucket"
CLEANED_BUCKET = "test-cleaned-bucket"
VALIDATED_BUCKET = "test-validated-bucket"
QUARANTINE_BUCKET = "test-quarantine-bucket"
TABLE_NAME = "PipelineAudit"
GLUE_DATABASE = "test_glue_db"


@pytest.fixture(autouse=True)
def mock_env(monkeypatch):
    monkeypatch.setenv("AWS_DEFAULT_REGION", REGION)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("ENVIRONMENT", "test")


@pytest.fixture()
def aws(mock_env):
    from shared.aws_clients import _client_cache
    _client_cache.clear()
    with mock_aws():
        yield
        _client_cache.clear()


@pytest.fixture()
def s3(aws):
    client = boto3.client("s3", region_name=REGION)
    for bucket in [RAW_BUCKET, CLEANED_BUCKET, VALIDATED_BUCKET, QUARANTINE_BUCKET]:
        client.create_bucket(Bucket=bucket)
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
        ],
        BillingMode="PAY_PER_REQUEST",
    )
    return client


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .master("local[1]")
        .appName("glue-test")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.default.parallelism", "1")
        .getOrCreate()
    )


@pytest.fixture()
def mock_glue_job():
    """Mocked GlueContext + Job pair for injecting into create_glue_context."""
    glue_ctx = MagicMock()
    job = MagicMock()
    return glue_ctx, job
