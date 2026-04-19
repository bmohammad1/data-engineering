"""Tests for map_state_processor Lambda handler."""

import json

import boto3
import pytest
import responses

from lambdas.map_state_processor.handler import handler
from lambdas.map_state_processor.tests.conftest import (
    API_BASE_URL,
    REGION,
    RAW_BUCKET,
    SAMPLE_API_RESPONSE,
    TABLE_NAME,
)
from shared.constants import PK_RUN_PREFIX, SK_TAG_PREFIX
from shared.exceptions import RetryableError

RUN_ID = "RUN-HANDLERTEST01"
TAG_ID = "TAG-00001"
ENDPOINT = f"{API_BASE_URL}/tag/{TAG_ID}"


class TestHandler:
    @pytest.fixture(autouse=True)
    def setup_all(self, dynamodb_table, s3_buckets, secret, seed_tag_item):
        self.dynamodb = dynamodb_table
        seed_tag_item(RUN_ID, TAG_ID)

    @responses.activate
    def test_happy_path_return_shape(self, lambda_context):
        responses.add(responses.GET, ENDPOINT, json=SAMPLE_API_RESPONSE, status=200)

        result = handler({"run_id": RUN_ID, "tag_id": TAG_ID, "endpoint": ENDPOINT}, lambda_context)

        assert result["tag_id"] == TAG_ID
        assert result["status"] == "SUCCESS"
        assert result["records_written"] == 2  # 2 measurements + 0 alarms in SAMPLE_API_RESPONSE

    @responses.activate
    def test_writes_raw_response_to_s3(self, lambda_context):
        responses.add(responses.GET, ENDPOINT, json=SAMPLE_API_RESPONSE, status=200)

        handler({"run_id": RUN_ID, "tag_id": TAG_ID, "endpoint": ENDPOINT}, lambda_context)

        s3 = boto3.client("s3", region_name=REGION)
        response = s3.get_object(Bucket=RAW_BUCKET, Key=f"raw/{RUN_ID}/{TAG_ID}.json")
        body = json.loads(response["Body"].read())

        assert isinstance(body, list)
        assert body[0]["tag"]["TagID"] == TAG_ID

    @responses.activate
    def test_updates_dynamodb_to_success(self, lambda_context):
        responses.add(responses.GET, ENDPOINT, json=SAMPLE_API_RESPONSE, status=200)

        handler({"run_id": RUN_ID, "tag_id": TAG_ID, "endpoint": ENDPOINT}, lambda_context)

        item = self.dynamodb.get_item(
            TableName=TABLE_NAME,
            Key={
                "PK": {"S": f"{PK_RUN_PREFIX}{RUN_ID}"},
                "SK": {"S": f"{SK_TAG_PREFIX}{TAG_ID}"},
            },
        )["Item"]

        assert item["overall_status"]["S"] == "SUCCESS"
        assert item["records_received"]["N"] == "2"
        assert item["stage_status"]["M"]["EXTRACT"]["S"] == "SUCCESS"

    @responses.activate
    def test_updates_dynamodb_to_failed_on_api_error(self, lambda_context):
        responses.add(responses.GET, ENDPOINT, status=500)

        with pytest.raises(RetryableError):
            handler({"run_id": RUN_ID, "tag_id": TAG_ID, "endpoint": ENDPOINT}, lambda_context)

        item = self.dynamodb.get_item(
            TableName=TABLE_NAME,
            Key={
                "PK": {"S": f"{PK_RUN_PREFIX}{RUN_ID}"},
                "SK": {"S": f"{SK_TAG_PREFIX}{TAG_ID}"},
            },
        )["Item"]

        assert item["overall_status"]["S"] == "FAILED"
        assert item["stage_status"]["M"]["EXTRACT"]["S"] == "FAILED"

    @responses.activate
    def test_re_raises_exception_after_failure_update(self, lambda_context):
        responses.add(responses.GET, ENDPOINT, status=500)

        with pytest.raises(RetryableError):
            handler({"run_id": RUN_ID, "tag_id": TAG_ID, "endpoint": ENDPOINT}, lambda_context)
