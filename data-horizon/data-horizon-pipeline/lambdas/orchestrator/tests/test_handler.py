"""Tests for orchestrator Lambda handler."""

import pytest

from lambdas.orchestrator.handler import handler
from lambdas.orchestrator.tests.conftest import TABLE_NAME
from shared.constants import PK_PIPELINE_RUN, SK_METADATA, SK_TAG_STATUS_PREFIX
from shared.exceptions import ConfigLoadError, PermanentError

TEST_RUN_ID = "RUN-TESTHANDLER01"


class TestHandler:
    @pytest.fixture(autouse=True)
    def setup_all(self, dynamodb_table, s3_buckets, secret, sample_tags_csv):
        self.dynamodb = dynamodb_table
        self.tags = sample_tags_csv

    def test_happy_path_return_shape(self, lambda_context):
        result = handler({"run_id": TEST_RUN_ID}, lambda_context)

        assert result["run_id"] == TEST_RUN_ID
        assert "map_items_s3_key" in result
        assert "concurrency" in result

    def test_uses_event_run_id(self, lambda_context):
        result = handler({"run_id": "RUN-CUSTOM123"}, lambda_context)

        assert result["run_id"] == "RUN-CUSTOM123"

    def test_raises_if_run_id_missing(self, lambda_context):
        with pytest.raises(PermanentError, match="run_id is required"):
            handler({}, lambda_context)

    def test_writes_run_metadata_to_dynamodb(self, lambda_context):
        result = handler({"run_id": TEST_RUN_ID}, lambda_context)
        run_id = result["run_id"]

        response = self.dynamodb.get_item(
            TableName=TABLE_NAME,
            Key={
                "PK": {"S": f"{PK_PIPELINE_RUN}{run_id}"},
                "SK": {"S": SK_METADATA},
            },
        )

        assert "Item" in response
        assert response["Item"]["total_tags"]["N"] == "10"

    def test_writes_tag_records_to_dynamodb(self, lambda_context):
        result = handler({"run_id": TEST_RUN_ID}, lambda_context)
        run_id = result["run_id"]

        response = self.dynamodb.query(
            TableName=TABLE_NAME,
            KeyConditionExpression="PK = :pk AND begins_with(SK, :prefix)",
            ExpressionAttributeValues={
                ":pk": {"S": f"{PK_PIPELINE_RUN}{run_id}"},
                ":prefix": {"S": SK_TAG_STATUS_PREFIX},
            },
        )

        assert response["Count"] == 10

    def test_config_load_failure(self, lambda_context, monkeypatch):
        monkeypatch.setenv("SECRET_NAME", "nonexistent-secret")

        with pytest.raises(ConfigLoadError):
            handler({"run_id": TEST_RUN_ID}, lambda_context)
