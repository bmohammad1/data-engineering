"""Tests for orchestrator Lambda handler."""

import pytest

from lambdas.orchestrator.handler import handler
from lambdas.orchestrator.tests.conftest import TABLE_NAME
from shared.constants import PK_PIPELINE_RUN, SK_METADATA, SK_TAG_STATUS_PREFIX
from shared.exceptions import ConfigLoadError


class TestHandler:
    @pytest.fixture(autouse=True)
    def setup_all(self, dynamodb_table, s3_buckets, secret, sample_tags_csv):
        self.dynamodb = dynamodb_table
        self.tags = sample_tags_csv

    def test_happy_path_return_shape(self, lambda_context):
        result = handler({}, lambda_context)

        assert "run_id" in result
        assert "map_items_s3_key" in result
        assert "total_tags" in result
        assert result["total_tags"] == 10

    def test_uses_event_run_id(self, lambda_context):
        result = handler({"run_id": "RUN-CUSTOM123"}, lambda_context)

        assert result["run_id"] == "RUN-CUSTOM123"

    def test_generates_run_id_when_not_in_event(self, lambda_context):
        result = handler({}, lambda_context)

        assert result["run_id"].startswith("RUN-")
        assert len(result["run_id"]) == 16

    def test_writes_run_metadata_to_dynamodb(self, lambda_context):
        result = handler({}, lambda_context)
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
        result = handler({}, lambda_context)
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
            handler({}, lambda_context)
