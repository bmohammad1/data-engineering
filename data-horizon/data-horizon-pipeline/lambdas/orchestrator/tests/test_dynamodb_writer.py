"""Tests for dynamodb_writer module."""

import boto3
import pytest

from lambdas.orchestrator.dynamodb_writer import write_run_metadata, write_tag_records
from lambdas.orchestrator.tests.conftest import REGION, TABLE_NAME
from shared.constants import PK_PIPELINE_RUN, SK_METADATA, SK_TAG_STATUS_PREFIX, STATUS_PENDING, STATUS_RUNNING
from shared.exceptions import DynamoDBError


class TestWriteRunMetadata:
    def test_writes_correct_item(self, dynamodb_table):
        write_run_metadata(TABLE_NAME, "RUN-TEST001", total_tags=100)

        response = dynamodb_table.get_item(
            TableName=TABLE_NAME,
            Key={
                "PK": {"S": f"{PK_PIPELINE_RUN}RUN-TEST001"},
                "SK": {"S": SK_METADATA},
            },
        )
        item = response["Item"]

        assert item["run_id"]["S"] == "RUN-TEST001"
        assert item["total_tags"]["N"] == "100"
        assert item["success_count"]["N"] == "0"
        assert item["failure_count"]["N"] == "0"
        assert item["final_status"]["S"] == STATUS_RUNNING

    def test_raises_dynamodb_error_on_failure(self, aws):
        with pytest.raises(DynamoDBError):
            write_run_metadata("nonexistent-table", "RUN-FAIL", total_tags=1)


class TestWriteTagRecords:
    def test_writes_all_tags(self, dynamodb_table):
        tags = [f"TAG-{i:05d}" for i in range(1, 11)]
        write_tag_records(TABLE_NAME, "RUN-TEST001", tags, "https://api.example.com")

        response = dynamodb_table.query(
            TableName=TABLE_NAME,
            KeyConditionExpression="PK = :pk AND begins_with(SK, :prefix)",
            ExpressionAttributeValues={
                ":pk": {"S": f"{PK_PIPELINE_RUN}RUN-TEST001"},
                ":prefix": {"S": SK_TAG_STATUS_PREFIX},
            },
        )

        assert response["Count"] == 10

    def test_default_status_is_pending(self, dynamodb_table):
        write_tag_records(TABLE_NAME, "RUN-TEST001", ["TAG-00001"], "https://api.example.com")

        response = dynamodb_table.get_item(
            TableName=TABLE_NAME,
            Key={
                "PK": {"S": f"{PK_PIPELINE_RUN}RUN-TEST001"},
                "SK": {"S": f"{SK_TAG_STATUS_PREFIX}TAG-00001"},
            },
        )
        item = response["Item"]

        assert item["final_status"]["S"] == STATUS_PENDING
        assert item["attempts"]["N"] == "0"
        assert item["records_received"]["N"] == "0"
        assert item["error_code"]["NULL"] is True

    def test_large_batch_exceeding_25_limit(self, dynamodb_table):
        tags = [f"TAG-{i:05d}" for i in range(1, 31)]
        write_tag_records(TABLE_NAME, "RUN-TEST001", tags, "https://api.example.com")

        response = dynamodb_table.query(
            TableName=TABLE_NAME,
            KeyConditionExpression="PK = :pk AND begins_with(SK, :prefix)",
            ExpressionAttributeValues={
                ":pk": {"S": f"{PK_PIPELINE_RUN}RUN-TEST001"},
                ":prefix": {"S": SK_TAG_STATUS_PREFIX},
            },
        )

        assert response["Count"] == 30

    def test_endpoint_format(self, dynamodb_table):
        write_tag_records(TABLE_NAME, "RUN-TEST001", ["TAG-00042"], "https://api.example.com")

        response = dynamodb_table.get_item(
            TableName=TABLE_NAME,
            Key={
                "PK": {"S": f"{PK_PIPELINE_RUN}RUN-TEST001"},
                "SK": {"S": f"{SK_TAG_STATUS_PREFIX}TAG-00042"},
            },
        )

        assert response["Item"]["endpoint"]["S"] == "https://api.example.com/tag/TAG-00042"
