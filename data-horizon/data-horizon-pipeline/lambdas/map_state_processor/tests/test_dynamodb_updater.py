"""Tests for dynamodb_updater module."""

import boto3
import pytest

from lambdas.map_state_processor.dynamodb_updater import update_tag_status
from lambdas.map_state_processor.tests.conftest import REGION, TABLE_NAME
from shared.constants import PK_RUN_PREFIX, SK_TAG_PREFIX
from shared.exceptions import DynamoDBError

RUN_ID = "RUN-DDBTEST001"
TAG_ID = "TAG-00001"


class TestUpdateTagStatus:
    def test_updates_overall_status_and_records_received(self, dynamodb_table, seed_tag_item):
        seed_tag_item(RUN_ID, TAG_ID)
        update_tag_status(TABLE_NAME, RUN_ID, TAG_ID, "SUCCESS", 42, extraction_duration_ms=500)

        item = dynamodb_table.get_item(
            TableName=TABLE_NAME,
            Key={
                "PK": {"S": f"{PK_RUN_PREFIX}{RUN_ID}"},
                "SK": {"S": f"{SK_TAG_PREFIX}{TAG_ID}"},
            },
        )["Item"]

        assert item["overall_status"]["S"] == "SUCCESS"
        assert item["records_received"]["N"] == "42"
        assert item["stage_status"]["M"]["EXTRACT"]["S"] == "SUCCESS"
        assert item["extraction_duration_ms"]["N"] == "500"

    def test_increments_attempts_on_each_call(self, dynamodb_table, seed_tag_item):
        seed_tag_item(RUN_ID, TAG_ID)
        update_tag_status(TABLE_NAME, RUN_ID, TAG_ID, "SUCCESS", 5)
        update_tag_status(TABLE_NAME, RUN_ID, TAG_ID, "SUCCESS", 5)

        item = dynamodb_table.get_item(
            TableName=TABLE_NAME,
            Key={
                "PK": {"S": f"{PK_RUN_PREFIX}{RUN_ID}"},
                "SK": {"S": f"{SK_TAG_PREFIX}{TAG_ID}"},
            },
        )["Item"]

        assert item["attempts"]["N"] == "2"

    def test_accumulates_extraction_duration_ms_across_calls(self, dynamodb_table, seed_tag_item):
        seed_tag_item(RUN_ID, TAG_ID)
        update_tag_status(TABLE_NAME, RUN_ID, TAG_ID, "FAILED", 0, extraction_duration_ms=300)
        update_tag_status(TABLE_NAME, RUN_ID, TAG_ID, "SUCCESS", 5, extraction_duration_ms=450)

        item = dynamodb_table.get_item(
            TableName=TABLE_NAME,
            Key={
                "PK": {"S": f"{PK_RUN_PREFIX}{RUN_ID}"},
                "SK": {"S": f"{SK_TAG_PREFIX}{TAG_ID}"},
            },
        )["Item"]

        assert item["extraction_duration_ms"]["N"] == "750"
        assert item["attempts"]["N"] == "2"

    def test_raises_dynamodb_error_on_missing_table(self, aws):
        with pytest.raises(DynamoDBError):
            update_tag_status("nonexistent-table", RUN_ID, TAG_ID, "SUCCESS", 0)
