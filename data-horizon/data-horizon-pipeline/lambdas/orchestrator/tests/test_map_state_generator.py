"""Tests for map_state_generator module."""

import json

import boto3
import pytest

from lambdas.orchestrator.map_state_generator import generate_map_state_input
from lambdas.orchestrator.tests.conftest import ORCHESTRATION_BUCKET, REGION, TEST_CONFIG


class TestGenerateMapStateInput:
    def test_writes_json_to_s3(self, s3_buckets):
        tags = ["TAG-00001", "TAG-00002"]
        s3_key = generate_map_state_input(ORCHESTRATION_BUCKET, "RUN-TEST001", tags, TEST_CONFIG)

        s3 = boto3.client("s3", region_name=REGION)
        response = s3.get_object(Bucket=ORCHESTRATION_BUCKET, Key=s3_key)
        body = json.loads(response["Body"].read().decode("utf-8"))

        assert isinstance(body, list)
        assert len(body) == 2

    def test_json_content_structure(self, s3_buckets):
        tags = ["TAG-00001"]
        s3_key = generate_map_state_input(ORCHESTRATION_BUCKET, "RUN-TEST001", tags, TEST_CONFIG)

        s3 = boto3.client("s3", region_name=REGION)
        response = s3.get_object(Bucket=ORCHESTRATION_BUCKET, Key=s3_key)
        items = json.loads(response["Body"].read().decode("utf-8"))

        item = items[0]
        assert item["tag_id"] == "TAG-00001"
        assert item["run_id"] == "RUN-TEST001"
        assert "endpoint" in item
        assert item["endpoint"].endswith("/tag/TAG-00001")

    def test_returns_s3_key(self, s3_buckets):
        tags = ["TAG-00001"]
        s3_key = generate_map_state_input(ORCHESTRATION_BUCKET, "RUN-TEST001", tags, TEST_CONFIG)

        assert s3_key == "maps/RUN-TEST001/map_input.json"
