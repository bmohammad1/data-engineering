"""Tests for s3_writer module."""

import json

import boto3
import pytest

from lambdas.map_state_processor.s3_writer import write_raw_response
from lambdas.map_state_processor.tests.conftest import RAW_BUCKET, REGION, SAMPLE_API_RESPONSE
from shared.exceptions import S3WriteError

RUN_ID = "RUN-S3TEST001"
TAG_ID = "TAG-00001"


class TestWriteRawResponse:
    def test_writes_json_to_correct_key(self, s3_buckets):
        write_raw_response(RAW_BUCKET, RUN_ID, TAG_ID, [SAMPLE_API_RESPONSE])

        s3 = boto3.client("s3", region_name=REGION)
        response = s3.get_object(Bucket=RAW_BUCKET, Key=f"raw/{RUN_ID}/{TAG_ID}.json")
        body = json.loads(response["Body"].read())

        assert isinstance(body, list)
        assert body[0]["tag"]["TagID"] == "TAG-00001"

    def test_returns_s3_key(self, s3_buckets):
        key = write_raw_response(RAW_BUCKET, RUN_ID, TAG_ID, [SAMPLE_API_RESPONSE])

        assert key == f"raw/{RUN_ID}/{TAG_ID}.json"

    def test_raises_s3_write_error_on_missing_bucket(self, aws):
        with pytest.raises(S3WriteError):
            write_raw_response("nonexistent-bucket", RUN_ID, TAG_ID, [SAMPLE_API_RESPONSE])
