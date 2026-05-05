"""Tests for config_loader module."""

import pytest

from lambdas.orchestrator.config_loader import load_pipeline_config, load_tags_from_s3
from lambdas.orchestrator.tests.conftest import (
    API_TOKEN,
    CONFIG_BUCKET,
    TEST_CONFIG,
)
from shared.exceptions import TagFileError


class TestLoadPipelineConfig:
    def test_returns_config_dict(self):
        config = load_pipeline_config(API_TOKEN)

        assert config["source_api_token"] == API_TOKEN


class TestLoadTagsFromS3:
    def test_returns_tag_list(self, sample_tags_csv):
        tags = load_tags_from_s3(TEST_CONFIG)

        assert len(tags) == 10
        assert tags[0] == "TAG-00001"
        assert tags[-1] == "TAG-00010"

    def test_raises_tag_file_error_when_file_missing(self, s3_buckets):
        with pytest.raises(TagFileError, match="not found"):
            load_tags_from_s3(TEST_CONFIG)

    def test_raises_tag_file_error_on_empty_file(self, s3_buckets):
        s3_buckets.put_object(
            Bucket=CONFIG_BUCKET,
            Key="source_config/tags.csv",
            Body=b"",
        )

        with pytest.raises(TagFileError, match="empty"):
            load_tags_from_s3(TEST_CONFIG)

    def test_raises_tag_file_error_when_no_valid_tags(self, s3_buckets):
        s3_buckets.put_object(
            Bucket=CONFIG_BUCKET,
            Key="source_config/tags.csv",
            Body=b"TagID\n\n\n",
        )

        with pytest.raises(TagFileError, match="No valid"):
            load_tags_from_s3(TEST_CONFIG)
