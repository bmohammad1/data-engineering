"""Tests for response_processor module."""

from lambdas.map_state_processor.response_processor import extract_records
from lambdas.map_state_processor.tests.conftest import SAMPLE_API_RESPONSE


class TestExtractRecords:
    def test_returns_full_response_as_single_record(self):
        records, _ = extract_records(SAMPLE_API_RESPONSE)

        assert len(records) == 1
        assert records[0] is SAMPLE_API_RESPONSE

    def test_returns_measurement_count(self):
        _, count = extract_records(SAMPLE_API_RESPONSE)

        assert count == 2

    def test_returns_zero_count_when_no_measurements(self):
        response = {"tag": {"TagID": "TAG-00001"}}

        records, count = extract_records(response)

        assert count == 0
        assert len(records) == 1

    def test_returns_empty_measurements_as_zero(self):
        response = {**SAMPLE_API_RESPONSE, "measurements": []}

        _, count = extract_records(response)

        assert count == 0
