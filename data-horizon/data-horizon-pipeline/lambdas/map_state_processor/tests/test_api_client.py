"""Tests for api_client module."""

import pytest
import requests
import responses

from lambdas.map_state_processor.api_client import fetch_tag_data
from lambdas.map_state_processor.tests.conftest import API_TOKEN
from shared.exceptions import PermanentError, RetryableError

ENDPOINT = "https://mock-api.example.com/dev/tag/TAG-00001"
SAMPLE_RESPONSE = {"tag": {"TagID": "TAG-00001"}, "measurements": []}


class TestFetchTagData:
    @responses.activate
    def test_returns_parsed_response(self):
        responses.add(responses.GET, ENDPOINT, json=SAMPLE_RESPONSE, status=200)

        result = fetch_tag_data(ENDPOINT, API_TOKEN)

        assert result["tag"]["TagID"] == "TAG-00001"

    @responses.activate
    def test_sends_bearer_token(self):
        responses.add(responses.GET, ENDPOINT, json=SAMPLE_RESPONSE, status=200)

        fetch_tag_data(ENDPOINT, API_TOKEN)

        assert responses.calls[0].request.headers["Authorization"] == f"Bearer {API_TOKEN}"

    @responses.activate
    def test_raises_retryable_on_429(self):
        responses.add(responses.GET, ENDPOINT, status=429)

        with pytest.raises(RetryableError):
            fetch_tag_data(ENDPOINT, API_TOKEN)

    @responses.activate
    def test_raises_retryable_on_500(self):
        responses.add(responses.GET, ENDPOINT, status=500)

        with pytest.raises(RetryableError):
            fetch_tag_data(ENDPOINT, API_TOKEN)

    @responses.activate
    def test_raises_permanent_on_404(self):
        responses.add(responses.GET, ENDPOINT, status=404)

        with pytest.raises(PermanentError):
            fetch_tag_data(ENDPOINT, API_TOKEN)

    @responses.activate
    def test_raises_permanent_on_403(self):
        responses.add(responses.GET, ENDPOINT, status=403)

        with pytest.raises(PermanentError):
            fetch_tag_data(ENDPOINT, API_TOKEN)

    def test_raises_retryable_on_connection_error(self, monkeypatch):
        def raise_connection_error(*args, **kwargs):
            raise requests.exceptions.ConnectionError("refused")

        monkeypatch.setattr(requests, "get", raise_connection_error)

        with pytest.raises(RetryableError):
            fetch_tag_data(ENDPOINT, API_TOKEN)
