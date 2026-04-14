"""Reusable boto3 client factory for the data-horizon pipeline."""

import boto3

_client_cache: dict[str, object] = {}


def get_client(service_name: str):
    """Return a cached boto3 client for the given AWS service."""
    if service_name not in _client_cache:
        _client_cache[service_name] = boto3.client(service_name)
    return _client_cache[service_name]
