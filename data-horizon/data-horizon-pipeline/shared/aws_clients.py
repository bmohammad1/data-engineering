"""Reusable boto3 client factory for the data-horizon pipeline."""

import boto3

_client_cache: dict[str, object] = {}


def get_client(service_name: str):
    """Return a cached boto3 client for the given AWS service."""
    if service_name not in _client_cache:
        _client_cache[service_name] = boto3.client(service_name)
    return _client_cache[service_name]


def put_pipeline_metric(
    metric_name: str,
    value: float,
    unit: str,
    namespace: str,
    run_id: str,
) -> None:
    """Emit a single custom metric to CloudWatch.

    Unit must be a valid CloudWatch unit string:
    Count, Milliseconds, Percent, Bytes, None, etc.
    """
    cloudwatch = get_client("cloudwatch")
    cloudwatch.put_metric_data(
        Namespace=namespace,
        MetricData=[
            {
                "MetricName": metric_name,
                "Dimensions": [{"Name": "RunId", "Value": run_id}],
                "Value": value,
                "Unit": unit,
            }
        ],
    )
