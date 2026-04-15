"""Normalise the raw API response into a storable payload."""

import logging

logger = logging.getLogger(__name__)


def extract_records(api_response: dict) -> tuple[list[dict], int]:
    """Extract the full response payload and measurement count.

    The source API returns a rich tag object with nested arrays (measurements,
    alarms, events, etc.). We store the entire response as a single JSON document
    so downstream Glue jobs have all the data. The measurement count is used as
    the records_received metric in DynamoDB.

    Returns:
        A tuple of (records_to_store, measurement_count) where records_to_store
        is a list containing the full API response and measurement_count is the
        length of the measurements array.
    """
    measurement_count = len(api_response.get("measurements", []))
    logger.debug("Records extracted", extra={"measurement_count": measurement_count})
    return [api_response], measurement_count
