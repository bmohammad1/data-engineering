"""Normalise the raw API response into a storable payload."""


def extract_records(api_response: dict) -> tuple[list[dict], int]:
    """Extract the full response payload and total record count.

    The source API returns a rich tag object with nested arrays (measurements,
    alarms, events, etc.). We store the entire response as a single JSON document
    so downstream Glue jobs have all the data. The total record count sums the
    lengths of all list-typed fields so records_received in DynamoDB reflects
    every data object returned, not just measurements.

    Returns:
        A tuple of (records_to_store, total_records) where records_to_store is a
        list containing the full API response and total_records is the combined
        count across all list-typed fields in the response.
    """
    total_records = sum(len(v) for v in api_response.values() if isinstance(v, list))
    return [api_response], total_records
