"""Call the source API for a single tag and return the parsed response."""

import logging

import requests

from shared.exceptions import PermanentError, RetryableError

logger = logging.getLogger(__name__)

RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}
REQUEST_TIMEOUT_SECONDS = 30


def fetch_tag_data(endpoint: str, token: str) -> dict:
    """GET the tag endpoint and return the parsed JSON body.

    Uses the provided Bearer token for authentication.
    Raises RetryableError on transient HTTP errors or timeouts.
    Raises PermanentError on client errors (4xx except 429).
    """
    headers = {"Authorization": f"Bearer {token}"}

    try:
        response = requests.get(endpoint, headers=headers, timeout=REQUEST_TIMEOUT_SECONDS)
    except requests.exceptions.Timeout as exc:
        raise RetryableError(
            f"Request timed out after {REQUEST_TIMEOUT_SECONDS}s: {endpoint}",
            service="source_api",
        ) from exc
    except requests.exceptions.ConnectionError as exc:
        raise RetryableError(
            f"Connection error reaching {endpoint}: {exc}",
            service="source_api",
        ) from exc

    if response.status_code in RETRYABLE_STATUS_CODES:
        raise RetryableError(
            f"Transient HTTP {response.status_code} from {endpoint}",
            service="source_api",
        )

    if not response.ok:
        raise PermanentError(
            f"HTTP {response.status_code} from {endpoint}: {response.text[:200]}",
            service="source_api",
        )

    logger.debug("API call succeeded", extra={"endpoint": endpoint, "status": response.status_code})
    return response.json()
