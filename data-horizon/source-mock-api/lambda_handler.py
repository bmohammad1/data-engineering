"""AWS Lambda entrypoint — adapts the FastAPI ASGI app for Lambda via Mangum."""

import logging
import time

from mangum import Mangum

from app.logging_config import configure_logging
from app.main import app

# Configure structured JSON logging before the first invocation.
configure_logging()

logger = logging.getLogger(__name__)

# lifespan="off" because Lambda manages its own lifecycle; ASGI startup/shutdown
# events are not meaningful in a serverless context.
_mangum_handler = Mangum(app, lifespan="off")


def handler(event: dict, context: object) -> dict:
    """Lambda entrypoint — logs the AWS request ID and total execution time."""
    lambda_request_id = getattr(context, "aws_request_id", "unknown")
    remaining_ms = getattr(context, "get_remaining_time_in_millis", lambda: 0)()

    logger.info(
        "Lambda invoked",
        extra={
            "lambda_request_id": lambda_request_id,
            "remaining_time_ms": remaining_ms,
        },
    )

    start = time.perf_counter()
    response = _mangum_handler(event, context)
    execution_ms = round((time.perf_counter() - start) * 1_000, 2)

    logger.info(
        "Lambda execution completed",
        extra={
            "lambda_request_id": lambda_request_id,
            "execution_ms": execution_ms,
            "status_code": response.get("statusCode"),
        },
    )

    return response
