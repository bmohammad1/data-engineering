"""AWS Lambda entrypoint — adapts the FastAPI ASGI app for Lambda via Mangum."""

import logging

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
    """Lambda entrypoint — logs the AWS request ID for CloudWatch correlation."""
    lambda_request_id = getattr(context, "aws_request_id", "unknown")
    logger.info(
        "Lambda invoked",
        extra={"lambda_request_id": lambda_request_id},
    )
    return _mangum_handler(event, context)
