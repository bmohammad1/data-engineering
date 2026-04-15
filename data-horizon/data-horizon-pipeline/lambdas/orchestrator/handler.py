"""Orchestrator Lambda handler.

Invoked by Child1 Step Function. Accepts a run_id from the event, loads tags
from S3, writes run and tag metadata to DynamoDB, produces Map State input JSON,
and returns the S3 key and concurrency value for the extraction step.
"""

import logging
import os
import time

from botocore.exceptions import ClientError

from shared.exceptions import PermanentError, RetryableError
from shared.logger import configure_logging, run_id_ctx

from .config_loader import load_pipeline_config, load_tags_from_s3
from .dynamodb_writer import write_run_metadata, write_tag_records
from .map_state_generator import generate_map_state_input

configure_logging()
logger = logging.getLogger(__name__)

RETRYABLE_ERROR_CODES = {
    "ThrottlingException",
    "ProvisionedThroughputExceededException",
    "RequestLimitExceeded",
    "ServiceUnavailable",
    "InternalServerError",
}


def handler(event: dict, context: object) -> dict:
    """Lambda entrypoint for the orchestrator."""
    start = time.perf_counter()
    lambda_request_id = getattr(context, "aws_request_id", "unknown")

    run_id = event.get("run_id")
    if not run_id:
        raise PermanentError(
            "run_id is required in the event payload",
            service="lambda",
        )

    run_id_ctx.set(run_id)

    logger.info(
        "Orchestrator started",
        extra={"lambda_request_id": lambda_request_id},
    )

    try:
        secret_name = os.environ.get("SECRET_NAME", "")
        config = load_pipeline_config(secret_name)
        tags = load_tags_from_s3(config)

        table_name = config["pipeline_state_table"]
        write_run_metadata(table_name, run_id, total_tags=len(tags))

        endpoint_base = config.get("source_api_base_url", "")
        write_tag_records(table_name, run_id, tags, endpoint_base)

        orchestration_bucket = config["orchestration_bucket_name"]
        map_items_s3_key = generate_map_state_input(
            bucket=orchestration_bucket,
            run_id=run_id,
            tags=tags,
            config=config,
        )

    except (RetryableError, PermanentError):
        raise
    except ClientError as exc:
        error_code = exc.response["Error"]["Code"]
        if error_code in RETRYABLE_ERROR_CODES:
            raise RetryableError(
                f"Transient AWS error: {exc}",
                service=exc.operation_name,
                run_id=run_id,
            )
        raise PermanentError(
            f"Non-retryable AWS error: {exc}",
            service=exc.operation_name,
            run_id=run_id,
        )

    duration_ms = round((time.perf_counter() - start) * 1_000, 2)
    logger.info(
        "Orchestrator completed",
        extra={
            "lambda_request_id": lambda_request_id,
            "total_tags": len(tags),
            "duration_ms": duration_ms,
        },
    )

    return {
        "run_id": run_id,
        "map_items_s3_key": map_items_s3_key,
        "concurrency": config.get("map_state_concurrency", 5),
    }
