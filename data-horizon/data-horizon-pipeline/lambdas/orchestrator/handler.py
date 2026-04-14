"""Orchestrator Lambda handler.

Invoked by Child1 Step Function. Generates a run ID, loads tags from S3,
writes run and tag metadata to DynamoDB, and produces Map State input JSON.
"""

import logging
import time
import uuid

from botocore.exceptions import ClientError

import os

from shared.exceptions import PermanentError, RetryableError
from shared.logger import configure_logging, run_id_ctx

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

    run_id = event.get("run_id") or f"RUN-{uuid.uuid4().hex[:12].upper()}"
    run_id_ctx.set(run_id)

    logger.info(
        "Orchestrator started",
        extra={"lambda_request_id": lambda_request_id},
    )

    try:
        from lambdas.orchestrator.config_loader import load_pipeline_config, load_tags_from_s3
        from lambdas.orchestrator.dynamodb_writer import write_run_metadata, write_tag_records
        from lambdas.orchestrator.map_state_generator import generate_map_state_input

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
        "total_tags": len(tags),
    }
