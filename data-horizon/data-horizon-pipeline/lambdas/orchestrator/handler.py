"""Config Loader Lambda handler.

Invoked by the Config Loader state machine. Accepts a run_id from the event,
loads tags from S3, writes run and tag metadata to DynamoDB, produces Map State
input JSON, and returns the S3 key and concurrency value for the extraction step.
"""

import logging
import os
import time

from botocore.exceptions import ClientError

from shared.exceptions import PermanentError, RetryableError
from shared.logger import configure_logging, run_id_ctx

from config_loader import load_pipeline_config, load_tags_from_s3
from dynamodb_writer import write_config_stage_end, write_run_metadata, write_tag_records
from map_state_generator import generate_map_state_input

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
    """Lambda entrypoint for the config loader."""
    start = time.perf_counter()
    lambda_request_id = getattr(context, "aws_request_id", "unknown")
    run_id = event.get("run_id")

    if not run_id:
        raise PermanentError(
            "run_id is required in the event payload",
            service="lambda",
        )

    run_id_ctx.set(run_id)
    environment = os.environ.get("ENVIRONMENT", "dev")

    try:
        secret_name = os.environ.get("SECRET_NAME", "")
        config = load_pipeline_config(secret_name)

        # Non-secret config comes from env vars; inject into config so helpers
        # (load_tags_from_s3, generate_map_state_input) can still read from it.
        config["config_bucket_name"]    = os.environ["CONFIG_BUCKET_NAME"]
        config["source_api_base_url"]   = os.environ.get("SOURCE_API_BASE_URL", "")

        tags = load_tags_from_s3(config)

        table_name           = os.environ["PIPELINE_STATE_TABLE"]
        orchestration_bucket = os.environ["ORCHESTRATION_BUCKET_NAME"]

        write_run_metadata(table_name, run_id, total_tags=len(tags), environment=environment)

        endpoint_base = config["source_api_base_url"]
        write_tag_records(table_name, run_id, tags, endpoint_base)

        map_items_s3_key = generate_map_state_input(
            bucket=orchestration_bucket,
            run_id=run_id,
            tags=tags,
            config=config,
        )

    except (RetryableError, PermanentError) as exc:
        logger.error(
            "config_loader failed",
            extra={
                "lambda_request_id": lambda_request_id,
                "run_id": run_id,
                "duration_ms": round((time.perf_counter() - start) * 1_000, 2),
                "status": "FAILED",
                "error": str(exc),
            },
            exc_info=True,
        )
        raise
    except ClientError as exc:
        error_code = exc.response["Error"]["Code"]
        logger.error(
            "config_loader failed",
            extra={
                "lambda_request_id": lambda_request_id,
                "run_id": run_id,
                "duration_ms": round((time.perf_counter() - start) * 1_000, 2),
                "status": "FAILED",
                "error": str(exc),
            },
            exc_info=True,
        )
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
    write_config_stage_end(table_name, run_id, int(duration_ms))

    logger.info(
        "config_loader completed",
        extra={
            "lambda_request_id": lambda_request_id,
            "run_id": run_id,
            "total_tags": len(tags),
            "map_items_s3_key": map_items_s3_key,
            "duration_ms": duration_ms,
            "status": "SUCCESS",
        },
    )

    return {
        "run_id": run_id,
        "map_items_s3_key": map_items_s3_key,
        "concurrency": int(os.environ.get("MAP_STATE_CONCURRENCY", "5")),
    }
