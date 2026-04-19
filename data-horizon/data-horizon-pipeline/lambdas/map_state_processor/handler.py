"""Map State Processor Lambda handler.

Invoked once per tag item by the Data Extractor state machine's distributed Map
State. Fetches tag data from the source API using a Bearer token from Secrets
Manager, writes the raw response to S3, and updates the DynamoDB tag record.
"""

import logging
import os
import time

from shared.constants import STATUS_FAILED, STATUS_SUCCESS
from shared.exceptions import PermanentError, RetryableError
from shared.logger import configure_logging, run_id_ctx

from api_client import fetch_tag_data
from dynamodb_updater import update_tag_status
from response_processor import extract_records
from s3_writer import write_raw_response

configure_logging()
logger = logging.getLogger(__name__)


def _load_config() -> dict:
    """Load config: API token from Secrets Manager, non-secrets from env vars."""
    import json
    from shared.aws_clients import get_client

    sm = get_client("secretsmanager")
    secret = json.loads(
        sm.get_secret_value(SecretId=os.environ["SECRET_NAME"])["SecretString"]
    )
    return {
        "source_api_token":     secret["source_api_token"],
        "pipeline_state_table": os.environ["PIPELINE_STATE_TABLE"],
        "raw_bucket_name":      os.environ["RAW_BUCKET_NAME"],
    }


def handler(event: dict, context: object) -> dict:
    """Lambda entrypoint for the map state processor.

    Event shape (from Data Extractor ItemSelector):
        { "run_id": "RUN-...", "tag_id": "TAG-00001", "endpoint": "https://..." }
    """
    run_id = event["run_id"]
    tag_id = event["tag_id"]
    endpoint = event["endpoint"]

    run_id_ctx.set(run_id)
    lambda_request_id = getattr(context, "aws_request_id", "unknown")

    config = _load_config()
    table_name = config["pipeline_state_table"]
    raw_bucket = config["raw_bucket_name"]
    token = config["source_api_token"]

    start = time.perf_counter()
    try:
        api_response = fetch_tag_data(endpoint, token)
        records, total_records = extract_records(api_response)
        write_raw_response(raw_bucket, run_id, tag_id, records)
        duration_ms = round((time.perf_counter() - start) * 1_000, 2)
        update_tag_status(
            table_name, run_id, tag_id, STATUS_SUCCESS, total_records, int(duration_ms)
        )

    except (RetryableError, PermanentError) as exc:
        duration_ms = round((time.perf_counter() - start) * 1_000, 2)
        try:
            update_tag_status(
                table_name, run_id, tag_id, STATUS_FAILED, 0, int(duration_ms)
            )
        except Exception:
            pass
        logger.error(
            "map_state_processor failed",
            extra={
                "lambda_request_id": lambda_request_id,
                "run_id": run_id,
                "tag_id": tag_id,
                "endpoint": endpoint,
                "duration_ms": duration_ms,
                "status": STATUS_FAILED,
                "error": str(exc),
            },
            exc_info=True,
        )
        raise

    logger.info(
        "map_state_processor completed",
        extra={
            "lambda_request_id": lambda_request_id,
            "run_id": run_id,
            "tag_id": tag_id,
            "endpoint": endpoint,
            "total_records": total_records,
            "duration_ms": duration_ms,
            "status": STATUS_SUCCESS,
        },
    )

    return {
        "tag_id": tag_id,
        "status": STATUS_SUCCESS,
        "records_written": total_records,
    }
