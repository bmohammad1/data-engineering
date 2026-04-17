"""Map State Processor Lambda handler.

Invoked once per tag item by the Data Extractor state machine's distributed Map
State. Fetches tag data from the source API using a Bearer token from Secrets
Manager, writes the raw response to S3, and updates the DynamoDB tag record.
"""

import logging
import os

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
    """Load pipeline config from Secrets Manager."""
    from shared.aws_clients import get_client
    import json

    secret_name = os.environ.get("SECRET_NAME", "")
    sm = get_client("secretsmanager")
    response = sm.get_secret_value(SecretId=secret_name)
    return json.loads(response["SecretString"])


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

    try:
        api_response = fetch_tag_data(endpoint, token)
        records, measurement_count = extract_records(api_response)
        write_raw_response(raw_bucket, run_id, tag_id, records)
        update_tag_status(table_name, run_id, tag_id, STATUS_SUCCESS, measurement_count)

    except (RetryableError, PermanentError) as exc:
        # Update DynamoDB to reflect the failure before letting SF handle retry/catch.
        try:
            update_tag_status(table_name, run_id, tag_id, STATUS_FAILED, 0)
        except Exception:
            logger.warning("Failed to update DynamoDB status to FAILED", extra={"tag_id": tag_id})
        logger.error(
            "map_state_processor failed",
            extra={
                "lambda_request_id": lambda_request_id,
                "run_id": run_id,
                "tag_id": tag_id,
                "status": "FAILED",
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
            "measurement_count": measurement_count,
            "status": "SUCCESS",
        },
    )

    return {
        "tag_id": tag_id,
        "status": STATUS_SUCCESS,
        "records_written": measurement_count,
    }
