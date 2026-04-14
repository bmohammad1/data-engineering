"""Write pipeline run metadata and tag records to DynamoDB."""

import logging
from datetime import datetime, timezone

from botocore.exceptions import ClientError

from shared.aws_clients import get_client
from shared.constants import (
    PK_PIPELINE_RUN,
    SK_METADATA,
    SK_TAG_STATUS_PREFIX,
    STATUS_PENDING,
    STATUS_RUNNING,
)
from shared.exceptions import DynamoDBError

logger = logging.getLogger(__name__)

BATCH_SIZE = 25


def write_run_metadata(table_name: str, run_id: str, total_tags: int) -> None:
    """Create the run metadata record in DynamoDB."""
    dynamodb = get_client("dynamodb")

    item = {
        "PK": {"S": f"{PK_PIPELINE_RUN}{run_id}"},
        "SK": {"S": SK_METADATA},
        "run_id": {"S": run_id},
        "schedule_timestamp": {"S": datetime.now(timezone.utc).isoformat()},
        "total_tags": {"N": str(total_tags)},
        "success_count": {"N": "0"},
        "failure_count": {"N": "0"},
        "elapsed_seconds": {"N": "0"},
        "final_status": {"S": STATUS_RUNNING},
    }

    try:
        dynamodb.put_item(TableName=table_name, Item=item)
    except ClientError as exc:
        raise DynamoDBError(
            f"Failed to write run metadata for {run_id}: {exc}",
            service="dynamodb",
            run_id=run_id,
        )

    logger.info("Run metadata written", extra={"run_id": run_id, "total_tags": total_tags})


def write_tag_records(
    table_name: str,
    run_id: str,
    tags: list[str],
    endpoint_base: str,
) -> None:
    """Write initial tag status records to DynamoDB using batch writes."""
    dynamodb = get_client("dynamodb")
    pk = f"{PK_PIPELINE_RUN}{run_id}"

    items = []
    for tag_id in tags:
        items.append({
            "PutRequest": {
                "Item": {
                    "PK": {"S": pk},
                    "SK": {"S": f"{SK_TAG_STATUS_PREFIX}{tag_id}"},
                    "tag_id": {"S": tag_id},
                    "endpoint": {"S": f"{endpoint_base}/tag/{tag_id}"},
                    "attempts": {"N": "0"},
                    "records_received": {"N": "0"},
                    "final_status": {"S": STATUS_PENDING},
                    "error_code": {"NULL": True},
                    "error_message": {"NULL": True},
                }
            }
        })

    written = 0
    for i in range(0, len(items), BATCH_SIZE):
        batch = items[i : i + BATCH_SIZE]
        request = {table_name: batch}

        try:
            response = dynamodb.batch_write_item(RequestItems=request)
            unprocessed = response.get("UnprocessedItems", {})

            retries = 0
            while unprocessed and retries < 3:
                retries += 1
                response = dynamodb.batch_write_item(RequestItems=unprocessed)
                unprocessed = response.get("UnprocessedItems", {})

            if unprocessed:
                raise DynamoDBError(
                    f"Unprocessed items remain after retries for batch starting at {i}",
                    service="dynamodb",
                    run_id=run_id,
                )

        except ClientError as exc:
            raise DynamoDBError(
                f"Batch write failed at offset {i}: {exc}",
                service="dynamodb",
                run_id=run_id,
            )

        written += len(batch)

    logger.info(
        "Tag records written",
        extra={"run_id": run_id, "tag_count": written},
    )
