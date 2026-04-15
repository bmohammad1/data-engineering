"""Update the DynamoDB tag status record after each Lambda invocation."""

import logging

from botocore.exceptions import ClientError

from shared.aws_clients import get_client
from shared.constants import PK_PIPELINE_RUN, SK_TAG_STATUS_PREFIX
from shared.exceptions import DynamoDBError

logger = logging.getLogger(__name__)


def update_tag_status(
    table_name: str,
    run_id: str,
    tag_id: str,
    status: str,
    records_received: int,
) -> None:
    """Update the TAG_STATUS item for this tag with the result of this invocation.

    Sets final_status and records_received; atomically increments attempts by 1.
    Raises DynamoDBError on any DynamoDB failure.
    """
    dynamodb = get_client("dynamodb")

    pk = f"{PK_PIPELINE_RUN}{run_id}"
    sk = f"{SK_TAG_STATUS_PREFIX}{tag_id}"

    try:
        dynamodb.update_item(
            TableName=table_name,
            Key={
                "PK": {"S": pk},
                "SK": {"S": sk},
            },
            UpdateExpression=(
                "SET final_status = :status, records_received = :count "
                "ADD attempts :one"
            ),
            ExpressionAttributeValues={
                ":status": {"S": status},
                ":count": {"N": str(records_received)},
                ":one": {"N": "1"},
            },
        )
    except ClientError as exc:
        raise DynamoDBError(
            f"Failed to update tag status for {tag_id} in run {run_id}: {exc}",
            service="dynamodb",
            run_id=run_id,
        ) from exc

    logger.debug(
        "Tag status updated",
        extra={"tag_id": tag_id, "status": status, "records_received": records_received},
    )
