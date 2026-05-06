"""Update the DynamoDB tag record after each Lambda invocation."""

from botocore.exceptions import ClientError

from shared.aws_clients import get_client
from shared.constants import PK_RUN_PREFIX, SK_TAG_PREFIX
from shared.exceptions import DynamoDBError


def update_tag_status(
    table_name: str,
    run_id: str,
    tag_id: str,
    status: str,
    records_received: int,
    extraction_duration_ms: int = 0,
) -> None:
    """Update the TAG item for this tag with the result of this invocation.

    Sets overall_status, records_received, extraction_duration_ms, and
    stage_status.EXTRACT; atomically increments attempts by 1. Raises
    DynamoDBError on any DynamoDB failure.
    """
    dynamodb = get_client("dynamodb")

    pk = f"{PK_RUN_PREFIX}{run_id}"
    sk = f"{SK_TAG_PREFIX}{tag_id}"

    try:
        dynamodb.update_item(
            TableName=table_name,
            Key={
                "PK": {"S": pk},
                "SK": {"S": sk},
            },
            UpdateExpression=(
                "SET overall_status = :status, records_received = :count, "
                "stage_status.#EXTRACT = :extract_status "
                "ADD attempts :one, extraction_duration_ms :duration_ms"
            ),
            ExpressionAttributeNames={"#EXTRACT": "EXTRACT"},
            ExpressionAttributeValues={
                ":status":         {"S": status},
                ":count":          {"N": str(records_received)},
                ":extract_status": {"S": status},
                ":duration_ms":    {"N": str(extraction_duration_ms)},
                ":one":            {"N": "1"},
            },
        )
    except ClientError as exc:
        raise DynamoDBError(
            f"Failed to update tag status for {tag_id} in run {run_id}: {exc}",
            service="dynamodb",
            run_id=run_id,
        ) from exc

