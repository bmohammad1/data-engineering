"""Update the tag record after each Lambda extraction invocation."""

import psycopg
from botocore.exceptions import ClientError

from shared.aws_clients import get_client
from shared.constants import PK_RUN_PREFIX, SK_TAG_PREFIX
from shared.exceptions import DynamoDBError, PostgresError
from shared.pg_connection import get_pg_pool


def update_tag_status(
    table_name: str,
    run_id: str,
    tag_id: str,
    status: str,
    records_received: int,
    extraction_duration_ms: int = 0,
) -> None:
    """Update the tag row with the result of this extraction invocation.

    Sets overall_status, records_received, stage_extract_status; atomically
    accumulates extraction_duration_ms and increments attempts.
    """

    # --- DynamoDB (commented out — preserved for rollback) ---
    # dynamodb = get_client("dynamodb")
    # pk = f"{PK_RUN_PREFIX}{run_id}"
    # sk = f"{SK_TAG_PREFIX}{tag_id}"
    # try:
    #     dynamodb.update_item(
    #         TableName=table_name,
    #         Key={
    #             "PK": {"S": pk},
    #             "SK": {"S": sk},
    #         },
    #         UpdateExpression=(
    #             "SET overall_status = :status, records_received = :count, "
    #             "stage_status.#EXTRACT = :extract_status "
    #             "ADD attempts :one, extraction_duration_ms :duration_ms"
    #         ),
    #         ExpressionAttributeNames={"#EXTRACT": "EXTRACT"},
    #         ExpressionAttributeValues={
    #             ":status":         {"S": status},
    #             ":count":          {"N": str(records_received)},
    #             ":extract_status": {"S": status},
    #             ":duration_ms":    {"N": str(extraction_duration_ms)},
    #             ":one":            {"N": "1"},
    #         },
    #     )
    # except ClientError as exc:
    #     raise DynamoDBError(
    #         f"Failed to update tag status for {tag_id} in run {run_id}: {exc}",
    #         service="dynamodb",
    #         run_id=run_id,
    #     ) from exc
    # --- End DynamoDB ---

    # --- PostgreSQL ---
    pool = get_pg_pool()
    try:
        with pool.connection() as conn:
            conn.execute(
                """
                UPDATE pipeline_audit.pipeline_tags
                   SET overall_status         = %s,
                       records_received       = %s,
                       stage_extract_status   = %s,
                       -- Replaces DynamoDB ADD: accumulates across retries
                       extraction_duration_ms = extraction_duration_ms + %s,
                       attempts               = attempts + 1
                 WHERE run_id  = %s
                   AND tag_key = %s
                """,
                (status, records_received, status, extraction_duration_ms, run_id, tag_id),
            )
    except psycopg.Error as exc:
        raise PostgresError(
            f"Failed to update tag status for {tag_id} in run {run_id}: {exc}",
            service="postgres",
            run_id=run_id,
        ) from exc
    # --- End PostgreSQL ---
