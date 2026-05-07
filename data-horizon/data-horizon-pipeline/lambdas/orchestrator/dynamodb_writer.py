"""Write pipeline run metadata and tag records to the pipeline_audit schema."""

import logging
from datetime import datetime, timedelta, timezone

import psycopg
from botocore.exceptions import ClientError

from shared.aws_clients import get_client
from shared.constants import (
    GSI1PK_PIPELINE_PREFIX,
    PK_RUN_PREFIX,
    SK_META,
    SK_TAG_PREFIX,
    STATUS_PENDING,
    STATUS_RUNNING,
)
from shared.exceptions import DynamoDBError, PostgresError
from shared.pg_connection import get_pg_pool

logger = logging.getLogger(__name__)

BATCH_SIZE = 25
PIPELINE_NAME = "data_horizon"
TTL_DAYS = 30


def write_run_metadata(
    table_name: str,
    run_id: str,
    total_tags: int,
    environment: str,
) -> None:
    """Create the run metadata record."""

    # --- DynamoDB (commented out — preserved for rollback) ---
    # dynamodb = get_client("dynamodb")
    # now_iso = datetime.now(timezone.utc).isoformat()
    # ttl_epoch = int((datetime.now(timezone.utc) + timedelta(days=TTL_DAYS)).timestamp())
    # item = {
    #     "PK":            {"S": f"{PK_RUN_PREFIX}{run_id}"},
    #     "SK":            {"S": SK_META},
    #     "run_id":        {"S": run_id},
    #     "pipeline_name": {"S": PIPELINE_NAME},
    #     "environment":   {"S": environment},
    #     "overall_status": {"S": STATUS_RUNNING},
    #     "start_time":    {"S": now_iso},
    #     "trigger_type":  {"S": "schedule"},
    #     "total_tags":    {"N": str(total_tags)},
    #     "created_at":    {"S": now_iso},
    #     "ttl":           {"N": str(ttl_epoch)},
    #     "GSI1PK":        {"S": f"{GSI1PK_PIPELINE_PREFIX}{PIPELINE_NAME}"},
    #     "GSI1SK":        {"S": now_iso},
    # }
    # try:
    #     dynamodb.put_item(TableName=table_name, Item=item)
    # except ClientError as exc:
    #     raise DynamoDBError(
    #         f"Failed to write run metadata for {run_id}: {exc}",
    #         service="dynamodb",
    #         run_id=run_id,
    #     )
    # --- End DynamoDB ---

    # --- PostgreSQL ---
    pool = get_pg_pool()
    try:
        with pool.connection() as conn:
            conn.execute(
                """
                INSERT INTO pipeline_audit.pipeline_runs
                    (run_id, pipeline_name, environment, overall_status,
                     start_time, trigger_type, total_tags, created_at)
                VALUES (%s, %s, %s, %s, NOW(), %s, %s, NOW())
                ON CONFLICT (run_id) DO NOTHING
                """,
                (run_id, PIPELINE_NAME, environment, STATUS_RUNNING, "schedule", total_tags),
            )
    except psycopg.Error as exc:
        raise PostgresError(
            f"Failed to write run metadata for {run_id}: {exc}",
            service="postgres",
            run_id=run_id,
        ) from exc
    # --- End PostgreSQL ---

    logger.debug(
        "Run metadata written",
        extra={"run_id": run_id, "total_tags": total_tags},
    )


def write_tag_records(
    table_name: str,
    run_id: str,
    tags: list[str],
    endpoint_base: str,
) -> None:
    """Write initial tag status records in a single transaction."""

    # --- DynamoDB (commented out — preserved for rollback) ---
    # dynamodb = get_client("dynamodb")
    # pk = f"{PK_RUN_PREFIX}{run_id}"
    # now_iso = datetime.now(timezone.utc).isoformat()
    # ttl_epoch = int((datetime.now(timezone.utc) + timedelta(days=TTL_DAYS)).timestamp())
    # items = []
    # for tag_id in tags:
    #     items.append({
    #         "PutRequest": {
    #             "Item": {
    #                 "PK":             {"S": pk},
    #                 "SK":             {"S": f"{SK_TAG_PREFIX}{tag_id}"},
    #                 "run_id":         {"S": run_id},
    #                 "tag_key":        {"S": tag_id},
    #                 "pipeline_name":  {"S": PIPELINE_NAME},
    #                 "overall_status": {"S": STATUS_PENDING},
    #                 "stage_status":   {"M": {
    #                     "EXTRACT":   {"S": STATUS_PENDING},
    #                     "TRANSFORM": {"S": STATUS_PENDING},
    #                     "VALIDATE":  {"S": STATUS_PENDING},
    #                 }},
    #                 "endpoint":       {"S": f"{endpoint_base}/tag/{tag_id}"},
    #                 "created_at":     {"S": now_iso},
    #                 "ttl":            {"N": str(ttl_epoch)},
    #             }
    #         }
    #     })
    # written = 0
    # for i in range(0, len(items), BATCH_SIZE):
    #     batch = items[i : i + BATCH_SIZE]
    #     request = {table_name: batch}
    #     try:
    #         response = dynamodb.batch_write_item(RequestItems=request)
    #         unprocessed = response.get("UnprocessedItems", {})
    #         retries = 0
    #         while unprocessed and retries < 3:
    #             retries += 1
    #             response = dynamodb.batch_write_item(RequestItems=unprocessed)
    #             unprocessed = response.get("UnprocessedItems", {})
    #         if unprocessed:
    #             raise DynamoDBError(
    #                 f"Unprocessed items remain after retries for batch starting at {i}",
    #                 service="dynamodb",
    #                 run_id=run_id,
    #             )
    #     except ClientError as exc:
    #         raise DynamoDBError(
    #             f"Batch write failed at offset {i}: {exc}",
    #             service="dynamodb",
    #             run_id=run_id,
    #         )
    #     written += len(batch)
    # --- End DynamoDB ---

    # --- PostgreSQL ---
    rows = [
        (run_id, tag_id, PIPELINE_NAME, f"{endpoint_base}/tag/{tag_id}")
        for tag_id in tags
    ]
    pool = get_pg_pool()
    try:
        with pool.connection() as conn:
            conn.executemany(
                """
                INSERT INTO pipeline_audit.pipeline_tags
                    (run_id, tag_key, pipeline_name, endpoint,
                     overall_status,
                     stage_extract_status, stage_transform_status, stage_validate_status,
                     created_at)
                VALUES (%s, %s, %s, %s, 'PENDING', 'PENDING', 'PENDING', 'PENDING', NOW())
                ON CONFLICT (run_id, tag_key) DO NOTHING
                """,
                rows,
            )
    except psycopg.Error as exc:
        raise PostgresError(
            f"Bulk tag insert failed for run {run_id}: {exc}",
            service="postgres",
            run_id=run_id,
        ) from exc
    # --- End PostgreSQL ---

    logger.debug(
        "Tag records written",
        extra={"run_id": run_id, "tag_count": len(tags)},
    )


def write_config_stage_end(table_name: str, run_id: str, duration_ms: int) -> None:
    """Record Child1 completion time and duration."""

    # --- DynamoDB (commented out — preserved for rollback) ---
    # dynamodb = get_client("dynamodb")
    # now_iso = datetime.now(timezone.utc).isoformat()
    # try:
    #     dynamodb.update_item(
    #         TableName=table_name,
    #         Key={"PK": {"S": f"{PK_RUN_PREFIX}{run_id}"}, "SK": {"S": SK_META}},
    #         UpdateExpression="SET config_end_time = :t, config_duration_ms = :d",
    #         ExpressionAttributeValues={
    #             ":t": {"S": now_iso},
    #             ":d": {"N": str(duration_ms)},
    #         },
    #     )
    # except ClientError as exc:
    #     raise DynamoDBError(
    #         f"Failed to write config stage end for {run_id}: {exc}",
    #         service="dynamodb",
    #         run_id=run_id,
    #     )
    # --- End DynamoDB ---

    # --- PostgreSQL ---
    pool = get_pg_pool()
    try:
        with pool.connection() as conn:
            conn.execute(
                """
                UPDATE pipeline_audit.pipeline_runs
                   SET config_end_time    = NOW(),
                       config_duration_ms = %s
                 WHERE run_id = %s
                """,
                (duration_ms, run_id),
            )
    except psycopg.Error as exc:
        raise PostgresError(
            f"Failed to write config stage end for {run_id}: {exc}",
            service="postgres",
            run_id=run_id,
        ) from exc
    # --- End PostgreSQL ---

    logger.debug(
        "Config stage end recorded",
        extra={"run_id": run_id, "config_duration_ms": duration_ms},
    )
