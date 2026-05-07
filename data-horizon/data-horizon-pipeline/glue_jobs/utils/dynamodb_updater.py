"""Database status updates for the TRANSFORM and VALIDATE pipeline stages.

Updates mirror the pattern established in
lambdas/map_state_processor/dynamodb_updater.py.

Two levels are updated after each Glue job:
  - Per-tag: stage_extract/transform/validate_status on the pipeline_tags row
  - Per-run: transform_status / validate_status + aggregate counts on pipeline_runs

Failure reasons are captured in structured logs (logger.error), not in the DB.
Per-tag rows carry record counts so operators can track data volume per tag.
"""

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import psycopg
from botocore.exceptions import ClientError

from shared.aws_clients import get_client
from shared.constants import PK_RUN_PREFIX, SK_META, SK_TAG_PREFIX, STATUS_RUNNING
from shared.exceptions import DynamoDBError, PostgresError
from shared.pg_connection import get_pg_pool

logger = logging.getLogger(__name__)

# Number of concurrent threads for per-tag database updates.
# At 50 threads with ~10ms per update round-trip, 5,000 tags complete
# in ~1 second instead of ~50 seconds sequentially.
_DB_WRITE_CONCURRENCY = 50


# ---------------------------------------------------------------------------
# Bulk concurrent update helpers
# ---------------------------------------------------------------------------


def bulk_update_tag_transform_status(
    table_name: str,
    run_id: str,
    tag_updates: list[dict],
) -> None:
    """Update TRANSFORM stage status on tag rows for all tags concurrently.

    Each entry in tag_updates must contain:
        tag_id, status, records_extracted, records_dropped, records_transformed

    A failure for any individual tag logs a warning and does not abort the
    remaining updates — fault isolation is preserved.
    """
    if not tag_updates:
        return

    with ThreadPoolExecutor(max_workers=_DB_WRITE_CONCURRENCY) as executor:
        future_to_tag_id = {}

        for update in tag_updates:
            tag_id = update["tag_id"]
            future = executor.submit(
                update_tag_transform_status,
                table_name,
                run_id,
                tag_id,
                update["status"],
                update.get("records_extracted", 0),
                update.get("records_dropped", 0),
                update.get("records_transformed", 0),
            )
            future_to_tag_id[future] = tag_id

        for future in as_completed(future_to_tag_id):
            tag_id = future_to_tag_id[future]
            exc = future.exception()
            if exc:
                logger.warning(
                    "Failed to update database TRANSFORM status for tag — continuing",
                    extra={"run_id": run_id, "tag_id": tag_id, "error": str(exc)},
                )

    logger.debug(
        "Bulk TRANSFORM status update complete",
        extra={"run_id": run_id, "tag_count": len(tag_updates)},
    )


def bulk_update_tag_validate_status(
    table_name: str,
    run_id: str,
    tag_updates: list[dict],
) -> None:
    """Update VALIDATE stage status on tag rows for all tags concurrently.

    Each entry in tag_updates must contain:
        tag_id, status, valid_count, invalid_count
    """
    if not tag_updates:
        return

    with ThreadPoolExecutor(max_workers=_DB_WRITE_CONCURRENCY) as executor:
        future_to_tag_id = {}

        for update in tag_updates:
            tag_id = update["tag_id"]
            future = executor.submit(
                update_tag_validate_status,
                table_name,
                run_id,
                tag_id,
                update["status"],
                update.get("valid_count", 0),
                update.get("invalid_count", 0),
            )
            future_to_tag_id[future] = tag_id

        for future in as_completed(future_to_tag_id):
            tag_id = future_to_tag_id[future]
            exc = future.exception()
            if exc:
                logger.warning(
                    "Failed to update database VALIDATE status for tag — continuing",
                    extra={"run_id": run_id, "tag_id": tag_id, "error": str(exc)},
                )


# ---------------------------------------------------------------------------
# Per-run tag fetch
# ---------------------------------------------------------------------------


def fetch_transform_succeeded_tags(table_name: str, run_id: str) -> set[str]:
    """Return tag IDs that completed the transform stage successfully.

    Used by the validation job to seed its tag universe rather than
    deriving it from S3 file contents.
    """

    # --- DynamoDB (commented out — preserved for rollback) ---
    # dynamodb = get_client("dynamodb")
    # partition_key_value = f"{PK_RUN_PREFIX}{run_id}"
    # tag_ids: set[str] = set()
    # query_params: dict = {
    #     "TableName": table_name,
    #     "KeyConditionExpression": "PK = :pk AND begins_with(SK, :sk_prefix)",
    #     "FilterExpression": "overall_status = :success",
    #     "ExpressionAttributeValues": {
    #         ":pk":        {"S": partition_key_value},
    #         ":sk_prefix": {"S": SK_TAG_PREFIX},
    #         ":success":   {"S": "SUCCESS"},
    #     },
    # }
    # try:
    #     while True:
    #         response = dynamodb.query(**query_params)
    #         for item in response.get("Items", []):
    #             sort_key_value = item["SK"]["S"]
    #             tag_id = sort_key_value.removeprefix(SK_TAG_PREFIX)
    #             tag_ids.add(tag_id)
    #         last_evaluated_key = response.get("LastEvaluatedKey")
    #         if last_evaluated_key is None:
    #             break
    #         query_params["ExclusiveStartKey"] = last_evaluated_key
    # except ClientError as exc:
    #     raise DynamoDBError(
    #         f"Failed to fetch transform-succeeded tags for run {run_id}: {exc}",
    #         service="dynamodb",
    #         run_id=run_id,
    #     ) from exc
    # --- End DynamoDB ---

    # --- PostgreSQL ---
    pool = get_pg_pool()
    try:
        with pool.connection() as conn:
            rows = conn.execute(
                """
                SELECT tag_key
                  FROM pipeline_audit.pipeline_tags
                 WHERE run_id         = %s
                   AND overall_status = 'SUCCESS'
                """,
                (run_id,),
            ).fetchall()
        tag_ids = {row["tag_key"] for row in rows}
    except psycopg.Error as exc:
        raise PostgresError(
            f"Failed to fetch transform-succeeded tags for run {run_id}: {exc}",
            service="postgres",
            run_id=run_id,
        ) from exc
    # --- End PostgreSQL ---

    logger.debug(
        "Fetched transform-succeeded tags",
        extra={"run_id": run_id, "tag_count": len(tag_ids)},
    )
    return tag_ids


# ---------------------------------------------------------------------------
# Per-tag TRANSFORM updates
# ---------------------------------------------------------------------------


def update_tag_transform_status(
    table_name: str,
    run_id: str,
    tag_id: str,
    status: str,
    records_extracted: int = 0,
    records_dropped: int = 0,
    records_transformed: int = 0,
    duration_ms: int = 0,
) -> None:
    """Set TRANSFORM status on a tag row with full record counts.

    Args:
        records_extracted:   Total records read from raw JSON across all tables.
        records_dropped:     Records silently dropped (null PK / null TagID after cast).
        records_transformed: Records successfully written to cleaned bucket.
    """

    # --- DynamoDB (commented out — preserved for rollback) ---
    # dynamodb = get_client("dynamodb")
    # pk = f"{PK_RUN_PREFIX}{run_id}"
    # sk = f"{SK_TAG_PREFIX}{tag_id}"
    # set_clause = (
    #     "SET overall_status = :status"
    #     ", stage_status.#TRANSFORM = :stage_status"
    #     ", transform_records_extracted = :extracted"
    #     ", transform_records_dropped = :dropped"
    #     ", transform_records_written = :written"
    # )
    # expression_values = {
    #     ":status":        {"S": status},
    #     ":stage_status":  {"S": status},
    #     ":extracted":     {"N": str(records_extracted)},
    #     ":dropped":       {"N": str(records_dropped)},
    #     ":written":       {"N": str(records_transformed)},
    # }
    # if duration_ms:
    #     expression_values[":dur_ms"] = {"N": str(duration_ms)}
    #     update_expr = set_clause + " ADD transform_duration_ms :dur_ms"
    # else:
    #     update_expr = set_clause
    # try:
    #     dynamodb.update_item(
    #         TableName=table_name,
    #         Key={"PK": {"S": pk}, "SK": {"S": sk}},
    #         UpdateExpression=update_expr,
    #         ExpressionAttributeNames={"#TRANSFORM": "TRANSFORM"},
    #         ExpressionAttributeValues=expression_values,
    #     )
    # except ClientError as exc:
    #     raise DynamoDBError(
    #         f"Failed to update TRANSFORM status for tag {tag_id} in run {run_id}: {exc}",
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
                   SET overall_status              = %s,
                       stage_transform_status      = %s,
                       transform_records_extracted = %s,
                       transform_records_dropped   = %s,
                       transform_records_written   = %s,
                       -- Replaces DynamoDB ADD: accumulates across retries
                       transform_duration_ms       = transform_duration_ms + %s
                 WHERE run_id  = %s
                   AND tag_key = %s
                """,
                (status, status, records_extracted, records_dropped,
                 records_transformed, duration_ms, run_id, tag_id),
            )
    except psycopg.Error as exc:
        raise PostgresError(
            f"Failed to update TRANSFORM status for tag {tag_id} in run {run_id}: {exc}",
            service="postgres",
            run_id=run_id,
        ) from exc
    # --- End PostgreSQL ---

    logger.debug(
        "Tag TRANSFORM status updated",
        extra={
            "run_id": run_id,
            "tag_id": tag_id,
            "status": status,
            "records_extracted": records_extracted,
            "records_dropped": records_dropped,
            "records_transformed": records_transformed,
        },
    )


# ---------------------------------------------------------------------------
# Per-tag VALIDATE updates
# ---------------------------------------------------------------------------


def update_tag_validate_status(
    table_name: str,
    run_id: str,
    tag_id: str,
    status: str,
    valid_count: int = 0,
    invalid_count: int = 0,
    duration_ms: int = 0,
) -> None:
    """Set VALIDATE status on a tag row with full record counts.

    Args:
        valid_count:   Records that passed all validation rules.
        invalid_count: Records that failed at least one rule → quarantined.
    """

    # --- DynamoDB (commented out — preserved for rollback) ---
    # dynamodb = get_client("dynamodb")
    # pk = f"{PK_RUN_PREFIX}{run_id}"
    # sk = f"{SK_TAG_PREFIX}{tag_id}"
    # set_clause = (
    #     "SET overall_status = :status"
    #     ", stage_status.#VALIDATE = :stage_status"
    #     ", validate_records_passed = :valid"
    #     ", validate_records_quarantined = :invalid"
    # )
    # expression_values = {
    #     ":status":       {"S": status},
    #     ":stage_status": {"S": status},
    #     ":valid":        {"N": str(valid_count)},
    #     ":invalid":      {"N": str(invalid_count)},
    # }
    # if duration_ms:
    #     expression_values[":dur_ms"] = {"N": str(duration_ms)}
    #     update_expr = set_clause + " ADD validate_duration_ms :dur_ms"
    # else:
    #     update_expr = set_clause
    # try:
    #     dynamodb.update_item(
    #         TableName=table_name,
    #         Key={"PK": {"S": pk}, "SK": {"S": sk}},
    #         UpdateExpression=update_expr,
    #         ExpressionAttributeNames={"#VALIDATE": "VALIDATE"},
    #         ExpressionAttributeValues=expression_values,
    #     )
    # except ClientError as exc:
    #     raise DynamoDBError(
    #         f"Failed to update VALIDATE status for tag {tag_id} in run {run_id}: {exc}",
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
                   SET overall_status               = %s,
                       stage_validate_status        = %s,
                       validate_records_passed      = %s,
                       validate_records_quarantined = %s,
                       -- Replaces DynamoDB ADD: accumulates across retries
                       validate_duration_ms         = validate_duration_ms + %s
                 WHERE run_id  = %s
                   AND tag_key = %s
                """,
                (status, status, valid_count, invalid_count, duration_ms, run_id, tag_id),
            )
    except psycopg.Error as exc:
        raise PostgresError(
            f"Failed to update VALIDATE status for tag {tag_id} in run {run_id}: {exc}",
            service="postgres",
            run_id=run_id,
        ) from exc
    # --- End PostgreSQL ---

    logger.debug(
        "Tag VALIDATE status updated",
        extra={
            "run_id": run_id,
            "tag_id": tag_id,
            "status": status,
            "valid_count": valid_count,
            "invalid_count": invalid_count,
        },
    )


# ---------------------------------------------------------------------------
# Per-run (pipeline_runs row) updates
# ---------------------------------------------------------------------------


def update_run_transform_status(
    table_name: str,
    run_id: str,
    status: str,
    transform_tags_success: int = 0,
    transform_tags_failed: int = 0,
    records_extracted: int = 0,
    records_transformed: int = 0,
    records_dropped: int = 0,
    duration_ms: int = 0,
) -> None:
    """Update the pipeline_runs row with transform stage aggregate outcomes."""

    # --- DynamoDB (commented out — preserved for rollback) ---
    # dynamodb = get_client("dynamodb")
    # pk = f"{PK_RUN_PREFIX}{run_id}"
    # job_is_still_running = status == STATUS_RUNNING
    # if job_is_still_running:
    #     overall_status_clause = ""
    # else:
    #     overall_status_clause = ", overall_status = :status"
    # set_clause = (
    #     f"SET transform_status = :status{overall_status_clause}"
    #     ", transform_tags_success = :tags_ok"
    #     ", transform_tags_failed = :tags_fail"
    #     ", transform_records_extracted = :extracted"
    #     ", transform_records_written = :written"
    #     ", transform_records_dropped = :dropped"
    # )
    # expression_values = {
    #     ":status":    {"S": status},
    #     ":tags_ok":   {"N": str(transform_tags_success)},
    #     ":tags_fail": {"N": str(transform_tags_failed)},
    #     ":extracted": {"N": str(records_extracted)},
    #     ":written":   {"N": str(records_transformed)},
    #     ":dropped":   {"N": str(records_dropped)},
    # }
    # if duration_ms:
    #     set_clause += ", transform_duration_ms = :dur_ms"
    #     expression_values[":dur_ms"] = {"N": str(duration_ms)}
    # try:
    #     dynamodb.update_item(
    #         TableName=table_name,
    #         Key={"PK": {"S": pk}, "SK": {"S": SK_META}},
    #         UpdateExpression=set_clause,
    #         ExpressionAttributeValues=expression_values,
    #     )
    # except ClientError as exc:
    #     raise DynamoDBError(
    #         f"Failed to update run META transform status for {run_id}: {exc}",
    #         service="dynamodb",
    #         run_id=run_id,
    #     ) from exc
    # --- End DynamoDB ---

    # --- PostgreSQL ---
    # Only promote overall_status when the job is no longer RUNNING —
    # mirrors the DynamoDB logic that omitted overall_status on STATUS_RUNNING.
    job_is_still_running = status == STATUS_RUNNING
    pool = get_pg_pool()
    try:
        with pool.connection() as conn:
            if job_is_still_running:
                conn.execute(
                    """
                    UPDATE pipeline_audit.pipeline_runs
                       SET transform_status            = %s,
                           transform_tags_success      = %s,
                           transform_tags_failed       = %s,
                           transform_records_extracted = %s,
                           transform_records_written   = %s,
                           transform_records_dropped   = %s,
                           transform_duration_ms       = %s
                     WHERE run_id = %s
                    """,
                    (status, transform_tags_success, transform_tags_failed,
                     records_extracted, records_transformed, records_dropped,
                     duration_ms, run_id),
                )
            else:
                conn.execute(
                    """
                    UPDATE pipeline_audit.pipeline_runs
                       SET overall_status              = %s,
                           transform_status            = %s,
                           transform_tags_success      = %s,
                           transform_tags_failed       = %s,
                           transform_records_extracted = %s,
                           transform_records_written   = %s,
                           transform_records_dropped   = %s,
                           transform_duration_ms       = %s
                     WHERE run_id = %s
                    """,
                    (status, status, transform_tags_success, transform_tags_failed,
                     records_extracted, records_transformed, records_dropped,
                     duration_ms, run_id),
                )
    except psycopg.Error as exc:
        raise PostgresError(
            f"Failed to update run transform status for {run_id}: {exc}",
            service="postgres",
            run_id=run_id,
        ) from exc
    # --- End PostgreSQL ---

    logger.debug(
        "Run TRANSFORM updated",
        extra={
            "run_id": run_id,
            "status": status,
            "transform_tags_success": transform_tags_success,
            "transform_tags_failed": transform_tags_failed,
            "records_extracted": records_extracted,
            "records_transformed": records_transformed,
            "records_dropped": records_dropped,
            "transform_duration_ms": duration_ms,
        },
    )


def update_run_validate_status(
    table_name: str,
    run_id: str,
    status: str,
    validate_tags_success: int = 0,
    validate_tags_failed: int = 0,
    records_validated: int = 0,
    records_rejected: int = 0,
    duration_ms: int = 0,
) -> None:
    """Update the pipeline_runs row with validation stage aggregate outcomes."""

    # --- DynamoDB (commented out — preserved for rollback) ---
    # dynamodb = get_client("dynamodb")
    # pk = f"{PK_RUN_PREFIX}{run_id}"
    # job_is_still_running = status == STATUS_RUNNING
    # if job_is_still_running:
    #     overall_status_clause = ""
    # else:
    #     overall_status_clause = ", overall_status = :status"
    # set_clause = (
    #     f"SET validate_status = :status{overall_status_clause}"
    #     ", validate_tags_success = :tags_ok"
    #     ", validate_tags_failed = :tags_fail"
    #     ", validate_records_passed = :validated"
    #     ", validate_records_quarantined = :rejected"
    # )
    # expression_values = {
    #     ":status":    {"S": status},
    #     ":tags_ok":   {"N": str(validate_tags_success)},
    #     ":tags_fail": {"N": str(validate_tags_failed)},
    #     ":validated": {"N": str(records_validated)},
    #     ":rejected":  {"N": str(records_rejected)},
    # }
    # if status != STATUS_RUNNING:
    #     now_iso = datetime.now(timezone.utc).isoformat()
    #     set_clause += ", end_time = :end_time"
    #     expression_values[":end_time"] = {"S": now_iso}
    # if duration_ms:
    #     set_clause += ", validate_duration_ms = :dur_ms"
    #     expression_values[":dur_ms"] = {"N": str(duration_ms)}
    # try:
    #     dynamodb.update_item(
    #         TableName=table_name,
    #         Key={"PK": {"S": pk}, "SK": {"S": SK_META}},
    #         UpdateExpression=set_clause,
    #         ExpressionAttributeValues=expression_values,
    #     )
    # except ClientError as exc:
    #     raise DynamoDBError(
    #         f"Failed to update run META validate status for {run_id}: {exc}",
    #         service="dynamodb",
    #         run_id=run_id,
    #     ) from exc
    # --- End DynamoDB ---

    # --- PostgreSQL ---
    job_is_still_running = status == STATUS_RUNNING
    pool = get_pg_pool()
    try:
        with pool.connection() as conn:
            if job_is_still_running:
                conn.execute(
                    """
                    UPDATE pipeline_audit.pipeline_runs
                       SET validate_status              = %s,
                           validate_tags_success        = %s,
                           validate_tags_failed         = %s,
                           validate_records_passed      = %s,
                           validate_records_quarantined = %s,
                           validate_duration_ms         = %s
                     WHERE run_id = %s
                    """,
                    (status, validate_tags_success, validate_tags_failed,
                     records_validated, records_rejected, duration_ms, run_id),
                )
            else:
                conn.execute(
                    """
                    UPDATE pipeline_audit.pipeline_runs
                       SET overall_status               = %s,
                           end_time                    = NOW(),
                           validate_status              = %s,
                           validate_tags_success        = %s,
                           validate_tags_failed         = %s,
                           validate_records_passed      = %s,
                           validate_records_quarantined = %s,
                           validate_duration_ms         = %s
                     WHERE run_id = %s
                    """,
                    (status, status, validate_tags_success, validate_tags_failed,
                     records_validated, records_rejected, duration_ms, run_id),
                )
    except psycopg.Error as exc:
        raise PostgresError(
            f"Failed to update run validate status for {run_id}: {exc}",
            service="postgres",
            run_id=run_id,
        ) from exc
    # --- End PostgreSQL ---

    logger.debug(
        "Run VALIDATE updated",
        extra={
            "run_id": run_id,
            "status": status,
            "validate_tags_success": validate_tags_success,
            "validate_tags_failed": validate_tags_failed,
            "records_validated": records_validated,
            "records_rejected": records_rejected,
            "validate_duration_ms": duration_ms,
        },
    )
