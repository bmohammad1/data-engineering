"""DynamoDB status updates for the TRANSFORM and VALIDATE pipeline stages.

Updates mirror the pattern established in
lambdas/map_state_processor/dynamodb_updater.py — using update_item
with ExpressionAttributeNames to safely target the nested stage_status map.

Two levels are updated after each Glue job:
  - Per-tag: stage_status.TRANSFORM / stage_status.VALIDATE on the TAG item
  - Per-run: transform_status / validate_status + aggregate counts on the META item

Failure reasons are captured in structured logs (logger.error), not in DynamoDB.
Per-tag items carry record counts so operators can track data volume per tag.
"""

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

from botocore.exceptions import ClientError

from shared.aws_clients import get_client
from shared.constants import PK_RUN_PREFIX, SK_META, SK_TAG_PREFIX, STATUS_RUNNING
from shared.exceptions import DynamoDBError

logger = logging.getLogger(__name__)

# Number of concurrent threads used when writing per-tag DynamoDB updates.
# At 50 threads with ~10ms per update_item round-trip, 5,000 tags complete
# in ~1 second instead of ~50 seconds sequentially.
_DYNAMODB_WRITE_CONCURRENCY = 50


# ---------------------------------------------------------------------------
# Bulk concurrent update helpers
# ---------------------------------------------------------------------------


def bulk_update_tag_transform_status(
    table_name: str,
    run_id: str,
    tag_updates: list[dict],
) -> None:
    """Update TRANSFORM stage status on existing TAG items for all tags concurrently.

    Each entry in tag_updates must contain:
        tag_id, status, records_extracted, records_dropped, records_transformed

    Submits one update_item call per tag to a thread pool so all updates run
    in parallel. A failure for any individual tag logs a warning and does not
    abort the remaining updates — fault isolation is preserved.
    """
    if not tag_updates:
        return

    with ThreadPoolExecutor(max_workers=_DYNAMODB_WRITE_CONCURRENCY) as executor:
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
                    "Failed to update DynamoDB TRANSFORM status for tag — continuing",
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
    """Update VALIDATE stage status on existing TAG items for all tags concurrently.

    Each entry in tag_updates must contain:
        tag_id, status, valid_count, invalid_count
    """
    if not tag_updates:
        return

    with ThreadPoolExecutor(max_workers=_DYNAMODB_WRITE_CONCURRENCY) as executor:
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
                    "Failed to update DynamoDB VALIDATE status for tag — continuing",
                    extra={"run_id": run_id, "tag_id": tag_id, "error": str(exc)},
                )


# ---------------------------------------------------------------------------
# Per-run tag fetch
# ---------------------------------------------------------------------------


def fetch_transform_succeeded_tags(table_name: str, run_id: str) -> set[str]:
    """Return tag IDs that completed the transform stage successfully.

    Queries all TAG items for the given run and returns only those whose
    overall_status is SUCCESS. Used by the validation job to seed its tag
    universe from DynamoDB rather than deriving it from S3 file contents.
    """
    dynamodb = get_client("dynamodb")
    partition_key_value = f"{PK_RUN_PREFIX}{run_id}"
    tag_ids: set[str] = set()

    query_params: dict = {
        "TableName": table_name,
        "KeyConditionExpression": "PK = :pk AND begins_with(SK, :sk_prefix)",
        "FilterExpression": "overall_status = :success",
        "ExpressionAttributeValues": {
            ":pk":        {"S": partition_key_value},
            ":sk_prefix": {"S": SK_TAG_PREFIX},
            ":success":   {"S": "SUCCESS"},
        },
    }

    try:
        while True:
            response = dynamodb.query(**query_params)

            for item in response.get("Items", []):
                sort_key_value = item["SK"]["S"]
                tag_id = sort_key_value.removeprefix(SK_TAG_PREFIX)
                tag_ids.add(tag_id)

            last_evaluated_key = response.get("LastEvaluatedKey")
            pagination_is_complete = last_evaluated_key is None
            if pagination_is_complete:
                break

            query_params["ExclusiveStartKey"] = last_evaluated_key

    except ClientError as exc:
        raise DynamoDBError(
            f"Failed to fetch transform-succeeded tags for run {run_id}: {exc}",
            service="dynamodb",
            run_id=run_id,
        ) from exc

    logger.debug(
        "Fetched transform-succeeded tags from DynamoDB",
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
    """Set stage_status.TRANSFORM on a TAG item with full record counts.

    Args:
        records_extracted:   Total records read from raw JSON across all tables.
        records_dropped:     Records silently dropped (null PK / null TagID after cast).
        records_transformed: Records successfully written to cleaned bucket.
    """
    dynamodb = get_client("dynamodb")
    pk = f"{PK_RUN_PREFIX}{run_id}"
    sk = f"{SK_TAG_PREFIX}{tag_id}"

    set_clause = (
        "SET overall_status = :status"
        ", stage_status.#TRANSFORM = :stage_status"
        ", transform_records_extracted = :extracted"
        ", transform_records_dropped = :dropped"
        ", transform_records_written = :written"
    )
    expression_values = {
        ":status":        {"S": status},
        ":stage_status":  {"S": status},
        ":extracted":     {"N": str(records_extracted)},
        ":dropped":       {"N": str(records_dropped)},
        ":written":       {"N": str(records_transformed)},
    }

    if duration_ms:
        expression_values[":dur_ms"] = {"N": str(duration_ms)}
        update_expr = set_clause + " ADD transform_duration_ms :dur_ms"
    else:
        update_expr = set_clause

    try:
        dynamodb.update_item(
            TableName=table_name,
            Key={"PK": {"S": pk}, "SK": {"S": sk}},
            UpdateExpression=update_expr,
            ExpressionAttributeNames={"#TRANSFORM": "TRANSFORM"},
            ExpressionAttributeValues=expression_values,
        )
    except ClientError as exc:
        raise DynamoDBError(
            f"Failed to update TRANSFORM status for tag {tag_id} in run {run_id}: {exc}",
            service="dynamodb",
            run_id=run_id,
        ) from exc

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
    """Set stage_status.VALIDATE on a TAG item with full record counts.

    Args:
        valid_count:   Records that passed all validation rules → written to validated bucket.
        invalid_count: Records that failed at least one rule → quarantined.
    """
    dynamodb = get_client("dynamodb")
    pk = f"{PK_RUN_PREFIX}{run_id}"
    sk = f"{SK_TAG_PREFIX}{tag_id}"

    set_clause = (
        "SET overall_status = :status"
        ", stage_status.#VALIDATE = :stage_status"
        ", validate_records_passed = :valid"
        ", validate_records_quarantined = :invalid"
    )
    expression_values = {
        ":status":       {"S": status},
        ":stage_status": {"S": status},
        ":valid":        {"N": str(valid_count)},
        ":invalid":      {"N": str(invalid_count)},
    }

    if duration_ms:
        expression_values[":dur_ms"] = {"N": str(duration_ms)}
        update_expr = set_clause + " ADD validate_duration_ms :dur_ms"
    else:
        update_expr = set_clause

    try:
        dynamodb.update_item(
            TableName=table_name,
            Key={"PK": {"S": pk}, "SK": {"S": sk}},
            UpdateExpression=update_expr,
            ExpressionAttributeNames={"#VALIDATE": "VALIDATE"},
            ExpressionAttributeValues=expression_values,
        )
    except ClientError as exc:
        raise DynamoDBError(
            f"Failed to update VALIDATE status for tag {tag_id} in run {run_id}: {exc}",
            service="dynamodb",
            run_id=run_id,
        ) from exc

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
# Per-run (META item) updates
# ---------------------------------------------------------------------------


def update_run_transform_status(
    table_name: str,
    run_id: str,
    status: str,
    transform_tags_success: int = 0,
    transform_tags_failed: int = 0,
    records_transformed: int = 0,
    records_dropped: int = 0,
    duration_ms: int = 0,
) -> None:
    """Update the RUN META item with transform stage aggregate outcomes."""
    dynamodb = get_client("dynamodb")
    pk = f"{PK_RUN_PREFIX}{run_id}"

    job_is_still_running = status == STATUS_RUNNING
    if job_is_still_running:
        overall_status_clause = ""
    else:
        overall_status_clause = ", overall_status = :status"

    set_clause = (
        f"SET transform_status = :status{overall_status_clause}"
        ", transform_tags_success = :tags_ok"
        ", transform_tags_failed = :tags_fail"
        ", transform_records_written = :written"
        ", transform_records_dropped = :dropped"
    )
    expression_values = {
        ":status":    {"S": status},
        ":tags_ok":   {"N": str(transform_tags_success)},
        ":tags_fail": {"N": str(transform_tags_failed)},
        ":written":   {"N": str(records_transformed)},
        ":dropped":   {"N": str(records_dropped)},
    }

    if duration_ms:
        set_clause += ", transform_duration_ms = :dur_ms"
        expression_values[":dur_ms"] = {"N": str(duration_ms)}

    try:
        dynamodb.update_item(
            TableName=table_name,
            Key={"PK": {"S": pk}, "SK": {"S": SK_META}},
            UpdateExpression=set_clause,
            ExpressionAttributeValues=expression_values,
        )
    except ClientError as exc:
        raise DynamoDBError(
            f"Failed to update run META transform status for {run_id}: {exc}",
            service="dynamodb",
            run_id=run_id,
        ) from exc

    logger.debug(
        "Run META TRANSFORM updated",
        extra={
            "run_id": run_id,
            "status": status,
            "transform_tags_success": transform_tags_success,
            "transform_tags_failed": transform_tags_failed,
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
    """Update the RUN META item with validation stage aggregate outcomes."""
    dynamodb = get_client("dynamodb")
    pk = f"{PK_RUN_PREFIX}{run_id}"

    job_is_still_running = status == STATUS_RUNNING
    if job_is_still_running:
        overall_status_clause = ""
    else:
        overall_status_clause = ", overall_status = :status"

    set_clause = (
        f"SET validate_status = :status{overall_status_clause}"
        ", validate_tags_success = :tags_ok"
        ", validate_tags_failed = :tags_fail"
        ", validate_records_passed = :validated"
        ", validate_records_quarantined = :rejected"
    )
    expression_values = {
        ":status":    {"S": status},
        ":tags_ok":   {"N": str(validate_tags_success)},
        ":tags_fail": {"N": str(validate_tags_failed)},
        ":validated": {"N": str(records_validated)},
        ":rejected":  {"N": str(records_rejected)},
    }

    if status != STATUS_RUNNING:
        now_iso = datetime.now(timezone.utc).isoformat()
        set_clause += ", end_time = :end_time"
        expression_values[":end_time"] = {"S": now_iso}

    if duration_ms:
        set_clause += ", validate_duration_ms = :dur_ms"
        expression_values[":dur_ms"] = {"N": str(duration_ms)}

    try:
        dynamodb.update_item(
            TableName=table_name,
            Key={"PK": {"S": pk}, "SK": {"S": SK_META}},
            UpdateExpression=set_clause,
            ExpressionAttributeValues=expression_values,
        )
    except ClientError as exc:
        raise DynamoDBError(
            f"Failed to update run META validate status for {run_id}: {exc}",
            service="dynamodb",
            run_id=run_id,
        ) from exc

    logger.debug(
        "Run META VALIDATE updated",
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
