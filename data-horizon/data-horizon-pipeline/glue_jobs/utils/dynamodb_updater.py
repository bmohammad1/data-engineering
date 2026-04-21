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

from botocore.exceptions import ClientError

from shared.aws_clients import get_client
from shared.constants import PK_RUN_PREFIX, SK_META, SK_TAG_PREFIX, STATUS_RUNNING
from shared.exceptions import DynamoDBError

logger = logging.getLogger(__name__)


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
) -> None:
    """Update the RUN META item with transform stage aggregate outcomes."""
    dynamodb = get_client("dynamodb")
    pk = f"{PK_RUN_PREFIX}{run_id}"

    overall_update = "" if status == STATUS_RUNNING else ", overall_status = :status"

    try:
        dynamodb.update_item(
            TableName=table_name,
            Key={"PK": {"S": pk}, "SK": {"S": SK_META}},
            UpdateExpression=(
                f"SET transform_status = :status{overall_update}"
                ", transform_tags_success = :tags_ok"
                ", transform_tags_failed = :tags_fail"
                ", transform_records_written = :written"
                ", transform_records_dropped = :dropped"
            ),
            ExpressionAttributeValues={
                ":status":    {"S": status},
                ":tags_ok":   {"N": str(transform_tags_success)},
                ":tags_fail": {"N": str(transform_tags_failed)},
                ":written":   {"N": str(records_transformed)},
                ":dropped":   {"N": str(records_dropped)},
            },
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
) -> None:
    """Update the RUN META item with validation stage aggregate outcomes."""
    dynamodb = get_client("dynamodb")
    pk = f"{PK_RUN_PREFIX}{run_id}"

    overall_update = "" if status == STATUS_RUNNING else ", overall_status = :status"

    try:
        dynamodb.update_item(
            TableName=table_name,
            Key={"PK": {"S": pk}, "SK": {"S": SK_META}},
            UpdateExpression=(
                f"SET validate_status = :status{overall_update}"
                ", validate_tags_success = :tags_ok"
                ", validate_tags_failed = :tags_fail"
                ", validate_records_passed = :validated"
                ", validate_records_quarantined = :rejected"
            ),
            ExpressionAttributeValues={
                ":status":    {"S": status},
                ":tags_ok":   {"N": str(validate_tags_success)},
                ":tags_fail": {"N": str(validate_tags_failed)},
                ":validated": {"N": str(records_validated)},
                ":rejected":  {"N": str(records_rejected)},
            },
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
        },
    )
