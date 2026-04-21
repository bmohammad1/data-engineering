"""DynamoDB status updates for the TRANSFORM and VALIDATE pipeline stages.

Updates mirror the pattern established in
lambdas/map_state_processor/dynamodb_updater.py — using update_item
with ExpressionAttributeNames to safely target the nested stage_status map.

Two levels are updated after each Glue job:
  - Per-tag: stage_status.TRANSFORM / stage_status.VALIDATE on the TAG item
  - Per-run: transform_status / validate_status + aggregate counts on the META item
"""

import logging

from botocore.exceptions import ClientError

from shared.aws_clients import get_client
from shared.constants import PK_RUN_PREFIX, SK_META, SK_TAG_PREFIX, STATUS_RUNNING
from shared.exceptions import DynamoDBError

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Per-tag updates
# ---------------------------------------------------------------------------


def update_tag_transform_status(
    table_name: str,
    run_id: str,
    tag_id: str,
    status: str,
    duration_ms: int = 0,
) -> None:
    """Set stage_status.TRANSFORM and overall_status on a TAG item."""
    _update_tag_stage(
        table_name=table_name,
        run_id=run_id,
        tag_id=tag_id,
        stage_key="TRANSFORM",
        status=status,
        extra_attrs={":duration_ms": {"N": str(duration_ms)}},
        extra_expression=" ADD transform_duration_ms :duration_ms",
    )


def update_tag_validate_status(
    table_name: str,
    run_id: str,
    tag_id: str,
    status: str,
    valid_count: int = 0,
    invalid_count: int = 0,
    duration_ms: int = 0,
) -> None:
    """Set stage_status.VALIDATE on a TAG item with per-tag record counts."""
    _update_tag_stage(
        table_name=table_name,
        run_id=run_id,
        tag_id=tag_id,
        stage_key="VALIDATE",
        status=status,
        extra_attrs={
            ":valid":   {"N": str(valid_count)},
            ":invalid": {"N": str(invalid_count)},
            ":dur_ms":  {"N": str(duration_ms)},
        },
        extra_expression=(
            " SET records_validated = :valid, records_rejected = :invalid"
            " ADD validate_duration_ms :dur_ms"
        ),
    )


def _update_tag_stage(
    table_name: str,
    run_id: str,
    tag_id: str,
    stage_key: str,
    status: str,
    extra_attrs: dict,
    extra_expression: str,
) -> None:
    """Internal helper — set stage_status.<stage_key> and overall_status."""
    dynamodb = get_client("dynamodb")

    pk = f"{PK_RUN_PREFIX}{run_id}"
    sk = f"{SK_TAG_PREFIX}{tag_id}"

    expression_values = {
        ":status":       {"S": status},
        ":stage_status": {"S": status},
    }
    expression_values.update(extra_attrs)

    try:
        dynamodb.update_item(
            TableName=table_name,
            Key={"PK": {"S": pk}, "SK": {"S": sk}},
            UpdateExpression=(
                f"SET overall_status = :status, stage_status.#{stage_key} = :stage_status"
                + extra_expression
            ),
            ExpressionAttributeNames={f"#{stage_key}": stage_key},
            ExpressionAttributeValues=expression_values,
        )
    except ClientError as exc:
        raise DynamoDBError(
            f"Failed to update {stage_key} status for tag {tag_id} in run {run_id}: {exc}",
            service="dynamodb",
            run_id=run_id,
        ) from exc

    logger.debug(
        "Tag stage status updated",
        extra={"run_id": run_id, "tag_id": tag_id, "stage": stage_key, "status": status},
    )


# ---------------------------------------------------------------------------
# Per-run (META item) updates
# ---------------------------------------------------------------------------


def update_run_transform_status(
    table_name: str,
    run_id: str,
    status: str,
    records_transformed: int = 0,
    records_failed: int = 0,
) -> None:
    """Update the RUN META item with transform stage outcome."""
    _update_run_meta(
        table_name=table_name,
        run_id=run_id,
        stage_field="transform_status",
        status=status,
        extra_attrs={
            ":transformed": {"N": str(records_transformed)},
            ":failed":      {"N": str(records_failed)},
        },
        extra_expression=" SET records_transformed = :transformed, transform_failed = :failed",
    )


def update_run_validate_status(
    table_name: str,
    run_id: str,
    status: str,
    records_validated: int = 0,
    records_rejected: int = 0,
) -> None:
    """Update the RUN META item with validation stage outcome."""
    _update_run_meta(
        table_name=table_name,
        run_id=run_id,
        stage_field="validate_status",
        status=status,
        extra_attrs={
            ":validated": {"N": str(records_validated)},
            ":rejected":  {"N": str(records_rejected)},
        },
        extra_expression=" SET records_validated = :validated, records_rejected = :rejected",
    )


def _update_run_meta(
    table_name: str,
    run_id: str,
    stage_field: str,
    status: str,
    extra_attrs: dict,
    extra_expression: str,
) -> None:
    """Internal helper — update the META item for a run."""
    dynamodb = get_client("dynamodb")

    pk = f"{PK_RUN_PREFIX}{run_id}"

    expression_values = {":status": {"S": status}}
    expression_values.update(extra_attrs)

    # Glue job keeps overall_status as RUNNING until Child4 (Redshift Load) completes.
    # Only set it to FAILED here if the stage itself failed — success is set by the orchestrator.
    overall_update = ""
    if status != STATUS_RUNNING:
        overall_update = ", overall_status = :status"

    try:
        dynamodb.update_item(
            TableName=table_name,
            Key={"PK": {"S": pk}, "SK": {"S": SK_META}},
            UpdateExpression=(
                f"SET {stage_field} = :status{overall_update}"
                + extra_expression
            ),
            ExpressionAttributeValues=expression_values,
        )
    except ClientError as exc:
        raise DynamoDBError(
            f"Failed to update run meta for {run_id}: {exc}",
            service="dynamodb",
            run_id=run_id,
        ) from exc

    logger.debug(
        "Run meta updated",
        extra={"run_id": run_id, "stage": stage_field, "status": status},
    )
