"""Generate the distributed Map State input JSON and upload to S3."""

import json
import logging

from botocore.exceptions import ClientError

from shared.aws_clients import get_client
from shared.exceptions import S3WriteError

logger = logging.getLogger(__name__)


def generate_map_state_input(
    bucket: str,
    run_id: str,
    tags: list[str],
    config: dict,
) -> str:
    """Build the Map State input JSON array and write it to S3.

    Returns the S3 key where the JSON was written.
    """
    endpoint_base = config.get("source_api_base_url", "")

    map_items = [
        {
            "tag_id": tag_id,
            "run_id": run_id,
            "endpoint": f"{endpoint_base}/tag/{tag_id}",
        }
        for tag_id in tags
    ]

    s3_key = f"maps/{run_id}/map_input.json"
    s3 = get_client("s3")

    try:
        s3.put_object(
            Bucket=bucket,
            Key=s3_key,
            Body=json.dumps(map_items).encode("utf-8"),
            ContentType="application/json",
        )
    except ClientError as exc:
        raise S3WriteError(
            f"Failed to write map input to s3://{bucket}/{s3_key}: {exc}",
            service="s3",
            run_id=run_id,
        )

    logger.debug(
        "Map state input written",
        extra={"run_id": run_id, "s3_key": s3_key, "tag_count": len(tags)},
    )

    return s3_key
