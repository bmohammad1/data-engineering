"""Write raw API response JSON to the raw S3 bucket."""

import json
import logging

from botocore.exceptions import ClientError

from shared.aws_clients import get_client
from shared.exceptions import S3WriteError

logger = logging.getLogger(__name__)


def write_raw_response(bucket: str, run_id: str, tag_id: str, records: list[dict]) -> str:
    """Serialise records as JSON and write to raw/<run_id>/<tag_id>.json.

    Returns the S3 key where the data was written.
    Raises S3WriteError on any S3 failure.
    """
    s3_key = f"raw/{run_id}/{tag_id}.json"
    s3 = get_client("s3")

    try:
        s3.put_object(
            Bucket=bucket,
            Key=s3_key,
            Body=json.dumps(records).encode("utf-8"),
            ContentType="application/json",
        )
    except ClientError as exc:
        raise S3WriteError(
            f"Failed to write raw response to s3://{bucket}/{s3_key}: {exc}",
            service="s3",
            run_id=run_id,
        ) from exc

    logger.debug("Raw response written", extra={"s3_key": s3_key})
    return s3_key
