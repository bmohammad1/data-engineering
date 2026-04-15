"""Load pipeline config from Secrets Manager and tags from S3."""

import csv
import io
import json
import logging

from botocore.exceptions import ClientError

from shared.aws_clients import get_client
from shared.exceptions import ConfigLoadError, TagFileError

logger = logging.getLogger(__name__)

TAGS_CSV_KEY = "source_config/tags.csv"


def load_pipeline_config(secret_name: str) -> dict:
    """Fetch pipeline configuration from Secrets Manager."""
    sm = get_client("secretsmanager")

    try:
        response = sm.get_secret_value(SecretId=secret_name)
        config = json.loads(response["SecretString"])
    except ClientError as exc:
        raise ConfigLoadError(
            f"Failed to load secret '{secret_name}': {exc}",
            service="secretsmanager",
        )
    except (json.JSONDecodeError, KeyError) as exc:
        raise ConfigLoadError(
            f"Malformed secret '{secret_name}': {exc}",
            service="secretsmanager",
        )

    logger.info("Pipeline config loaded", extra={"secret_name": secret_name})
    return config


def load_tags_from_s3(config: dict) -> list[str]:
    """Read tag IDs from a CSV file in the config S3 bucket."""
    bucket = config["config_bucket_name"]
    s3 = get_client("s3")

    try:
        response = s3.get_object(Bucket=bucket, Key=TAGS_CSV_KEY)
        body = response["Body"].read().decode("utf-8")
    except ClientError as exc:
        error_code = exc.response["Error"]["Code"]
        if error_code in ("NoSuchKey", "NoSuchBucket"):
            raise TagFileError(
                f"Tags file not found at s3://{bucket}/{TAGS_CSV_KEY}",
                service="s3",
            )
        raise TagFileError(f"Failed to read tags CSV: {exc}", service="s3")

    if not body.strip():
        raise TagFileError("Tags CSV file is empty", service="s3")

    reader = csv.DictReader(io.StringIO(body))
    tags = []
    for row in reader:
        tag_id = row.get("TagID", "").strip()
        if tag_id:
            tags.append(tag_id)

    if not tags:
        raise TagFileError(
            "No valid TagID values found in CSV",
            service="s3",
        )

    logger.info("Tags loaded from S3", extra={"tag_count": len(tags)})
    return tags
