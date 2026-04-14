"""Shared constants for the data-horizon pipeline."""

import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent.parent / ".env", override=False)

SECRET_NAME = os.environ.get("SECRET_NAME", "")
ENVIRONMENT = os.environ.get("ENVIRONMENT", "dev")

PK_PIPELINE_RUN = "PIPELINE_RUN#"
SK_METADATA = "METADATA"
SK_TAG_STATUS_PREFIX = "TAG_STATUS#"
SK_LOAD_METRICS = "LOAD_METRICS"

STATUS_RUNNING = "RUNNING"
STATUS_PENDING = "PENDING"
STATUS_SUCCESS = "SUCCESS"
STATUS_FAILED = "FAILED"
