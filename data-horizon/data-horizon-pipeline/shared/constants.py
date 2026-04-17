"""Shared constants for the data-horizon pipeline."""

import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent.parent / ".env", override=False)

SECRET_NAME = os.environ.get("SECRET_NAME", "")
ENVIRONMENT = os.environ.get("ENVIRONMENT", "dev")

# Primary key prefixes — PipelineAudit single-table schema
PK_RUN_PREFIX = "RUN#"
SK_META = "META"
SK_TAG_PREFIX = "TAG#"

# GSI key prefix
GSI1PK_PIPELINE_PREFIX = "PIPELINE#"

STATUS_RUNNING = "RUNNING"
STATUS_PENDING = "PENDING"
STATUS_SUCCESS = "SUCCESS"
STATUS_FAILED = "FAILED"
