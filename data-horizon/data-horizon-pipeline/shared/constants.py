"""Shared constants for the data-horizon pipeline."""

import os
from pathlib import Path

# Load .env only in local dev — python-dotenv is not available in Glue or Lambda.
# In deployed environments, env vars are injected by Terraform / Glue job arguments.
_env_file = Path(__file__).resolve().parent.parent / ".env"
if _env_file.exists():
    try:
        from dotenv import load_dotenv
        load_dotenv(_env_file, override=False)
    except ModuleNotFoundError:
        pass

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
