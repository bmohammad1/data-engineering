"""Shared constants and SSM-backed runtime config for the data-horizon pipeline."""

import os
from pathlib import Path
from typing import Any

# Load .env only in local dev — python-dotenv is not available in Glue or Lambda.
_env_file = Path(__file__).resolve().parent.parent / ".env"
if _env_file.exists():
    try:
        from dotenv import load_dotenv
        load_dotenv(_env_file, override=False)
    except ModuleNotFoundError:
        pass

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


def load_ssm_config(environment: str | None = None) -> dict[str, Any]:
    """Fetch all pipeline parameters from SSM Parameter Store for the given environment."""
    env = environment or ENVIRONMENT
    path = f"/data-horizon/{env}/"

    from shared.aws_clients import get_client
    ssm = get_client("ssm")

    params: list[dict] = []
    paginator = ssm.get_paginator("get_parameters_by_path")
    for page in paginator.paginate(Path=path, WithDecryption=True):
        params.extend(page["Parameters"])

    return {p["Name"].removeprefix(path): p["Value"] for p in params}
