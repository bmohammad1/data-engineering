from __future__ import annotations

import os
import io
import json
from pathlib import Path
from typing import Any, Dict, List

from airflow import DAG
from airflow.models.param import Param
# from airflow.decorators import task, get_current_context

from airflow.decorators import task
from airflow.operators.python import get_current_context

from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta

# ---------------------------
# Core configuration
# ---------------------------
AWS_CONN_ID = "aws_default"
REDSHIFT_CONN_ID = "redshift_default"

# Redshift COPY role
IAM_ROLE_ARN = "arn:aws:iam::742460038752:role/service-role/AmazonRedshift-CommandsAccessRole-20260331T184407"
# If S3 bucket region differs from Redshift region
ADD_REGION_IN_COPY = False
S3_REGION = "us-east-1"

# Where to load the registry from. If not set, uses local file under ../config/schema_registry.yml
# Examples:
#   export SCHEMA_REGISTRY_URI="/opt/airflow/config/schema_registry.yml"
#   export SCHEMA_REGISTRY_URI="s3://my-config-bucket/schema/registry.yml"
SCHEMA_REGISTRY_URI = os.environ.get(
    "SCHEMA_REGISTRY_URI",
    # default: ../config/schema_registry.yml relative to this file
    str((Path(__file__).resolve().parent / "../config/schema_registry.yml").resolve())
)

# ---------------------------
# DAG defaults
# ---------------------------
default_args = {
    "owner": "data-eng",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=3),
}

# ---------------------------
# Helpers
# ---------------------------
def _resolve_load_date(context, provided: str | None) -> str:
    if provided:
        return provided
    dag_run = context.get("dag_run")
    if dag_run and getattr(dag_run, "conf", None):
        val = dag_run.conf.get("load_date")
        if val:
            return val
    return context["ds"]  # YYYY-MM-DD

def _read_registry(uri: str) -> Dict[str, Any]:
    """
    Read registry from local path or s3://bucket/key.
    Returns parsed YAML dict.
    """
    import yaml

    if uri.startswith("s3://"):
        # Parse s3 URI
        # s3://bucket/key...
        without = uri[len("s3://"):]
        bucket, key = without.split("/", 1)
        s3 = S3Hook(aws_conn_id=AWS_CONN_ID)
        text = s3.read_key(key=key, bucket_name=bucket)
        return yaml.safe_load(text)
    else:
        path = Path(uri)
        with path.open("r", encoding="utf-8") as f:
            return yaml.safe_load(f)

def _normalize_dataset(name: str, cfg: Dict[str, Any], defaults: Dict[str, Any]) -> Dict[str, Any]:
    """
    Apply defaults and derive computed fields:
      - schema, table, s3_bucket, root_prefix
      - s3_prefix_base (default root_prefix/name)
      - columns (optional)
    """
    schema = cfg.get("schema", defaults.get("schema"))
    table = cfg.get("table", name)
    s3_bucket = cfg.get("s3_bucket", defaults.get("s3_bucket"))
    root_prefix = cfg.get("root_prefix", defaults.get("root_prefix"))
    s3_prefix_base = cfg.get("s3_prefix_base", f"{root_prefix.strip('/')}/{name}")

    # IO / format
    data_format = (cfg.get("format", defaults.get("format", "csv")) or "csv").lower()
    header = bool(cfg.get("header", defaults.get("header", True)))
    delimiter = cfg.get("delimiter", defaults.get("delimiter", ","))
    compression = (cfg.get("compression", defaults.get("compression", "none")) or "none").lower()
    jsonpaths = cfg.get("jsonpaths") or cfg.get("jsonpath")  # support either spelling
    required = bool(cfg.get("required", defaults.get("required", True)))

    copy_options = list(defaults.get("copy_options", [])) + list(cfg.get("copy_options", []))

    columns = cfg.get("columns", [])  # list of {name, type}

    return {
        "name": name,
        "schema": schema,
        "table": table,
        "s3_bucket": s3_bucket,
        "s3_prefix_base": s3_prefix_base,
        "format": data_format,
        "header": header,
        "delimiter": delimiter,
        "compression": compression,
        "jsonpaths": jsonpaths,
        "required": required,
        "copy_options": copy_options,
        "columns": columns,
    }

def _build_create_table_sql(schema: str, table: str, columns: List[Dict[str, str]]) -> str | None:
    """
    Build CREATE TABLE IF NOT EXISTS using the provided columns.
    If columns list is empty, return None (caller can skip DDL).
    """
    if not columns:
        return None
    cols_sql = ",\n        ".join([f'{c["name"]} {c["type"]}' for c in columns])
    return f'''
    CREATE SCHEMA IF NOT EXISTS "{schema}";
    CREATE TABLE IF NOT EXISTS "{schema}"."{table}" (
        {cols_sql}
    )
    DISTSTYLE AUTO
    SORTKEY AUTO;
    '''

def _build_copy_sql(ds_cfg: Dict[str, Any], s3_uri: str, include_columns: bool = True) -> str:
    """
    Build Redshift COPY statement for dataset cfg and source s3_uri (prefix).
    """
    schema = ds_cfg["schema"]
    table = ds_cfg["table"]
    fmt = ds_cfg["format"]
    header = ds_cfg["header"]
    delimiter = ds_cfg["delimiter"]
    compression = ds_cfg["compression"]
    jsonpaths = ds_cfg["jsonpaths"]
    copy_opts = list(ds_cfg["copy_options"] or [])

    # Columns list (optional)
    columns = ds_cfg.get("columns", [])
    columns_clause = ""
    if include_columns and columns:
        col_list = ", ".join([c["name"] for c in columns])
        columns_clause = f" ({col_list})"

    # Format clause
    if fmt == "csv":
        fmt_clauses = ["CSV"]
        if header:
            fmt_clauses.append("IGNOREHEADER 1")
        if delimiter and delimiter != ",":
            fmt_clauses.append(f"DELIMITER '{delimiter}'")
    elif fmt == "json":
        if jsonpaths:
            fmt_clauses = [f"JSON '{jsonpaths}'"]
        else:
            fmt_clauses = ["JSON 'auto'"]
    elif fmt == "parquet":
        fmt_clauses = ["FORMAT AS PARQUET"]
        # For Parquet, you usually omit explicit columns in COPY.
        columns_clause = "" if include_columns else ""
    else:
        raise ValueError(f"Unsupported format: {fmt}")

    # Compression
    if compression == "gzip":
        fmt_clauses.append("GZIP")

    region_clause = f"\nREGION '{S3_REGION}'" if ADD_REGION_IN_COPY else ""

    all_clauses = fmt_clauses + copy_opts
    clauses_str = "\n    ".join(all_clauses)

    copy_sql = (
        f'COPY "{schema}"."{table}"{columns_clause}\n'
        f"FROM '{s3_uri}'\n"
        f"IAM_ROLE '{IAM_ROLE_ARN}'\n"
        f"{clauses_str}{region_clause};"
    )
    return copy_sql
    # return f'''
    # COPY "{schema}"."{table}"{columns_clause}
    # FROM '{s3_uri}'
    # IAM_ROLE '{IAM_ROLE_ARN}'
    # {'\n    '.join(all_clauses)}{region_clause};
    # '''

# ---------------------------
# Tasks
# ---------------------------
@task
def load_registry(registry_uri: str) -> Dict[str, Any]:
    reg = _read_registry(registry_uri)
    if not reg or "datasets" not in reg:
        raise ValueError("schema registry missing 'datasets' section")
    return reg

@task
def plan_datasets(registry: Dict[str, Any], load_date: str | None = None) -> List[Dict[str, Any]]:
    """
    Merge defaults, build dataset configs, check S3 for files for the date, and
    return a list of dataset dicts to ingest. Skips non-required datasets with no files.
    """
    from airflow.utils.context import Context
    context: Context = get_current_context()  # type: ignore
    effective_date = _resolve_load_date(context, load_date)

    defaults = registry.get("defaults", {}) or {}
    datasets = registry.get("datasets", {}) or {}

    s3 = S3Hook(aws_conn_id=AWS_CONN_ID)
    planned: List[Dict[str, Any]] = []
    for name, cfg in datasets.items():
        ds_cfg = _normalize_dataset(name, cfg or {}, defaults)
        bucket = ds_cfg["s3_bucket"]
        prefix_base = ds_cfg["s3_prefix_base"].rstrip("/")
        date_prefix = f"{prefix_base}/{effective_date}/"
        keys = s3.list_keys(bucket_name=bucket, prefix=date_prefix) or []

        if not keys:
            if ds_cfg["required"]:
                # Hard fail for required datasets with no files
                raise FileNotFoundError(
                    f"No files found for required dataset '{name}' under s3://{bucket}/{date_prefix}"
                )
            else:
                # Skip optional datasets
                continue

        ds_cfg["s3_date_prefix"] = date_prefix
        planned.append(ds_cfg)

    if not planned:
        raise FileNotFoundError(f"No datasets with files found for {effective_date}")

    return planned

@task
def ingest_dataset(ds_cfg: Dict[str, Any], load_date: str | None = None) -> None:
    """
    For a single dataset:
      - CREATE TABLE IF NOT EXISTS (if columns provided)
      - COPY all files from s3://bucket/<prefix_base>/<load_date>/ into schema.table
    """
    from airflow.utils.context import Context
    context: Context = get_current_context()  # type: ignore
    effective_date = _resolve_load_date(context, load_date)
    schema = ds_cfg["schema"]
    table = ds_cfg["table"]
    columns = ds_cfg.get("columns", [])
    bucket = ds_cfg["s3_bucket"]
    date_prefix = ds_cfg["s3_date_prefix"]  # set by plan_datasets
    s3_uri = f"s3://{bucket}/{date_prefix}"

    # DDL (if schema provided)
    # ddl = _build_create_table_sql(ds_cfg["schema"], ds_cfg["table"], ds_cfg.get("columns", []))
    pg = PostgresHook(postgres_conn_id=REDSHIFT_CONN_ID)
    pg.run(f'CREATE SCHEMA IF NOT EXISTS "{schema}";')

    # 2. Create table (ALWAYS separate execute)
    if columns:
        cols_sql = ",\n        ".join(
            [f'{c["name"]} {c["type"]}' for c in columns]
        )

        create_table_sql = f'''
        CREATE TABLE IF NOT EXISTS "{schema}"."{table}" (
            {cols_sql}
        )
        DISTSTYLE AUTO
        SORTKEY AUTO;
        '''
        pg.run(create_table_sql)
        print(f"[{ds_cfg['name']}] Ensured table {schema}.{table}")
    # if ddl:
    #     pg.run(ddl)
    #     print(f"[{ds_cfg['name']}] Ensured table {ds_cfg['schema']}.{ds_cfg['table']}")

    # COPY
    copy_sql = _build_copy_sql(ds_cfg, s3_uri, include_columns=True)
    print(f"[{ds_cfg['name']}] Executing COPY:\n{copy_sql}")
    pg.run(copy_sql)
    print(f"[{ds_cfg['name']}] COPY completed from {s3_uri}")

with DAG(
    dag_id="s3_to_redshift_registry",
    description="Registry-driven ingestion: auto-create tables if missing and COPY from S3 by date for all datasets.",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,  # change to '@daily' later
    catchup=False,
    params={
        "load_date": Param(default=None, type=["null", "string"]),  # optional YYYY-MM-DD
        # Optional override at trigger time:
        # "schema_registry_uri": Param(default=None, type=["null", "string"]),
    },
    tags=["s3", "redshift", "registry", "auto"],
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    registry_uri = SCHEMA_REGISTRY_URI  # or read from params if you prefer:
    # registry_uri = "{{ params.schema_registry_uri or var.value.get('SCHEMA_REGISTRY_URI', '') or '" + SCHEMA_REGISTRY_URI + "' }}"

    registry = load_registry(registry_uri=registry_uri)
    planned = plan_datasets(registry=registry)

    # Dynamic task mapping = one task per dataset (parallelizable)
    ingestions = ingest_dataset.expand(ds_cfg=planned)

    start >> registry >> planned >> ingestions >> end