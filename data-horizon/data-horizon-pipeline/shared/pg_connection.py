"""Process-level PostgreSQL connection pool for the data-horizon pipeline.

Mirrors the pattern in shared/aws_clients.py — a lazy singleton that is
created once per process (Lambda instance or Glue executor) and reused
across invocations.

Usage:
    from shared.pg_connection import get_pg_pool

    pool = get_pg_pool()
    with pool.connection() as conn:
        conn.execute(
            "UPDATE pipeline_audit.pipeline_runs SET overall_status = %s WHERE run_id = %s",
            ("SUCCESS", run_id),
        )
        # auto-committed on clean exit; rolled back on exception
"""

import os

from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

_pool: ConnectionPool | None = None


def get_pg_pool() -> ConnectionPool:
    """Return the process-level connection pool, creating it on first call."""
    global _pool
    if _pool is None:
        conninfo = _get_connection_string()
        max_size = int(os.environ.get("PG_POOL_MAX_SIZE", "10"))
        _pool = ConnectionPool(
            conninfo=conninfo,
            min_size=1,
            max_size=max_size,
            kwargs={"row_factory": dict_row, "autocommit": False},
            open=True,
        )
    return _pool


def _get_connection_string() -> str:
    """Read the Postgres DSN from the environment or SSM Parameter Store.

    Expected SSM key: postgres-connection-string
    Expected format:  postgresql://user:password@host:5432/dbname?sslmode=require
    """
    direct = os.environ.get("PG_CONNECTION_STRING")
    if direct:
        return direct

    from shared.constants import ENVIRONMENT, load_ssm_config
    ssm = load_ssm_config(ENVIRONMENT)
    return ssm["postgres-connection-string"]
