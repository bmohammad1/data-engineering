"""Centralized logging configuration for the Mock Source API.

Provides structured JSON logging for production (Lambda) and human-readable
output for local development.  A ``contextvars``-based request ID is threaded
through every log entry automatically via a custom filter.
"""

import json
import logging
import logging.config
import os
import uuid
from contextvars import ContextVar
from datetime import datetime, timezone

# ---------------------------------------------------------------------------
# Request-scoped context variable — set by the request middleware, read by
# the log filter so every log line within a request shares the same ID.
# ---------------------------------------------------------------------------
request_id_ctx: ContextVar[str] = ContextVar("request_id", default="")

# Accumulates structured fields throughout the request lifecycle so the
# middleware can emit a single consolidated log at the end.
request_log_ctx: ContextVar[dict] = ContextVar("request_log", default={})


class RequestIdFilter(logging.Filter):
    """Inject the current ``request_id`` into every log record."""

    def filter(self, record: logging.LogRecord) -> bool:
        record.request_id = request_id_ctx.get("")  # type: ignore[attr-defined]
        return True


class JsonFormatter(logging.Formatter):
    """Emit each log record as a single JSON line with structured fields."""

    def format(self, record: logging.LogRecord) -> str:
        """Build a JSON object from the log record and any extra fields."""
        entry: dict = {
            "timestamp": datetime.fromtimestamp(
                record.created, tz=timezone.utc
            ).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "request_id": getattr(record, "request_id", ""),
        }

        # Merge any extra fields the caller passed via the ``extra`` kwarg.
        # Skip internal LogRecord attributes to avoid noise.
        _skip = {
            *logging.LogRecord("", 0, "", 0, "", (), None).__dict__,
            "message",
            "request_id",
        }
        for key, value in record.__dict__.items():
            if key not in _skip and key not in entry:
                entry[key] = value

        if record.exc_info and not record.exc_text:
            record.exc_text = self.formatException(record.exc_info)
        if record.exc_text:
            entry["exception"] = record.exc_text

        return json.dumps(entry, default=str)


def new_request_id() -> str:
    """Generate a fresh request ID (32-char hex UUID)."""
    return uuid.uuid4().hex


# ---------------------------------------------------------------------------
# Whether we're running inside AWS Lambda — controls the default formatter.
# ---------------------------------------------------------------------------
_is_lambda = bool(os.environ.get("AWS_LAMBDA_FUNCTION_NAME"))


def configure_logging(
    log_level: str | None = None,
    json_output: bool | None = None,
) -> None:
    """Configure the root logger with structured output and request-ID injection.

    Parameters
    ----------
    log_level:
        Python log level name (e.g. ``"INFO"``).  Defaults to ``"INFO"``.
    json_output:
        When ``True`` use JSON formatter (production).  When ``False`` use
        human-readable lines (local dev).  Defaults to ``True`` inside Lambda.
    """
    level = (log_level or "INFO").upper()
    use_json = json_output if json_output is not None else _is_lambda

    if use_json:
        formatter_config: dict = {
            "()": f"{__name__}.JsonFormatter",
        }
    else:
        formatter_config = {
            "format": (
                "%(asctime)s | %(levelname)-8s | %(name)s | "
                "req=%(request_id)s | %(message)s"
            ),
            "datefmt": "%Y-%m-%dT%H:%M:%S",
        }

    config = {
        "version": 1,
        "disable_existing_loggers": False,
        "filters": {
            "request_id": {
                "()": f"{__name__}.RequestIdFilter",
            },
        },
        "formatters": {
            "default": formatter_config,
        },
        "handlers": {
            "console": {
                "class": "logging.StreamHandler",
                "formatter": "default",
                "filters": ["request_id"],
                "stream": "ext://sys.stdout",
            },
        },
        "root": {
            "level": level,
            "handlers": ["console"],
        },
    }

    logging.config.dictConfig(config)
