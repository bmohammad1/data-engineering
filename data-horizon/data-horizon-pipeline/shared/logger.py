"""Centralized logging configuration for the data-horizon pipeline."""

import json
import logging
import logging.config
import os
from contextvars import ContextVar
from datetime import datetime, timezone

run_id_ctx: ContextVar[str] = ContextVar("run_id", default="")


class PipelineContextFilter(logging.Filter):
    """Inject the current run_id into every log record."""

    def filter(self, record: logging.LogRecord) -> bool:
        record.run_id = run_id_ctx.get("")  # type: ignore[attr-defined]
        return True


class JsonFormatter(logging.Formatter):
    """Emit each log record as a single JSON line."""

    def format(self, record: logging.LogRecord) -> str:
        entry: dict = {
            "timestamp": datetime.fromtimestamp(
                record.created, tz=timezone.utc
            ).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "run_id": getattr(record, "run_id", ""),
        }

        _skip = {
            *logging.LogRecord("", 0, "", 0, "", (), None).__dict__,
            "message",
            "run_id",
        }
        for key, value in record.__dict__.items():
            if key not in _skip and key not in entry:
                entry[key] = value

        if record.exc_info and not record.exc_text:
            record.exc_text = self.formatException(record.exc_info)
        if record.exc_text:
            entry["exception"] = record.exc_text

        return json.dumps(entry, default=str)


_is_lambda = bool(os.environ.get("AWS_LAMBDA_FUNCTION_NAME"))


def configure_logging(log_level: str | None = None) -> None:
    """Configure the root logger with structured output and run_id injection."""
    level = (log_level or os.environ.get("LOG_LEVEL", "INFO")).upper()
    use_json = _is_lambda

    if use_json:
        formatter_config: dict = {"()": f"{__name__}.JsonFormatter"}
    else:
        formatter_config = {
            "format": (
                "%(asctime)s | %(levelname)-8s | %(name)s | "
                "run=%(run_id)s | %(message)s"
            ),
            "datefmt": "%Y-%m-%dT%H:%M:%S",
        }

    config = {
        "version": 1,
        "disable_existing_loggers": False,
        "filters": {
            "pipeline_context": {"()": f"{__name__}.PipelineContextFilter"},
        },
        "formatters": {"default": formatter_config},
        "handlers": {
            "console": {
                "class": "logging.StreamHandler",
                "formatter": "default",
                "filters": ["pipeline_context"],
                "stream": "ext://sys.stdout",
            },
        },
        "root": {"level": level, "handlers": ["console"]},
        "loggers": {
            "botocore.credentials": {
                "level": "WARNING",
                "propagate": True,
            },
        },
    }

    logging.config.dictConfig(config)
