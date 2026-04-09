"""Request lifecycle logging middleware.

Emits a single consolidated log per request containing all context fields:
request ID, method, path, status code, duration, client IP, user agent,
and any domain-specific fields added by route handlers (e.g. tag_id,
record_count).
"""

import logging
import time

from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint

from app.logging_config import new_request_id, request_id_ctx, request_log_ctx

logger = logging.getLogger(__name__)

# Requests slower than this threshold get logged at WARNING instead of INFO.
SLOW_REQUEST_THRESHOLD_MS = 5_000


class RequestLoggingMiddleware(BaseHTTPMiddleware):
    """Collect context throughout the request and emit one log at the end."""

    async def dispatch(
        self, request: Request, call_next: RequestResponseEndpoint
    ) -> Response:
        """Wrap a single request in a consolidated log entry."""
        # --- Resolve or generate a request correlation ID ---
        rid = request.headers.get("x-request-id") or new_request_id()
        request_id_ctx.set(rid)

        # Fresh dict for this request — route handlers can add fields to it.
        request_log_ctx.set({})

        method = request.method
        path = request.url.path
        client_ip = request.headers.get(
            "x-forwarded-for", request.client.host if request.client else "unknown"
        )
        user_agent = request.headers.get("user-agent", "")

        start = time.perf_counter()
        try:
            response = await call_next(request)
        except Exception:
            duration_ms = round((time.perf_counter() - start) * 1_000, 2)
            # On unhandled exceptions we still emit one log with everything we have.
            log_fields = {
                **request_log_ctx.get({}),
                "method": method,
                "path": path,
                "client_ip": client_ip,
                "user_agent": user_agent,
                "duration_ms": duration_ms,
                "status": "error",
            }
            logger.exception("Request failed", extra=log_fields)
            raise

        duration_ms = round((time.perf_counter() - start) * 1_000, 2)
        status_code = response.status_code
        is_slow = duration_ms > SLOW_REQUEST_THRESHOLD_MS

        # Merge handler-supplied fields with middleware fields into one dict.
        log_fields = {
            **request_log_ctx.get({}),
            "method": method,
            "path": path,
            "client_ip": client_ip,
            "user_agent": user_agent,
            "status_code": status_code,
            "duration_ms": duration_ms,
            "slow": is_slow,
        }

        if status_code >= 500:
            logger.error("Request completed", extra=log_fields)
        elif status_code >= 400 or is_slow:
            logger.warning("Request completed", extra=log_fields)
        else:
            logger.info("Request completed", extra=log_fields)

        return response
