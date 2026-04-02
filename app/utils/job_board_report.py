"""Helpers for persisting and reading job-board run reports in Redis."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict

import redis as redis_mod

logger = logging.getLogger(__name__)

JOB_BOARD_REPORT_KEY = "job_board:last_report"
JOB_BOARD_REPORT_HISTORY_KEY = "job_board:history"


def utc_now_iso() -> str:
    """Return current UTC timestamp in ISO-8601 format."""
    return datetime.now(timezone.utc).isoformat()


def _serialize_value(value: Any) -> Any:
    """Convert objects to JSON-safe values recursively."""
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat()
    if isinstance(value, dict):
        return {str(k): _serialize_value(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_serialize_value(v) for v in value]
    return value


def write_job_board_report(
    *,
    redis_url: str,
    report: Dict[str, Any],
    ttl_seconds: int = 7 * 24 * 3600,
    history_limit: int = 7,
) -> None:
    """Write latest report and append it to a bounded history list."""
    payload = _serialize_value(report)
    raw = json.dumps(payload)

    client = None
    try:
        client = redis_mod.from_url(
            redis_url,
            decode_responses=True,
            socket_timeout=3,
        )
        client.setex(JOB_BOARD_REPORT_KEY, int(ttl_seconds), raw)
        client.lpush(JOB_BOARD_REPORT_HISTORY_KEY, raw)
        client.ltrim(JOB_BOARD_REPORT_HISTORY_KEY, 0, max(0, int(history_limit) - 1))
        client.expire(JOB_BOARD_REPORT_HISTORY_KEY, int(ttl_seconds))
    except Exception as exc:
        logger.warning("job_board_report_write_failed: %s", exc)
    finally:
        if client is not None:
            try:
                client.close()
            except Exception:
                pass


def read_job_board_report(*, redis_url: str) -> Dict[str, Any]:
    """Read the latest job-board run report from Redis."""
    client = None
    try:
        client = redis_mod.from_url(
            redis_url,
            decode_responses=True,
            socket_timeout=3,
        )
        raw = client.get(JOB_BOARD_REPORT_KEY)
        if not raw:
            return {
                "status": "never_run",
                "note": "No JobBoard run report recorded yet.",
            }
        decoded = json.loads(raw)
        if isinstance(decoded, dict):
            return decoded
        return {
            "status": "invalid_report",
            "note": "Unexpected JobBoard report payload type.",
        }
    except Exception as exc:
        return {
            "status": "unavailable",
            "error": str(exc),
        }
    finally:
        if client is not None:
            try:
                client.close()
            except Exception:
                pass
