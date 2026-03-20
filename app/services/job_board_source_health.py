"""Minimal health check for job-board sources.

We keep it lightweight:
- return only `health_score` and `last_health_check_at`
- best-effort HTTP GET/HEAD with short timeouts
"""

from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Dict

import httpx


def check_source_health(url: str, use_get: bool = True) -> Dict[str, object]:
    """
    Returns:
      {
        "health_score": float,
        "last_health_check_at": datetime
      }
    """
    checked_at = datetime.now(timezone.utc)
    if not url or not url.startswith(("http://", "https://")):
        return {"health_score": 0.0, "last_health_check_at": checked_at}

    method = "GET" if use_get else "HEAD"
    timeout_s = 8.0

    # Simple scoring: fast + success => higher score, otherwise degrade.
    try:
        start = time.time()
        headers = {
            "User-Agent": "PlacementJobBoard/1.0",
            "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
        }
        async_timeout = httpx.Timeout(timeout_s)
        with httpx.Client(timeout=async_timeout, follow_redirects=True) as client:
            resp = client.request(method, url, headers=headers)
        elapsed_ms = (time.time() - start) * 1000.0

        if 200 <= resp.status_code < 400:
            # Map elapsed_ms to score in [60..100] and bump by status family.
            base = max(0.0, 100.0 - (elapsed_ms / 50.0))
            if 200 <= resp.status_code < 300:
                base += 5.0
            score = min(100.0, max(0.0, float(base)))
            return {"health_score": round(score, 2), "last_health_check_at": checked_at}

        # non-success status
        return {"health_score": 10.0, "last_health_check_at": checked_at}
    except httpx.TimeoutException:
        return {"health_score": 0.0, "last_health_check_at": checked_at}
    except Exception:
        return {"health_score": 0.0, "last_health_check_at": checked_at}

