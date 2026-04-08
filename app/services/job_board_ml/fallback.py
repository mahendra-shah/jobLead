"""Append-only JSONL fallback when Postgres is unreachable during ingest."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def append_rows_jsonl(path: Path, rows: list[dict[str, Any]], *, reason: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    with path.open("a", encoding="utf-8") as f:
        for row in rows:
            rec = {
                "fallback_at_utc": ts,
                "postgres_error_reason": reason,
                "job": {k: v for k, v in row.items() if not str(k).startswith("_")},
                "dedupe_key": row.get("_dedupe_key"),
                "ml_scores": row.get("_ml_scores"),
            }
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
