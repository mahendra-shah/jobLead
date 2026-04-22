from __future__ import annotations

import os
import time
import uuid
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable
from urllib.parse import urlparse

from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import OperationalError

from app.config import settings
from app.utils.job_dedupe import normalize_url


@dataclass
class SyncStats:
    inserted: int = 0
    updated: int = 0
    skipped: int = 0


def _sync_db_url() -> str:
    local_db_url = os.getenv("LOCAL_DATABASE_URL")
    if local_db_url:
        db_url = local_db_url
    else:
        db_url = str(settings.DATABASE_URL).replace("+asyncpg", "")
    db_url = db_url.replace("?ssl=require", "?sslmode=require")
    db_url = db_url.replace("&ssl=require", "&sslmode=require")
    return db_url


def build_sync_engine() -> Engine:
    return create_engine(_sync_db_url(), pool_pre_ping=True)


def run_db_with_retries(fn: Callable[[], SyncStats], retries: int = 3, base_delay_s: float = 1.5) -> SyncStats:
    last_error: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            return fn()
        except OperationalError as exc:
            last_error = exc
            if attempt >= retries:
                raise
            time.sleep(base_delay_s * attempt)
    if last_error is not None:
        raise last_error
    return SyncStats()


def _as_list(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(x).strip() for x in value if str(x).strip()]
    if isinstance(value, str):
        parts = [p.strip() for p in value.split(",")]
        return [p for p in parts if p]
    return []


def _source_domain(row: dict[str, Any], source_url: str) -> str:
    domain = str(row.get("source_domain") or "").strip()
    if domain:
        return domain
    if source_url:
        return urlparse(source_url).netloc
    return ""


def _column_max_lengths(engine_columns: list[dict[str, Any]]) -> dict[str, int]:
    """Return VARCHAR max lengths by column name from inspector metadata."""
    limits: dict[str, int] = {}
    for col in engine_columns:
        name = str(col.get("name") or "")
        typ = col.get("type")
        if not name or typ is None:
            continue
        length = getattr(typ, "length", None)
        if isinstance(length, int) and length > 0:
            limits[name] = length
    return limits


def _apply_varchar_limits(values: dict[str, Any], limits: dict[str, int]) -> dict[str, Any]:
    """Trim string values to DB varchar limits to avoid insert/update failures."""
    out = dict(values)
    for key, max_len in limits.items():
        val = out.get(key)
        if isinstance(val, str) and len(val) > max_len:
            out[key] = val[:max_len]
    return out


def sync_job_board_rows(
    engine: Engine,
    rows: list[dict[str, Any]],
    *,
    dry_run: bool = False,
    batch_size: int = 40,
) -> SyncStats:
    stats = SyncStats()
    if not rows:
        return stats

    inspector = inspect(engine)
    engine_columns = inspector.get_columns("jobs")
    cols = {c["name"] for c in engine_columns}
    varchar_limits = _column_max_lengths(engine_columns)
    now = datetime.now(timezone.utc).replace(tzinfo=None)

    with engine.begin() as conn:
        for idx, row in enumerate(rows, start=1):
            source_url_raw = str(row.get("apply_url") or row.get("url") or "").strip()
            source_url = normalize_url(source_url_raw) or source_url_raw
            dedupe_key = str(row.get("_dedupe_key") or "").strip()

            if not source_url and not dedupe_key:
                stats.skipped += 1
                continue

            payload: dict[str, Any] = {
                "title": str(row.get("title") or "").strip(),
                "company_name": str(row.get("company") or "").strip() or None,
                "description": row.get("description"),
                "skills_required": json.dumps(_as_list(row.get("skills_required") or row.get("skills"))),
                "experience_required": row.get("experience_required") or row.get("experience") or None,
                "experience": row.get("experience") or row.get("experience_required") or None,
                "salary": row.get("salary") or None,
                # Keep JSON text so it works with raw SQL bindings and JSON columns.
                "salary_range": json.dumps({"raw": row.get("salary")}) if row.get("salary") else None,
                "location": row.get("location") or row.get("location_detail") or None,
                "job_type": row.get("work_type") or None,
                "employment_type": row.get("employment_type") or "fulltime",
                "source": "job_board",
                "source_url": source_url or None,
                "source_channel_name": _source_domain(row, source_url),
                "source_message_id": dedupe_key or None,
                "ml_confidence": str((row.get("_ml_scores") or {}).get("confidence") or "") or None,
                "is_active": True,
                "is_verified": True,
                "updated_at": now,
            }

            insert_values = {"id": str(uuid.uuid4()), "created_at": now, **payload}
            payload = {k: v for k, v in payload.items() if k in cols}
            insert_values = {k: v for k, v in insert_values.items() if k in cols}
            payload = _apply_varchar_limits(payload, varchar_limits)
            insert_values = _apply_varchar_limits(insert_values, varchar_limits)

            if "title" in insert_values and not insert_values["title"]:
                stats.skipped += 1
                continue

            where_parts: list[str] = ["source = 'job_board'"]
            bind_params: dict[str, Any] = {}
            if dedupe_key and "source_message_id" in cols:
                where_parts.append("source_message_id = :dedupe_key")
                bind_params["dedupe_key"] = dedupe_key
            if source_url and "source_url" in cols:
                where_parts.append("source_url = :source_url")
                bind_params["source_url"] = source_url
            if len(where_parts) == 1:
                stats.skipped += 1
                continue

            where_sql = f"({') OR ('.join(where_parts[1:])}) AND {where_parts[0]}"
            existing = conn.execute(
                text(f"SELECT id FROM jobs WHERE {where_sql} ORDER BY updated_at DESC NULLS LAST LIMIT 1"),
                bind_params,
            ).fetchone()

            if dry_run:
                if existing:
                    stats.updated += 1
                else:
                    stats.inserted += 1
                continue

            if existing:
                set_fields = {k: v for k, v in payload.items() if k not in {"created_at", "id"}}
                if set_fields:
                    set_sql = ", ".join([f"{k} = :set_{k}" for k in set_fields.keys()])
                    set_params = {f"set_{k}": v for k, v in set_fields.items()}
                    set_params["id"] = str(existing[0])
                    conn.execute(text(f"UPDATE jobs SET {set_sql} WHERE id = :id"), set_params)
                stats.updated += 1
            else:
                keys = list(insert_values.keys())
                cols_sql = ", ".join(keys)
                vals_sql = ", ".join([f":{k}" for k in keys])
                conn.execute(text(f"INSERT INTO jobs ({cols_sql}) VALUES ({vals_sql})"), insert_values)
                stats.inserted += 1

    return stats
