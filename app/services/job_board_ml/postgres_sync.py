"""Postgres upsert for job_board rows (shared by ML worker and batch sync script)."""

from __future__ import annotations

import os
import time
import uuid
import warnings
from dataclasses import dataclass
from typing import Any, Callable

from sqlalchemy import MetaData, Table, create_engine, func as sa_func, inspect, insert, select, update
from sqlalchemy.engine import Engine
from sqlalchemy.exc import OperationalError

from app.config import settings


def as_skills_list(v: Any) -> list[str]:
    if isinstance(v, list):
        return [str(x).strip() for x in v if str(x).strip()]
    if isinstance(v, str):
        return [x.strip() for x in v.split(",") if x.strip()]
    return []


def salary_range_payload(row: dict) -> dict:
    raw = row.get("salary") or ""
    if row.get("salary_min") is None and row.get("salary_max") is None and not raw:
        return {}
    return {
        "min": row.get("salary_min"),
        "max": row.get("salary_max"),
        "currency": "INR",
        "raw": raw,
    }


def location_line(row: dict) -> str:
    loc = (row.get("location_detail") or row.get("location") or "").strip()
    return loc[:255] if loc else ""


def job_board_export_block(row: dict, existing_q: dict | None) -> dict:
    prev = {}
    if isinstance(existing_q, dict):
        inner = existing_q.get("job_board_export")
        if isinstance(inner, dict):
            prev = dict(inner)
    co = (row.get("country") or "").strip()
    deg = (row.get("degree") or row.get("education") or "").strip()
    posted = (row.get("job_posted_at_raw") or row.get("posted_at") or "").strip()
    disc = (row.get("source_discovered_date") or "").strip()
    out = {
        **prev,
        "country": co or prev.get("country") or "",
        "degree": deg or prev.get("degree") or "",
        "job_posted_at_raw": posted or prev.get("job_posted_at_raw") or "",
        "source_discovered_date": disc or prev.get("source_discovered_date") or "",
    }
    return {k: v for k, v in out.items() if v}


def merge_quality_breakdown(existing_q: dict | None, row: dict) -> dict:
    base = dict(existing_q) if isinstance(existing_q, dict) else {}
    export = job_board_export_block(row, base)
    if export:
        base["job_board_export"] = export
    return base


def is_transient_db_error(exc: Exception) -> bool:
    msg = str(exc).lower()
    return any(
        m in msg
        for m in (
            "could not receive data from server",
            "connection was closed",
            "server closed the connection",
            "connection reset",
            "network is unreachable",
            "ssl syscall",
            "timeout",
            "could not connect to server",
            "connection refused",
            "temporary failure",
        )
    )


def run_db_with_retries(fn: Callable[[], Any], *, max_retries: int = 5, base_delay: float = 2.0) -> Any:
    for attempt in range(max_retries):
        try:
            return fn()
        except OperationalError as e:
            if not is_transient_db_error(e) or attempt >= max_retries - 1:
                raise
            delay = base_delay * (2**attempt)
            print(f"job_board_ml.postgres_sync: transient DB error, retry in {delay:.1f}s ({e})", flush=True)
            time.sleep(delay)


def ml_confidence_str(row: dict) -> str | None:
    conf = (row.get("_ml_scores") or {}).get("confidence")
    if conf is None:
        return None
    try:
        return f"{float(conf):.6f}"[:10]
    except (TypeError, ValueError):
        return str(conf)[:10] or None


def build_sync_engine() -> Engine:
    local_db_url = os.getenv("LOCAL_DATABASE_URL")
    if local_db_url:
        sync_database_url = local_db_url
    else:
        sync_database_url = str(settings.DATABASE_URL).replace("+asyncpg", "")
        sync_database_url = sync_database_url.replace("?ssl=require", "?sslmode=require")
        sync_database_url = sync_database_url.replace("&ssl=require", "&sslmode=require")

    return create_engine(
        sync_database_url,
        pool_pre_ping=True,
        pool_recycle=280,
        connect_args={
            "connect_timeout": 30,
            "keepalives": 1,
            "keepalives_idle": 30,
            "keepalives_interval": 10,
            "keepalives_count": 5,
        },
    )


@dataclass
class JobBoardSyncStats:
    inserted: int = 0
    updated: int = 0
    skipped: int = 0


def load_jobs_table(engine: Engine) -> tuple[Table, set[str], bool]:
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", message=".*Did not recognize type 'vector'.*")
        inspector = inspect(engine)
        job_columns = {str(c.get("name")) for c in inspector.get_columns("jobs")}
        md = MetaData()
        jobs_table = Table("jobs", md, autoload_with=engine)
    has_source_col = "source" in job_columns
    return jobs_table, job_columns, has_source_col


def _process_one_row(
    conn: Any,
    jobs_table: Table,
    job_columns: set[str],
    has_source_col: bool,
    row: dict,
    *,
    dry_run: bool,
    stats: JobBoardSyncStats,
) -> None:
    source_url = str(row.get("apply_url") or row.get("url") or "").strip()
    title = str(row.get("title") or "").strip()
    if not source_url or not title:
        stats.skipped += 1
        return

    where_expr = jobs_table.c.source_url == source_url
    if has_source_col:
        where_expr = where_expr & (jobs_table.c.source == "job_board")

    existing_row = conn.execute(select(jobs_table).where(where_expr).limit(1)).mappings().first()
    existing_qb = existing_row.get("quality_breakdown") if existing_row else None

    desc_full = (row.get("description") or "").strip()
    qbm = merge_quality_breakdown(existing_qb, row)
    payload_all = {
        "title": title[:500],
        "company_name": (row.get("company") or "")[:500] if isinstance(row.get("company"), str) else None,
        "description": desc_full or None,
        "location": location_line(row),
        "skills_required": as_skills_list(row.get("skills")),
        "experience_required": (row.get("seniority") or row.get("experience_required") or "")[:50],
        "work_type": (row.get("location_type") or "").lower() or None,
        "job_type": (row.get("work_type") or "").lower() or None,
        "employment_type": (row.get("work_type") or "").lower() or None,
        "salary_range": salary_range_payload(row),
        "salary_min": row.get("salary_min"),
        "salary_max": row.get("salary_max"),
        "source": "job_board",
        "source_url": source_url,
        "source_channel_name": row.get("source_domain"),
        "raw_text": desc_full or None,
        "source_message_id": str(row.get("_dedupe_key") or "")[:255] or None,
        "ml_confidence": ml_confidence_str(row),
        "quality_breakdown": qbm,
        "is_active": True,
        "is_verified": True,
    }
    payload = {k: v for k, v in payload_all.items() if k in job_columns}
    if has_source_col:
        payload["source"] = "job_board"

    if existing_row:
        if "updated_at" in job_columns:
            payload["updated_at"] = sa_func.now()
        if not dry_run:
            conn.execute(update(jobs_table).where(where_expr).values(**payload))
        stats.updated += 1
        return
    if "id" in job_columns and "id" not in payload:
        payload["id"] = uuid.uuid4()
    if "created_at" in job_columns and "created_at" not in payload:
        payload["created_at"] = sa_func.now()
    if "updated_at" in job_columns and "updated_at" not in payload:
        payload["updated_at"] = sa_func.now()
    if not dry_run:
        conn.execute(insert(jobs_table).values(**payload))
    stats.inserted += 1


def sync_job_board_rows(
    engine: Engine,
    rows: list[dict],
    *,
    dry_run: bool = False,
    batch_size: int = 40,
) -> JobBoardSyncStats:
    """Idempotent upsert for job_board source rows."""
    jobs_table, job_columns, has_source_col = load_jobs_table(engine)
    stats = JobBoardSyncStats()
    row_list = list(rows)

    for i in range(0, len(row_list), batch_size):
        chunk = row_list[i : i + batch_size]

        def _run_chunk() -> None:
            with engine.begin() as conn:
                for row in chunk:
                    _process_one_row(
                        conn,
                        jobs_table,
                        job_columns,
                        has_source_col,
                        row,
                        dry_run=dry_run,
                        stats=stats,
                    )

        run_db_with_retries(_run_chunk)

    return stats
