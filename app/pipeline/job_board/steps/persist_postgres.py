"""Step 4: persist verified rows to Postgres with JSONL fallback."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from sqlalchemy.exc import OperationalError, ProgrammingError

from app.pipeline.job_board.contracts import PostgresPersistStats
from app.services.job_board_ml.fallback import append_rows_jsonl
from app.services.job_board_ml.postgres_sync import build_sync_engine, sync_job_board_rows


def persist_verified_to_postgres(
    *,
    rows: list[dict[str, Any]],
    batch_size: int = 40,
    fallback_jsonl: Path | None = None,
) -> PostgresPersistStats:
    stats = PostgresPersistStats()
    if not rows:
        return stats

    engine = build_sync_engine()
    try:
        s = sync_job_board_rows(engine, rows, batch_size=int(batch_size))
        stats.inserted = int(s.inserted)
        stats.updated = int(s.updated)
        stats.skipped = int(s.skipped)
        return stats
    except ProgrammingError:
        # schema issue should fail loud; no fallback for logic/programming errors
        raise
    except OperationalError as e:
        if fallback_jsonl is not None:
            append_rows_jsonl(fallback_jsonl, rows, reason=str(e))
            stats.fallback_appended = len(rows)
            stats.used_fallback = True
            return stats
        raise
    finally:
        engine.dispose()
