"""Contracts for the modular job-board pipeline."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


@dataclass
class CrawlStepOutput:
    """Output from step 1 crawler."""

    batch_id: str
    source_offset: int
    jobs_json_path: Path
    jobs_count: int


@dataclass
class MongoIngestStats:
    """Stats for step 2 Mongo ingest."""

    attempted: int = 0
    upserted: int = 0
    errors: int = 0


@dataclass
class MLClassifyStats:
    """Stats for step 3 direct ML classification."""

    processed: int = 0
    verified: int = 0
    rejected: int = 0
    spam_prefilter: int = 0
    no_apply_contact: int = 0
    title_company: int = 0
    experience_cap: int = 0
    depth_gate: int = 0
    quality_gate: int = 0
    reason_counts: dict[str, int] = field(default_factory=dict)


@dataclass
class PostgresPersistStats:
    """Stats for step 4 Postgres persistence."""

    inserted: int = 0
    updated: int = 0
    skipped: int = 0
    fallback_appended: int = 0
    used_fallback: bool = False


@dataclass
class PipelineResult:
    """Top-level result emitted by the end-to-end runner."""

    crawl: CrawlStepOutput
    mongo: MongoIngestStats
    ml: MLClassifyStats
    postgres: PostgresPersistStats
    sheet_export_ran: bool
    sheet_exit_code: int
    verified_rows: list[dict[str, Any]]
