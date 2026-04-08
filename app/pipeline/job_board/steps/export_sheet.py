"""Step 5: export job-board rows to Google Sheets."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path


def export_sheet_from_postgres(
    *,
    root: Path,
    append_jobs: bool = False,
    max_jobs_per_domain: int = 0,
    sheet_date: str | None = None,
) -> int:
    py = sys.executable
    cmd = [
        py,
        "scripts/export_job_board_jobs_to_sheets.py",
        "--from-postgres",
    ]
    if sheet_date:
        cmd.extend(["--date", str(sheet_date)])
    if append_jobs:
        cmd.append("--append-jobs")
    if int(max_jobs_per_domain) > 0:
        cmd.extend(["--max-jobs-per-domain", str(int(max_jobs_per_domain))])
    return int(subprocess.run(cmd, cwd=root).returncode)


def export_sheet_from_json(
    *,
    root: Path,
    jobs_json_path: Path,
    append_jobs: bool = False,
    sheet_date: str | None = None,
) -> int:
    """Export from jobs JSON (used when Postgres fallback is active)."""
    py = sys.executable
    path = jobs_json_path if jobs_json_path.is_absolute() else (root / jobs_json_path)
    cmd = [
        py,
        "scripts/export_job_board_jobs_to_sheets.py",
        "--jobs-json",
        str(path),
    ]
    if sheet_date:
        cmd.extend(["--date", str(sheet_date)])
    if append_jobs:
        cmd.append("--append-jobs")
    return int(subprocess.run(cmd, cwd=root).returncode)
