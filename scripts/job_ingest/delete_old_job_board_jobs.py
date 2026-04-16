#!/usr/bin/env python3
"""
Backward-compatible wrapper for moved job-board cleanup script.

New location:
  scripts/job_board_flow/job_ingest/delete_old_job_board_jobs.py
"""

from __future__ import annotations

import runpy
import sys
from pathlib import Path


def main() -> int:
    root = Path(__file__).resolve().parents[2]
    target = root / "scripts" / "job_board_flow" / "job_ingest" / "delete_old_job_board_jobs.py"
    if not target.exists():
        print(f"Target script not found: {target}", file=sys.stderr)
        return 2

    runpy.run_path(str(target), run_name="__main__")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
