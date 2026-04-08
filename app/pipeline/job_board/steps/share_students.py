"""Optional step: share matched jobs with students via existing email workflow."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path


def share_jobs_with_students(
    *,
    root: Path,
    dry_run: bool = False,
    student_limit: int = 0,
    max_jobs_per_student: int = 5,
    min_score: int = 2,
    send_limit: int = 0,
    job_date: str | None = None,
) -> int:
    py = sys.executable
    cmd = [
        py,
        "scripts/send_matched_jobs_emails.py",
        "--max-jobs-per-student",
        str(int(max_jobs_per_student)),
        "--min-score",
        str(int(min_score)),
    ]
    if int(student_limit) > 0:
        cmd.extend(["--student-limit", str(int(student_limit))])
    if int(send_limit) > 0:
        cmd.extend(["--send-limit", str(int(send_limit))])
    if job_date:
        cmd.extend(["--job-date", str(job_date)])
    if dry_run:
        cmd.append("--dry-run")
    return int(subprocess.run(cmd, cwd=root).returncode)
