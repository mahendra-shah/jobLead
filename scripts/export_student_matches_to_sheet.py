#!/usr/bin/env python3
"""
Export student-job matches to Google Sheets tab for a given date.
Tab name: student_match_YYYY_MM_DD
- No emails sent
- No DB updates
- Robust error handling and edge case coverage
"""

from __future__ import annotations
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import argparse
import re
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Sequence, Set, Tuple

from sqlalchemy import create_engine, text
from app.config import settings
from app.utils.timezone import now_ist

try:
    from app.services.google_sheets_service import GoogleSheetsService
except ImportError as e:
    print(f"[IMPORT][ERROR] Could not import GoogleSheetsService: {e}", flush=True)
    GoogleSheetsService = None

# --- Utility functions (copied from main script) ---
def normalize_token(value: str) -> str:
    if not value:
        return ""
    cleaned = re.sub(r"[^a-z0-9+.#]+", " ", value.lower()).strip()
    return re.sub(r"\s+", " ", cleaned)

def normalize_list(values: Sequence[str] | None) -> List[str]:
    if not values:
        return []
    out: List[str] = []
    seen: Set[str] = set()
    for raw in values:
        token = normalize_token(str(raw))
        if not token:
            continue
        if token in seen:
            continue
        seen.add(token)
        out.append(token)
    return out

def normalize_maybe_list(value) -> List[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return normalize_list([str(item) for item in value])
    return normalize_list([str(value)])

def sanitize_email(value: str) -> str:
    email = (value or "").strip().lower()
    email = re.sub(r"\s+", "", email)
    email = email.rstrip("|,;")
    return email

def is_valid_email(value: str) -> bool:
    if not value:
        return False
    return re.match(r"^[a-z0-9._%+\-]+@[a-z0-9.\-]+\.[a-z]{2,}$", value) is not None

def resolve_job_window(job_date_arg: str | None) -> Tuple[datetime, datetime, str]:
    if job_date_arg:
        target_date = datetime.strptime(job_date_arg, "%Y-%m-%d")
    else:
        target_date = (now_ist() - timedelta(days=1)).replace(tzinfo=None)
    start_local = target_date.replace(hour=0, minute=0, second=0, microsecond=0)
    end_local = start_local + timedelta(days=1)
    start_utc = start_local - timedelta(hours=5, minutes=30)
    end_utc = end_local - timedelta(hours=5, minutes=30)
    return start_utc, end_utc, start_local.strftime("%Y-%m-%d")

# --- Main export logic ---

def normalize_db_url() -> str:
    db_url = str(settings.DATABASE_URL)
    if db_url.startswith("postgresql+asyncpg://"):
        db_url = db_url.replace("postgresql+asyncpg://", "postgresql://", 1)
    return db_url

def match_student_jobs(student, jobs, min_score=2, max_jobs_per_student=5, debug_fn=None):
    primary_skills = normalize_list(student.skills or [])
    technical_skills = normalize_list(student.technical_skills or [])
    merged_skills = normalize_list(primary_skills + technical_skills)
    preferred_roles = normalize_list(student.preferred_job_role or [])
    preference_obj = student.preference if isinstance(student.preference, dict) else {}
    preferred_job_types = normalize_maybe_list(preference_obj.get("job_type"))
    preferred_job_categories = normalize_list(
        normalize_maybe_list(student.job_category)
        + normalize_maybe_list(preference_obj.get("job_category"))
    )
    cleaned_email = sanitize_email(student.email or "")
    if not is_valid_email(cleaned_email):
        if debug_fn:
            debug_fn(f"[SKIP] {student.full_name} ({student.email}): invalid email")
        return []
    matches = []
    found_match = False
    for job in jobs:
        if student.id in set(job.students_shown_to or []):
            continue
        job_text = normalize_token(" ".join([
            job.title, job.company_name, job.description, job.job_type, job.employment_type
        ]))
        job_tokens = set(normalize_list(job.skills_required or []) + [job.title, job.company_name, job.job_type, job.employment_type])
        matched_skills = [s for s in merged_skills if s and (s in job_tokens or s in job_text)]
        matched_roles = [r for r in preferred_roles if r and (r in job_tokens or r in job_text)]
        matched_job_types = [jt for jt in preferred_job_types if jt and (jt in job_tokens or jt in job_text)]
        matched_job_categories = [jc for jc in preferred_job_categories if jc and (jc in job_tokens or jc in job_text)]
        has_skill_or_role_input = bool(merged_skills) or bool(preferred_roles)
        has_skill_or_role_match = bool(matched_skills) or bool(matched_roles)
        if not has_skill_or_role_input:
            if debug_fn:
                debug_fn(f"[SKIP] {student.full_name} ({student.email}): no skill or role input")
            continue
        if not has_skill_or_role_match:
            continue
        score = (
            (len(set(matched_skills)) * 3)
            + (len(set(matched_roles)) * 2)
            + (len(set(matched_job_types)) * 2)
            + (len(set(matched_job_categories)) * 2)
        )
        if score < min_score:
            if debug_fn:
                debug_fn(f"[SKIP] {student.full_name} ({student.email}): score {score} < min_score {min_score}")
            continue
        found_match = True
        matches.append({
            "student": student,
            "job": job,
            "matched_skills": matched_skills,
            "matched_roles": matched_roles,
            "matched_job_types": matched_job_types,
            "matched_job_categories": matched_job_categories,
            "score": score,
            "cleaned_email": cleaned_email,
        })
    # Sort by score descending, then by job id for stability
    matches.sort(key=lambda m: (-m["score"], m["job"].id))
    return matches[:max_jobs_per_student]
    if not found_match and debug_fn:
        debug_fn(f"[SKIP] {student.full_name} ({student.email}): no matches found")
    return matches[:max_jobs_per_student]

def main():
    parser = argparse.ArgumentParser(description="Export student-job matches to Google Sheets tab for a given date.")
    parser.add_argument("--job-date", default=None, help="Job date in IST (YYYY-MM-DD). Default: yesterday in IST")
    parser.add_argument("--min-score", type=int, default=2, help="Minimum match score (default: 2)")
    parser.add_argument("--max-jobs-per-student", type=int, default=5, help="Maximum jobs to export per student (default: 5)")
    args = parser.parse_args()

    # Check config
    if not hasattr(settings, 'SHEET_ID') or not settings.SHEET_ID:
        print("❌ SHEET_ID not configured in settings. Aborting export.")
        sys.exit(1)
    if GoogleSheetsService is None:
        print("❌ GoogleSheetsService import failed. Aborting export.")
        sys.exit(1)

    db_url = normalize_db_url()
    engine = create_engine(db_url)

    export_header = [
        "Student Name",
        "Student Email",
        "ID (Student ID)",
        "Message ID (Job ID)",
        "Date (Match/Job Date)",
        "Time (Match/Job Time)",
        "Job Type",
        "Keywords Found",
        "Apply Link",
        "Full Message Text",
    ]
    export_rows = []

    with engine.connect() as conn:
        try:
            students = list(conn.execute(text("""
                SELECT
                    id::text AS id,
                    COALESCE(full_name, '') AS full_name,
                    lower(trim(email)) AS email,
                    COALESCE(skills, '[]'::jsonb) AS skills,
                    COALESCE(technical_skills, '[]'::jsonb) AS technical_skills,
                    COALESCE(preferred_job_role, '[]'::jsonb) AS preferred_job_role,
                    COALESCE(preference, '{}'::jsonb) AS preference,
                    COALESCE(job_category, '') AS job_category
                FROM students
                WHERE email IS NOT NULL
                  AND btrim(email) <> ''
                  AND COALESCE(email_notifications, TRUE) = TRUE
                  AND COALESCE(status, 'active') = 'active'
                ORDER BY created_at DESC
            """)))
            start_utc, end_utc, job_date_label = resolve_job_window(args.job_date)
            jobs = list(conn.execute(text("""
                SELECT
                    id::text AS id,
                    COALESCE(title, '') AS title,
                    COALESCE(company_name, '') AS company_name,
                    COALESCE(location, '') AS location,
                    COALESCE(description, '') AS description,
                    COALESCE(source_url, '') AS source_url,
                    COALESCE(skills_required, '[]'::jsonb) AS skills_required,
                    COALESCE(job_type, '') AS job_type,
                    COALESCE(employment_type, '') AS employment_type,
                    COALESCE(students_shown_to, '[]'::jsonb) AS students_shown_to,
                    created_at
                FROM jobs
                WHERE COALESCE(is_active, TRUE) = TRUE
                  AND created_at >= :start_utc
                  AND created_at < :end_utc
                ORDER BY created_at DESC
            """), {"start_utc": start_utc, "end_utc": end_utc}))
        except Exception as e:
            print(f"❌ DB query failed: {e}")
            sys.exit(1)

        print(f"Loaded students: {len(students)}")
        print(f"Loaded active jobs for date {job_date_label} (IST): {len(jobs)}")

        def debug_fn(msg):
            print(msg)

        for student in students:
            matches = match_student_jobs(student, jobs, min_score=args.min_score, max_jobs_per_student=args.max_jobs_per_student, debug_fn=debug_fn)
            for match in matches:
                job = match["job"]
                date_str = ""
                time_str = ""
                if hasattr(job, "created_at") and job.created_at:
                    try:
                        dt = job.created_at
                        date_str = dt.strftime("%Y-%m-%d")
                        time_str = dt.strftime("%H:%M:%S")
                    except Exception:
                        pass
                export_rows.append([
                    student.full_name,
                    match["cleaned_email"],
                    student.id,
                    job.id,
                    date_str,
                    time_str,
                    job.job_type,
                    ", ".join(match["matched_skills"] + match["matched_roles"]),
                    job.source_url,
                    (job.description or "")[:1000],
                ])

    # --- Export to Google Sheets ---
    print(f"[EXPORT] Preparing to export {len(export_rows)} rows to Google Sheet tab for {job_date_label}")
    try:
        sheets_service = GoogleSheetsService()
        tab_name = f"student_match_{job_date_label.replace('-', '_')}"
        sheet_metadata = sheets_service.sheets.get(spreadsheetId=sheets_service.sheet_id).execute()
        sheet_names = [s['properties']['title'] for s in sheet_metadata.get('sheets', [])]
        if tab_name not in sheet_names:
            print(f"[EXPORT] Tab '{tab_name}' not found. Creating...")
            add_sheet_request = {
                'requests': [{
                    'addSheet': {
                        'properties': {'title': tab_name}
                    }
                }]
            }
            sheets_service.sheets.batchUpdate(
                spreadsheetId=sheets_service.sheet_id,
                body=add_sheet_request
            ).execute()
            print(f"[EXPORT] Created new tab '{tab_name}' in sheet.")
        all_rows = [export_header] + export_rows if export_rows else [export_header]
        end_col = chr(ord('A') + len(all_rows[0]) - 1)
        range_name = f"{tab_name}!A1:{end_col}{len(all_rows)}"
        print(f"[EXPORT] Writing {len(all_rows)} rows to range: {range_name}")
        sheets_service.sheets.values().update(
            spreadsheetId=sheets_service.sheet_id,
            range=range_name,
            valueInputOption='RAW',
            body={'values': all_rows},
        ).execute()
        print(f"[EXPORT] Successfully wrote data to tab '{tab_name}'")
        if len(export_rows) == 0:
            print(f"[EXPORT] No matched jobs to export for this run. Only header written to tab '{tab_name}'.")
        else:
            print(f"[EXPORT] Exported {len(export_rows)} matched rows to Google Sheet tab '{tab_name}'")
    except Exception as exc:
        print(f"❌ Failed to export to Google Sheets: {exc}")
        sys.exit(1)

if __name__ == "__main__":
    main()
