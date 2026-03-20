#!/usr/bin/env python3
"""
Import student profile data from Google Sheet (Resume Review tab) into students table.

Rules requested:
- full_name, email, preferred_job_role, job_category, skills store in DB
- email_notifications always True
- From skill category text like:
    "Frontend Development: React (Level 1), JavaScript (Level 1), TypeScript (Level 1)"
  preferred_job_role = "Frontend Development"
  skills = ["React", "JavaScript", "TypeScript"] (no level)

Usage:
  python3 scripts/import_students_from_resume_review.py \
    --spreadsheet-url "https://docs.google.com/spreadsheets/d/112GNJ2uwwoYbf7OdmjSPFgDa0m_nH-STEuVbeCpECeg/edit?gid=2000769700#gid=2000769700" \
    --tab-name "Resume Review"
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import uuid
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from google.oauth2 import service_account
from googleapiclient.discovery import build
from sqlalchemy import create_engine, text

# Project imports
sys.path.insert(0, str(Path(__file__).parent.parent))
from app.config import settings  # noqa: E402


DEFAULT_SPREADSHEET = "https://docs.google.com/spreadsheets/d/112GNJ2uwwoYbf7OdmjSPFgDa0m_nH-STEuVbeCpECeg/edit?gid=2000769700#gid=2000769700"
DEFAULT_TAB_NAME = "Resume Review"


def parse_spreadsheet_id(spreadsheet_url_or_id: str) -> str:
    value = (spreadsheet_url_or_id or "").strip()
    match = re.search(r"/spreadsheets/d/([a-zA-Z0-9-_]+)", value)
    return match.group(1) if match else value


def normalize_key(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", (value or "").strip().lower()).strip("_")


def clean_skill_token(token: str) -> str:
    token = re.sub(r"\(.*?\)", "", token or "")
    token = token.strip().strip("-").strip()
    return token


def _is_role_like_skill_token(token: str) -> bool:
    token_norm = (token or "").strip().lower()
    if not token_norm:
        return False

    # Role-like tokens that should be treated as preferred role, not skill
    role_like_exact = {
        "mern stack",
        "mean stack",
        "full stack",
        "fullstack",
    }

    if token_norm in role_like_exact:
        return True

    if "stack" in token_norm:
        return True

    return False


def sanitize_full_name(name: str) -> str:
    """Remove metadata suffixes from full_name (e.g., _Resume_1_1, -Resume, .resume, -RESUME, -FlowCV)."""
    if not name:
        return name

    # Remove patterns like _Resume_X_Y where X and Y are digits
    sanitized = re.sub(r"_Resume_\d+_\d+$", "", name, flags=re.IGNORECASE)

    # Remove common metadata patterns (case insensitive) with separators: _, -, .
    # Matches: _Resume, -Resume, .Resume, _RESUME, etc.
    sanitized = re.sub(r"[_\-.]\s*resume[\w\d_\-.]*$", "", sanitized, flags=re.IGNORECASE)
    sanitized = re.sub(r"[_\-.]\s*flowcv[\w\d_\-.]*$", "", sanitized, flags=re.IGNORECASE)
    sanitized = re.sub(r"[_\-.]\s*cv[\w\d_\-.]*$", "", sanitized, flags=re.IGNORECASE)
    sanitized = re.sub(r"[_\-.]\d+$", "", sanitized)  # Remove trailing _123 or -123 or .123 patterns
    sanitized = re.sub(r"_\d+_\d+$", "", sanitized)

    return sanitized.strip()


def parse_roles_and_skills(skill_text: str) -> Tuple[List[str], List[str]]:
    """
    Example:
      "Frontend Development: React (Level 1), JavaScript (Level 1), TypeScript (Level 1)"
    -> (["Frontend Development"], ["React", "JavaScript", "TypeScript"])
    """
    if not skill_text:
        return [], []

    raw = skill_text.replace("\n", " ").strip()
    if not raw:
        return [], []

    blocks = [block.strip() for block in raw.split("|") if block.strip()]
    preferred_roles: List[str] = []

    # Preferred roles = all category names (before each colon)
    for block in blocks if blocks else [raw]:
        if ":" in block:
            category_name = block.split(":", 1)[0].strip()
            if category_name:
                preferred_roles.append(category_name)

    # Skills = all category items after colon across all blocks
    skill_tokens: List[str] = []
    for block in blocks if blocks else [raw]:
        if ":" in block:
            _, block_skills_part = block.split(":", 1)
            source_for_skills = block_skills_part
        else:
            source_for_skills = block

        for token in re.split(r"[,;]+", source_for_skills):
            cleaned = clean_skill_token(token)
            if cleaned:
                if _is_role_like_skill_token(cleaned):
                    preferred_roles.append(cleaned)
                else:
                    skill_tokens.append(cleaned)

    # De-duplicate preserving order
    seen = set()
    deduped_skills: List[str] = []
    for item in skill_tokens:
        key = item.lower()
        if key in seen:
            continue
        seen.add(key)
        deduped_skills.append(item)

    seen_roles = set()
    deduped_roles: List[str] = []
    for item in preferred_roles:
        key = item.lower()
        if key in seen_roles:
            continue
        seen_roles.add(key)
        deduped_roles.append(item)

    return deduped_roles, deduped_skills


def get_value(row: List[str], index_map: Dict[str, int], keys: List[str]) -> str:
    for key in keys:
        idx = index_map.get(key)
        if idx is not None and idx < len(row):
            return (row[idx] or "").strip()
    return ""


def resolve_tab_title_case_insensitive(sheets_api, spreadsheet_id: str, requested_tab: str) -> str:
    meta = sheets_api.get(spreadsheetId=spreadsheet_id).execute()
    titles = [s["properties"]["title"] for s in meta.get("sheets", [])]
    requested_norm = requested_tab.strip().lower()

    for title in titles:
        if title.lower() == requested_norm:
            return title

    # fallback to first tab containing both words
    for title in titles:
        low = title.lower()
        if "resume" in low and "review" in low:
            return title

    raise ValueError(f"Tab '{requested_tab}' not found. Available tabs: {titles}")


def build_google_sheets_client(credentials_file: Optional[str]):
    if credentials_file:
        credentials_path = Path(credentials_file)
    elif settings.GOOGLE_CLIENT_SECRET:
        credentials_path = Path(settings.GOOGLE_CLIENT_SECRET)
    else:
        credentials_path = Path(__file__).parent.parent / "credentials.json"

    if not credentials_path.exists():
        raise FileNotFoundError(
            f"Google credentials file not found: {credentials_path}. "
            "Provide --credentials-file or place credentials.json at project root."
        )

    scopes = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
    creds = service_account.Credentials.from_service_account_file(str(credentials_path), scopes=scopes)
    service = build("sheets", "v4", credentials=creds)
    return service.spreadsheets()


def get_sync_db_url() -> str:
    db_url = str(settings.DATABASE_URL)
    if db_url.startswith("postgresql+asyncpg://"):
        db_url = db_url.replace("postgresql+asyncpg://", "postgresql://", 1)
    return db_url


def upsert_student(
    conn,
    *,
    full_name: str,
    email: str,
    preferred_roles: List[str],
    skills: List[str],
    job_category: Optional[str],
    dry_run: bool,
) -> str:
    email_normalized = (email or "").strip().lower()
    if not email_normalized:
        return "skipped"

    preferred_role_list = preferred_roles or []
    now = datetime.utcnow()

    existing = conn.execute(
        text("SELECT id FROM students WHERE lower(email) = :email LIMIT 1"),
        {"email": email_normalized},
    ).fetchone()

    preference_payload = {
        "preferred_job_role": preferred_role_list,
        "job_category": job_category,
    }

    params = {
        "id": str(uuid.uuid4()),
        "full_name": full_name,
        "email": email_normalized,
        "preferred_job_role": json.dumps(preferred_role_list),
        "skills": json.dumps(skills),
        "job_category": job_category,
        "preference": json.dumps(preference_payload),
        "now": now,
    }

    if dry_run:
        return "updated" if existing else "inserted"

    if existing:
        conn.execute(
            text(
                """
                UPDATE students
                SET
                    full_name = :full_name,
                    email = :email,
                    preferred_job_role = CAST(:preferred_job_role AS jsonb),
                    skills = CAST(:skills AS jsonb),
                    technical_skills = CASE
                        WHEN technical_skills IS NULL OR technical_skills = '[]'::jsonb
                        THEN CAST(:skills AS jsonb)
                        ELSE technical_skills
                    END,
                    job_category = :job_category,
                    preference = COALESCE(preference, '{}'::jsonb) || CAST(:preference AS jsonb),
                    email_notifications = TRUE,
                    status = COALESCE(status, 'active'),
                    updated_at = :now
                WHERE id = :id
                """
            ),
            {**params, "id": str(existing[0])},
        )
        return "updated"

    conn.execute(
        text(
            """
            INSERT INTO students (
                id,
                full_name,
                email,
                preferred_job_role,
                job_category,
                skills,
                technical_skills,
                preference,
                email_notifications,
                status,
                created_at,
                updated_at
            ) VALUES (
                CAST(:id AS uuid),
                :full_name,
                :email,
                CAST(:preferred_job_role AS jsonb),
                :job_category,
                CAST(:skills AS jsonb),
                CAST(:skills AS jsonb),
                CAST(:preference AS jsonb),
                TRUE,
                'active',
                :now,
                :now
            )
            """
        ),
        params,
    )
    return "inserted"


def main() -> int:
    parser = argparse.ArgumentParser(description="Import students from Resume Review tab into students table")
    parser.add_argument("--spreadsheet-url", default=DEFAULT_SPREADSHEET, help="Google Sheet URL or spreadsheet ID")
    parser.add_argument("--tab-name", default=DEFAULT_TAB_NAME, help="Sheet tab name (default: Resume Review)")
    parser.add_argument("--credentials-file", default=None, help="Path to Google service account JSON")
    parser.add_argument("--limit", type=int, default=0, help="Optional max rows to process")
    parser.add_argument("--dry-run", action="store_true", help="Parse and report without writing DB")
    args = parser.parse_args()

    spreadsheet_id = parse_spreadsheet_id(args.spreadsheet_url)
    sheets = build_google_sheets_client(args.credentials_file)
    tab_title = resolve_tab_title_case_insensitive(sheets, spreadsheet_id, args.tab_name)

    result = sheets.values().get(
        spreadsheetId=spreadsheet_id,
        range=f"'{tab_title}'!A1:ZZ",
    ).execute()
    values = result.get("values", [])

    if not values:
        print("No data found in the sheet tab.")
        return 1

    header = values[0]
    rows = values[1:]

    if args.limit and args.limit > 0:
        rows = rows[: args.limit]

    index_map = {normalize_key(col): idx for idx, col in enumerate(header)}

    db_url = get_sync_db_url()
    engine = create_engine(db_url)

    inserted = 0
    updated = 0
    skipped = 0
    errors = 0

    with engine.begin() as conn:
        for row_num, row in enumerate(rows, start=2):
            full_name = get_value(row, index_map, ["student_name", "full_name", "name"])
            full_name = sanitize_full_name(full_name)
            email = get_value(row, index_map, ["email", "email_id", "mail"])

            # Primary source as requested: Skill Tags column
            skill_text = get_value(
                row,
                index_map,
                [
                    "skill_tags",
                    "primary_skill_category",
                    "skill_category",
                    "skills",
                ],
            )

            resume_type = get_value(
                row,
                index_map,
                ["resume_type", "resume_category", "job_category", "category"],
            )

            if not email:
                skipped += 1
                continue

            if not full_name:
                full_name = email.split("@")[0]

            parsed_roles, parsed_skills = parse_roles_and_skills(skill_text)

            # fallback: if no preferred role but skill text has simple category value
            if not parsed_roles and skill_text:
                fallback_role = clean_skill_token(skill_text.split("|")[0])
                parsed_roles = [fallback_role] if fallback_role else []

            try:
                action = upsert_student(
                    conn,
                    full_name=full_name,
                    email=email,
                    preferred_roles=parsed_roles,
                    skills=parsed_skills,
                    job_category=resume_type or None,
                    dry_run=args.dry_run,
                )
                if action == "inserted":
                    inserted += 1
                elif action == "updated":
                    updated += 1
                else:
                    skipped += 1
            except Exception as exc:
                errors += 1
                print(f"Row {row_num} failed for email '{email}': {exc}")

    print("\nImport summary")
    print(f"  Spreadsheet ID : {spreadsheet_id}")
    print(f"  Tab            : {tab_title}")
    print(f"  Processed rows : {len(rows)}")
    print(f"  Inserted       : {inserted}")
    print(f"  Updated        : {updated}")
    print(f"  Skipped        : {skipped}")
    print(f"  Errors         : {errors}")
    print(f"  Dry run        : {args.dry_run}")

    return 0 if errors == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
