#!/usr/bin/env python3
"""Send matched jobs to students via email and track unique shares.

Rules implemented:
- Match by student skills + preferred job role
- Send jobs only if not already shared with that student
- Increment jobs.shared_count for each successful unique student-job share
- Track delivered student IDs in jobs.students_shown_to
- Send email using SMTP settings from environment/config

Usage:
  python3 scripts/send_matched_jobs_emails.py
  python3 scripts/send_matched_jobs_emails.py --dry-run --student-limit 10
  python3 scripts/send_matched_jobs_emails.py --max-jobs-per-student 5 --min-score 2
    python3 scripts/send_matched_jobs_emails.py --job-date 2026-03-17
"""

from __future__ import annotations

import argparse
import re
import smtplib
import sys
from datetime import datetime, timedelta
from dataclasses import dataclass
from email.message import EmailMessage
from pathlib import Path
from typing import Dict, List, Sequence, Set, Tuple

from sqlalchemy import create_engine, text

sys.path.insert(0, str(Path(__file__).parent.parent))
from app.config import settings  # noqa: E402
from app.utils.timezone import now_ist  # noqa: E402


@dataclass
class StudentRow:
    id: str
    full_name: str
    email: str
    skills: List[str]
    preferred_roles: List[str]
    preferred_job_types: List[str]
    preferred_job_categories: List[str]


@dataclass
class JobRow:
    id: str
    title: str
    company_name: str
    location: str
    description: str
    source_url: str
    skills_required: List[str]
    job_type: str
    employment_type: str
    students_shown_to: List[str]


def normalize_db_url() -> str:
    db_url = str(settings.DATABASE_URL)
    if db_url.startswith("postgresql+asyncpg://"):
        db_url = db_url.replace("postgresql+asyncpg://", "postgresql://", 1)
    return db_url


def normalize_token(value: str) -> str:
    if not value:
        return ""
    cleaned = re.sub(r"[^a-z0-9+.#]+", " ", value.lower()).strip()
    return re.sub(r"\s+", " ", cleaned)


def sanitize_email(value: str) -> str:
    email = (value or "").strip().lower()
    email = re.sub(r"\s+", "", email)
    email = email.rstrip("|,;")
    return email


def is_valid_email(value: str) -> bool:
    if not value:
        return False
    return re.match(r"^[a-z0-9._%+\-]+@[a-z0-9.\-]+\.[a-z]{2,}$", value) is not None


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


def token_match(needle: str, haystack_text: str, haystack_tokens: Set[str]) -> bool:
    if not needle:
        return False
    if needle in haystack_tokens:
        return True
    if needle in haystack_text:
        return True

    for token in haystack_tokens:
        if needle in token or token in needle:
            return True
    return False


def fetch_students(conn, limit: int) -> List[StudentRow]:
    limit_clause = "LIMIT :limit" if limit > 0 else ""
    result = conn.execute(
        text(
            f"""
            SELECT
                id::text AS id,
                COALESCE(full_name, '') AS full_name,
                lower(trim(email)) AS email,
                COALESCE(skills, '[]'::jsonb) AS skills,
                COALESCE(technical_skills, '[]'::jsonb) AS technical_skills,
                COALESCE(preferred_job_role, '[]'::jsonb) AS preferred_job_role,
                COALESCE(preference, '{{}}'::jsonb) AS preference,
                COALESCE(job_category, '[]'::jsonb) AS job_category
            FROM students
            WHERE email IS NOT NULL
              AND btrim(email) <> ''
              AND COALESCE(email_notifications, TRUE) = TRUE
              AND COALESCE(status, 'active') = 'active'
            ORDER BY created_at DESC
            {limit_clause}
            """
        ),
        {"limit": limit} if limit > 0 else {},
    )

    students: List[StudentRow] = []
    for row in result:
        primary_skills = normalize_list(row.skills or [])
        technical_skills = normalize_list(row.technical_skills or [])
        merged_skills = normalize_list(primary_skills + technical_skills)

        preferred_roles = normalize_list(row.preferred_job_role or [])
        preference_obj = row.preference if isinstance(row.preference, dict) else {}
        preferred_job_types = normalize_maybe_list(preference_obj.get("job_type"))
        preferred_job_categories = normalize_list(
            normalize_maybe_list(row.job_category)
            + normalize_maybe_list(preference_obj.get("job_category"))
        )

        cleaned_email = sanitize_email(row.email or "")
        if not is_valid_email(cleaned_email):
            continue

        students.append(
            StudentRow(
                id=row.id,
                full_name=(row.full_name or "Student").strip() or "Student",
                email=cleaned_email,
                skills=merged_skills,
                preferred_roles=preferred_roles,
                preferred_job_types=preferred_job_types,
                preferred_job_categories=preferred_job_categories,
            )
        )

    return students


def resolve_job_window(job_date_arg: str | None) -> Tuple[datetime, datetime, str]:
    if job_date_arg:
        target_date = datetime.strptime(job_date_arg, "%Y-%m-%d")
    else:
        target_date = (now_ist() - timedelta(days=1)).replace(tzinfo=None)

    start_local = target_date.replace(hour=0, minute=0, second=0, microsecond=0)
    end_local = start_local + timedelta(days=1)

    # Convert IST local day window to UTC-naive datetimes for DB filtering
    # IST is UTC+05:30, so subtract 5h30m
    start_utc = start_local - timedelta(hours=5, minutes=30)
    end_utc = end_local - timedelta(hours=5, minutes=30)
    return start_utc, end_utc, start_local.strftime("%Y-%m-%d")


def fetch_jobs(conn, start_utc: datetime, end_utc: datetime) -> List[JobRow]:
    result = conn.execute(
        text(
            """
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
                COALESCE(students_shown_to, '[]'::jsonb) AS students_shown_to
            FROM jobs
            WHERE COALESCE(is_active, TRUE) = TRUE
              AND created_at >= :start_utc
              AND created_at < :end_utc
            ORDER BY created_at DESC
            """
        ),
        {"start_utc": start_utc, "end_utc": end_utc},
    )

    jobs: List[JobRow] = []
    for row in result:
        jobs.append(
            JobRow(
                id=row.id,
                title=(row.title or "").strip(),
                company_name=(row.company_name or "").strip(),
                location=(row.location or "").strip(),
                description=(row.description or "").strip(),
                source_url=(row.source_url or "").strip(),
                skills_required=normalize_list(row.skills_required or []),
                job_type=normalize_token(row.job_type or ""),
                employment_type=normalize_token(row.employment_type or ""),
                students_shown_to=[str(x) for x in (row.students_shown_to or []) if str(x).strip()],
            )
        )

    return jobs


def match_student_jobs(
    student: StudentRow,
    jobs: List[JobRow],
    max_jobs_per_student: int,
    min_score: int,
) -> List[Tuple[JobRow, int, List[str], List[str], List[str], List[str]]]:
    matches: List[Tuple[JobRow, int, List[str], List[str], List[str], List[str]]] = []

    for job in jobs:
        if student.id in set(job.students_shown_to):
            continue

        job_text = normalize_token(" ".join([job.title, job.company_name, job.description, job.job_type, job.employment_type]))
        job_tokens = set(normalize_list(job.skills_required + [job.title, job.company_name, job.job_type, job.employment_type]))

        matched_skills = [s for s in student.skills if token_match(s, job_text, job_tokens)]
        matched_roles = [r for r in student.preferred_roles if token_match(r, job_text, job_tokens)]
        matched_job_types = [jt for jt in student.preferred_job_types if token_match(jt, job_text, job_tokens)]
        matched_job_categories = [jc for jc in student.preferred_job_categories if token_match(jc, job_text, job_tokens)]

        has_skill_or_role_input = bool(student.skills) or bool(student.preferred_roles)
        has_skill_or_role_match = bool(matched_skills) or bool(matched_roles)
        if not has_skill_or_role_input or not has_skill_or_role_match:
            continue

        score = (
            (len(set(matched_skills)) * 3)
            + (len(set(matched_roles)) * 2)
            + (len(set(matched_job_types)) * 2)
            + (len(set(matched_job_categories)) * 2)
        )
        if score < min_score:
            continue

        matches.append(
            (
                job,
                score,
                sorted(set(matched_skills)),
                sorted(set(matched_roles)),
                sorted(set(matched_job_types)),
                sorted(set(matched_job_categories)),
            )
        )

    matches.sort(key=lambda item: item[1], reverse=True)
    return matches[:max_jobs_per_student]


def build_email(
    student: StudentRow,
    matches: List[Tuple[JobRow, int, List[str], List[str], List[str], List[str]]],
) -> Tuple[str, str]:
    lines = [
        f"Hi {student.full_name},",
        "",
        "We found job opportunities matching your skills and preferred role:",
        "",
    ]

    html_items: List[str] = []

    for i, (job, score, skill_hits, role_hits, job_type_hits, job_category_hits) in enumerate(matches, start=1):
        company = job.company_name or "Unknown Company"
        location = job.location or "Not specified"
        link = job.source_url or ""

        lines.append(f"{i}. {job.title} | {company}")
        lines.append(f"   Location: {location}")
        lines.append(f"   Match score: {score}")
        if skill_hits:
            lines.append(f"   Matched skills: {', '.join(skill_hits)}")
        if role_hits:
            lines.append(f"   Matched roles: {', '.join(role_hits)}")
        if job_type_hits:
            lines.append(f"   Matched job type: {', '.join(job_type_hits)}")
        if job_category_hits:
            lines.append(f"   Matched job category: {', '.join(job_category_hits)}")
        lines.append(f"   Apply: {link if link else 'Link not available'}")
        lines.append("")

        apply_html = f'<a href="{link}">{link}</a>' if link else 'Link not available'

        html_items.append(
            "<li>"
            f"<strong>{job.title}</strong> | {company}<br>"
            f"Location: {location}<br>"
            f"Match score: {score}<br>"
            f"Matched skills: {', '.join(skill_hits) if skill_hits else 'N/A'}<br>"
            f"Matched roles: {', '.join(role_hits) if role_hits else 'N/A'}<br>"
            f"Matched job type: {', '.join(job_type_hits) if job_type_hits else 'N/A'}<br>"
            f"Matched job category: {', '.join(job_category_hits) if job_category_hits else 'N/A'}<br>"
            f"Apply: {apply_html}"
            "</li>"
        )

    lines.extend(
        [
            "Best regards,",
            "Placement Team",
        ]
    )

    text_body = "\n".join(lines)
    html_body = (
        f"<p>Hi {student.full_name},</p>"
        "<p>We found job opportunities matching your skills and preferred role:</p>"
        f"<ol>{''.join(html_items)}</ol>"
        "<p>Best regards,<br>Placement Team</p>"
    )

    return text_body, html_body


def send_email(to_email: str, subject: str, text_body: str, html_body: str) -> None:
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = settings.EMAIL_FROM
    msg["To"] = to_email
    msg.set_content(text_body)
    msg.add_alternative(html_body, subtype="html")

    with smtplib.SMTP(settings.SMTP_HOST, settings.SMTP_PORT, timeout=30) as server:
        server.starttls()
        if settings.SMTP_USER and settings.SMTP_PASSWORD:
            server.login(settings.SMTP_USER, settings.SMTP_PASSWORD)
        server.send_message(msg)


def mark_jobs_shared(conn, student_id: str, job_ids: List[str]) -> int:
    if not job_ids:
        return 0

    updated = 0
    for job_id in job_ids:
        result = conn.execute(
            text(
                """
                UPDATE jobs
                SET
                    students_shown_to = COALESCE(students_shown_to, '[]'::jsonb) || to_jsonb(CAST(:student_id AS text)),
                    shared_count = COALESCE(shared_count, 0) + 1,
                    updated_at = NOW()
                WHERE id = CAST(:job_id AS uuid)
                  AND NOT (COALESCE(students_shown_to, '[]'::jsonb) ? :student_id)
                """
            ),
            {"student_id": student_id, "job_id": job_id},
        )
        updated += result.rowcount or 0
    return updated


def validate_email_settings() -> None:
    missing: List[str] = []
    if not settings.SMTP_HOST:
        missing.append("SMTP_HOST")
    if not settings.SMTP_PORT:
        missing.append("SMTP_PORT")
    if not settings.EMAIL_FROM:
        missing.append("EMAIL_FROM")

    if missing:
        raise RuntimeError(f"Missing email config in env: {', '.join(missing)}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Send matched jobs email to students and track unique shares")
    parser.add_argument("--student-limit", type=int, default=0, help="Process only first N students (0 = all)")
    parser.add_argument("--max-jobs-per-student", type=int, default=5, help="Max jobs to send per student")
    parser.add_argument("--min-score", type=int, default=2, help="Minimum match score")
    parser.add_argument(
        "--job-date",
        default=None,
        help="Job date in IST (YYYY-MM-DD). Default: yesterday in IST",
    )
    parser.add_argument("--dry-run", action="store_true", help="Compute matches only; no emails, no DB updates")
    args = parser.parse_args()

    validate_email_settings()
    db_url = normalize_db_url()
    engine = create_engine(db_url)

    summary: Dict[str, int] = {
        "students_processed": 0,
        "students_with_matches": 0,
        "emails_sent": 0,
        "jobs_shared": 0,
        "students_skipped_no_match": 0,
        "errors": 0,
    }

    with engine.begin() as conn:
        students = fetch_students(conn, args.student_limit)
        start_utc, end_utc, job_date_label = resolve_job_window(args.job_date)
        jobs = fetch_jobs(conn, start_utc, end_utc)

        print(f"Loaded students: {len(students)}")
        print(f"Loaded active jobs for date {job_date_label} (IST): {len(jobs)}")

        for student in students:
            summary["students_processed"] += 1
            if not student.email:
                summary["students_skipped_no_match"] += 1
                continue

            matches = match_student_jobs(
                student=student,
                jobs=jobs,
                max_jobs_per_student=args.max_jobs_per_student,
                min_score=args.min_score,
            )

            if not matches:
                summary["students_skipped_no_match"] += 1
                continue

            summary["students_with_matches"] += 1
            text_body, html_body = build_email(student, matches)
            subject = f"{len(matches)} job match(es) for your profile"
            matched_job_ids = [item[0].id for item in matches]

            try:
                if args.dry_run:
                    print(f"[DRY RUN] Would send {len(matches)} jobs to {student.email}")
                else:
                    send_email(student.email, subject, text_body, html_body)
                    updated_rows = mark_jobs_shared(conn, student.id, matched_job_ids)
                    summary["emails_sent"] += 1
                    summary["jobs_shared"] += updated_rows
                    print(f"Sent {len(matches)} job(s) to {student.email} | shared_count updates: {updated_rows}")
            except Exception as exc:
                summary["errors"] += 1
                print(f"Failed for {student.email}: {exc}")

    print("\nRun summary")
    print(f"  Students processed      : {summary['students_processed']}")
    print(f"  Students with matches   : {summary['students_with_matches']}")
    print(f"  Emails sent             : {summary['emails_sent']}")
    print(f"  Jobs newly shared       : {summary['jobs_shared']}")
    print(f"  Students skipped        : {summary['students_skipped_no_match']}")
    print(f"  Errors                  : {summary['errors']}")
    print(f"  Dry run                 : {args.dry_run}")

    return 0 if summary["errors"] == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
