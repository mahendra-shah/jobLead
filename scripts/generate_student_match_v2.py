#!/usr/bin/env python3
"""
Generate student-job matches from Google Sheets and write to a new tab.

Flow:
1) Read students from tab: "resume review"
2) Read jobs from tab: yesterday's "YYYY-MM-DD_v2" (auto)
3) Compute match score per student-job
4) Create/overwrite tab: today's "YYYY-MM-DD_student_match_v2" (auto)
5) Send summary email

Usage:
  python scripts/generate_student_match_v2.py \
    --spreadsheet-url "https://docs.google.com/spreadsheets/d/.../edit" \
    --resume-tab "resume review" \
    --email-to "navgurukul10@gmail.com"
"""

import argparse
import os
import re
import smtplib
import sys
from datetime import datetime, timedelta
from dataclasses import dataclass
from difflib import SequenceMatcher
from email.message import EmailMessage
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# Add project root for app imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from app.config import settings
from app.utils.timezone import now_ist


DEFAULT_SPREADSHEET_ID = "112GNJ2uwwoYbf7OdmjSPFgDa0m_nH-STEuVbeCpECeg"
DEFAULT_RESUME_TAB = "resume review"


def resolve_tab_names(source_tab: str, target_tab: str, run_date: str) -> Tuple[str, str]:
    if run_date:
        run_dt = datetime.strptime(run_date, "%Y-%m-%d")
    else:
        run_dt = now_ist().replace(tzinfo=None)

    yesterday = (run_dt - timedelta(days=1)).strftime("%Y-%m-%d")
    today = run_dt.strftime("%Y-%m-%d")

    final_source = source_tab or f"{yesterday}_v2"
    final_target = target_tab or f"{today}_student_match_v2"
    return final_source, final_target


def normalize_key(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", (value or "").strip().lower()).strip("_")


def parse_spreadsheet_id(spreadsheet_url_or_id: str) -> str:
    text = spreadsheet_url_or_id.strip()
    match = re.search(r"/spreadsheets/d/([a-zA-Z0-9-_]+)", text)
    if match:
        return match.group(1)
    return text


def split_items(text: str) -> List[str]:
    if not text:
        return []
    parts = re.split(r"[,/;|\n]+", text)
    return [part.strip().lower() for part in parts if part and part.strip()]


def parse_skill_tags(text: str) -> List[str]:
    if not text:
        return []
    cleaned = text.replace("\n", " ")
    if ":" in cleaned:
        cleaned = cleaned.split(":", 1)[1]
    raw = re.split(r"[,;|]+", cleaned)
    skills: List[str] = []
    for item in raw:
        token = re.sub(r"\(.*?\)", "", item).strip().lower()
        if token:
            skills.append(token)
    return skills


@dataclass
class Student:
    name: str
    email: str
    preferred_roles: List[str]
    skills: List[str]
    preferred_locations: List[str]


@dataclass
class Job:
    date: str
    company: str
    title: str
    location: str
    work_type: str
    job_type: str
    skills: List[str]
    apply_link: str
    channel_name: str
    full_message_text: str


class StudentJobMatcher:
    def __init__(self, spreadsheet_id: str, credentials_file: Optional[str] = None):
        self.spreadsheet_id = spreadsheet_id
        candidate_path = credentials_file or settings.GOOGLE_CLIENT_SECRET or ""
        if candidate_path:
            credentials_path = Path(candidate_path)
        else:
            env_path = Path("/tmp/non-existent")
            if "GOOGLE_APPLICATION_CREDENTIALS" in os.environ:
                env_path = Path(os.environ["GOOGLE_APPLICATION_CREDENTIALS"])
            default_path = Path(__file__).parent.parent / "credentials.json"
            credentials_path = env_path if env_path.exists() else default_path

        if not credentials_path.exists():
            raise FileNotFoundError(
                "Google credentials file not found. Provide --credentials-file or set GOOGLE_APPLICATION_CREDENTIALS."
            )

        scopes = ["https://www.googleapis.com/auth/spreadsheets"]

        credentials = service_account.Credentials.from_service_account_file(
            str(credentials_path),
            scopes=scopes,
        )
        self.service = build("sheets", "v4", credentials=credentials)
        self.sheets = self.service.spreadsheets()

    def read_tab(self, tab_name: str) -> Tuple[List[str], List[List[str]]]:
        result = self.sheets.values().get(
            spreadsheetId=self.spreadsheet_id,
            range=f"'{tab_name}'!A1:ZZ",
        ).execute()
        values = result.get("values", [])
        if not values:
            return [], []
        header = values[0]
        rows = values[1:]
        return header, rows

    def ensure_target_tab(self, tab_name: str):
        metadata = self.sheets.get(spreadsheetId=self.spreadsheet_id).execute()
        sheets = metadata.get("sheets", [])
        existing = {s["properties"]["title"]: s["properties"]["sheetId"] for s in sheets}

        if tab_name not in existing:
            self.sheets.batchUpdate(
                spreadsheetId=self.spreadsheet_id,
                body={
                    "requests": [
                        {
                            "addSheet": {
                                "properties": {
                                    "title": tab_name,
                                    "gridProperties": {"rowCount": 3000, "columnCount": 25},
                                }
                            }
                        }
                    ]
                },
            ).execute()
        else:
            self.sheets.values().clear(
                spreadsheetId=self.spreadsheet_id,
                range=f"'{tab_name}'!A1:ZZ",
            ).execute()

    def write_tab(self, tab_name: str, rows: List[List[str]]):
        body = {"values": rows}
        self.sheets.values().update(
            spreadsheetId=self.spreadsheet_id,
            range=f"'{tab_name}'!A1",
            valueInputOption="RAW",
            body=body,
        ).execute()

    def get_sheet_gid(self, tab_name: str) -> Optional[int]:
        """Return the numeric GID of a sheet tab, or None if not found."""
        metadata = self.sheets.get(spreadsheetId=self.spreadsheet_id).execute()
        for s in metadata.get("sheets", []):
            if s["properties"]["title"] == tab_name:
                return s["properties"]["sheetId"]
        return None


def get_value(row: List[str], index_map: Dict[str, int], keys: List[str]) -> str:
    for key in keys:
        idx = index_map.get(key)
        if idx is not None and idx < len(row):
            return (row[idx] or "").strip()
    return ""


def build_students(header: List[str], rows: List[List[str]]) -> List[Student]:
    index_map = {normalize_key(col): idx for idx, col in enumerate(header)}
    students: List[Student] = []

    for row in rows:
        name = get_value(row, index_map, ["student_name", "name", "full_name"])
        email = get_value(row, index_map, ["email", "email_id", "mail"])
        preferred_role_text = get_value(
            row,
            index_map,
            [
                "preferred_job_role",
                "preferred_role",
                "job_role",
                "role",
                "primary_skill_category",
                "resume_type",
            ],
        )
        skills_text = get_value(
            row,
            index_map,
            ["technical_skills", "skills", "skill", "tech_skills", "skill_tags"],
        )
        location_text = get_value(row, index_map, ["preferred_location", "location", "location_preference"])

        if not (name or email):
            continue

        parsed_skills = parse_skill_tags(skills_text)

        students.append(
            Student(
                name=name or "Unknown",
                email=email,
                preferred_roles=split_items(preferred_role_text),
                skills=parsed_skills if parsed_skills else split_items(skills_text),
                preferred_locations=split_items(location_text),
            )
        )
    return students


def build_jobs(header: List[str], rows: List[List[str]]) -> List[Job]:
    index_map = {normalize_key(col): idx for idx, col in enumerate(header)}
    jobs: List[Job] = []

    for row in rows:
        title = get_value(row, index_map, ["job_title", "title"])
        if not title:
            continue
        skills_text = get_value(row, index_map, ["skills", "skills_required"])
        jobs.append(
            Job(
                date=get_value(row, index_map, ["date"]),
                company=get_value(row, index_map, ["company", "company_name"]),
                title=title,
                location=get_value(row, index_map, ["location"]),
                work_type=get_value(row, index_map, ["work_type"]),
                job_type=get_value(row, index_map, ["job_type"]),
                skills=split_items(skills_text),
                apply_link=get_value(row, index_map, ["apply_link", "source_url"]),
                channel_name=get_value(row, index_map, ["channel_name", "source_channel"]),
                full_message_text=get_value(row, index_map, ["full_message_text", "message", "raw_text"]),
            )
        )
    return jobs


def similarity(a: str, b: str) -> float:
    if not a or not b:
        return 0.0
    return SequenceMatcher(None, a.lower(), b.lower()).ratio()


def get_skill_match_details(student_skills: List[str], job_skills: List[str]) -> Tuple[List[str], int, int]:
    """Return (matched_labels, exact_count, fuzzy_count)."""
    if not student_skills or not job_skills:
        return [], 0, 0

    student_unique = sorted(set(skill.strip().lower() for skill in student_skills if skill.strip()))
    job_unique = sorted(set(skill.strip().lower() for skill in job_skills if skill.strip()))

    exact_matches = sorted(set(student_unique) & set(job_unique))

    fuzzy_pairs: List[str] = []
    used_student = set(exact_matches)
    used_job = set(exact_matches)

    for student_skill in student_unique:
        if student_skill in used_student:
            continue
        for job_skill in job_unique:
            if job_skill in used_job:
                continue

            # Accept strong fuzzy/substring matches (e.g. "node" vs "node.js")
            sim = similarity(student_skill, job_skill)
            if sim >= 0.82 or student_skill in job_skill or job_skill in student_skill:
                fuzzy_pairs.append(f"{student_skill}~{job_skill}")
                used_student.add(student_skill)
                used_job.add(job_skill)
                break

    labels = exact_matches + fuzzy_pairs
    return labels, len(exact_matches), len(fuzzy_pairs)


def score_match(student: Student, job: Job) -> Tuple[int, List[str]]:
    score = 0
    reasons: List[str] = []

    # Skills overlap (higher priority than role similarity)
    matched_skills, exact_count, fuzzy_count = get_skill_match_details(student.skills, job.skills)
    if exact_count or fuzzy_count:
        # Skills carry strongest weight now
        skill_score = min(60, (exact_count * 15) + (fuzzy_count * 8))
        score += skill_score
        reasons.append(f"skills:{skill_score} ({', '.join(matched_skills)})")

    # Preferred role vs title
    role_score = 0
    if student.preferred_roles:
        best = max(similarity(role, job.title) for role in student.preferred_roles)
        role_score = int(best * 30)

        # Avoid high score from role-only matches when skills don't align
        if student.skills and job.skills and not (exact_count or fuzzy_count):
            role_score = min(role_score, 12)

        if role_score > 0:
            score += role_score
            reasons.append(f"role:{role_score}")
    elif student.skills:
        best_skill_title = max(similarity(skill, job.title) for skill in student.skills)
        fallback_role_score = int(best_skill_title * 20)
        if fallback_role_score > 0:
            score += fallback_role_score
            reasons.append(f"skill_title:{fallback_role_score}")

    # Location preference
    if student.preferred_locations and job.location:
        job_loc = job.location.lower()
        if any(loc in job_loc for loc in student.preferred_locations):
            score += 15
            reasons.append("location:15")

    # Small boost if fresher/internship intent visible in job type
    if student.preferred_roles and job.job_type:
        jt = job.job_type.lower()
        if "intern" in jt and any("intern" in role for role in student.preferred_roles):
            score += 5
            reasons.append("job_type:5")

    return min(score, 100), reasons


def generate_match_rows(
    students: List[Student],
    jobs: List[Job],
    min_score: int,
    top_k: int,
) -> Tuple[List[List[str]], Dict[str, Tuple[str, int]]]:
    """Returns (output_rows, {student_email: (student_name, match_count)})."""
    header = [
        "Student Name",
        "Student Email",
        "Preferred Roles",
        "Student Skills",
        "Preferred Locations",
        "Job Title",
        "Company",
        "Job Location",
        "Work Type",
        "Job Type",
        "Job Skills",
        "Apply Link",
        "Channel Name",
        "Job Date",
        "Match Score",
        "Match Reason",
        "Full Message Text",
    ]
    output = [header]
    student_match_counts: Dict[str, Tuple[str, int]] = {}

    for student in students:
        scored: List[Tuple[int, List[str], Job]] = []
        for job in jobs:
            score, reasons = score_match(student, job)
            if score >= min_score:
                scored.append((score, reasons, job))

        scored.sort(key=lambda x: x[0], reverse=True)
        top_matches = scored[:top_k]

        if top_matches and student.email:
            student_match_counts[student.email] = (student.name, len(top_matches))

        for score, reasons, job in top_matches:
            output.append(
                [
                    student.name,
                    student.email,
                    ", ".join(student.preferred_roles),
                    ", ".join(student.skills),
                    ", ".join(student.preferred_locations),
                    job.title,
                    job.company,
                    job.location,
                    job.work_type,
                    job.job_type,
                    ", ".join(job.skills),
                    job.apply_link,
                    job.channel_name,
                    job.date,
                    str(score),
                    " | ".join(reasons),
                    job.full_message_text,
                ]
            )

    return output, student_match_counts


def send_summary_email(
    to_email: str,
    subject: str,
    body: str,
    from_email: Optional[str] = None,
):
    smtp_user = settings.SMTP_USER
    smtp_password = settings.SMTP_PASSWORD

    if not smtp_user or not smtp_password:
        raise RuntimeError("SMTP_USER/SMTP_PASSWORD not configured in .env")

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = from_email or smtp_user
    msg["To"] = to_email
    msg.set_content(body)

    with smtplib.SMTP(settings.SMTP_HOST, settings.SMTP_PORT) as server:
        server.starttls()
        server.login(smtp_user, smtp_password)
        server.send_message(msg)


def send_student_notification_email(
    student_name: str,
    student_email: str,
    match_count: int,
    sheet_url: str,
    tab_name: str,
    from_email: Optional[str] = None,
):
    """Send a rich HTML job-match notification to an individual student."""
    smtp_user = settings.SMTP_USER
    smtp_password = settings.SMTP_PASSWORD

    if not smtp_user or not smtp_password:
        raise RuntimeError("SMTP_USER/SMTP_PASSWORD not configured in .env")

    first_name = student_name.split()[0].capitalize() if student_name else "Student"
    subject = f"\U0001f3af {match_count} job(s) matched your profile!"

    plain_text = (
        f"Hi {first_name}!\n\n"
        f"Great news! We found {match_count} job(s) that match your profile.\n\n"
        f"\U0001f4ca View your matching jobs here:\n"
        f"{sheet_url}\n\n"
        f"Sheet: {tab_name}\n\n"
        f"The jobs are sorted by match score, so the best matches are at the top.\n"
        f"Check the 'Apply Link' column to apply directly.\n\n"
        f"Good luck with your job search!\n\n"
        f"Best regards,\n"
        f"NavGurukul Job Matching System"
    )

    html_content = f"""<!DOCTYPE html>
<html>
<body style="font-family:Arial,sans-serif;max-width:600px;margin:0 auto;padding:20px;color:#333;background:#f4f4f4;">
  <div style="background:linear-gradient(135deg,#4f46e5 0%,#7c3aed 100%);padding:32px 30px;border-radius:12px 12px 0 0;text-align:center;">
    <h1 style="color:white;margin:0;font-size:26px;letter-spacing:-0.5px;">\U0001f3af Job Matches Found!</h1>
    <p style="color:rgba(255,255,255,0.85);margin:8px 0 0;font-size:14px;">NavGurukul Placement Portal</p>
  </div>

  <div style="background:#ffffff;padding:32px 30px;border-radius:0 0 12px 12px;border:1px solid #e0e0e0;">
    <p style="font-size:18px;margin:0 0 8px;">Hi <strong>{first_name}</strong>! \U0001f44b</p>
    <p style="font-size:15px;color:#555;margin:0 0 24px;">Great news! We found matching jobs for you.</p>

    <div style="background:#f0f0ff;border:1px solid #c7d2fe;border-radius:10px;padding:20px;text-align:center;margin-bottom:24px;">
      <p style="font-size:13px;color:#6366f1;font-weight:600;text-transform:uppercase;letter-spacing:1px;margin:0 0 6px;">\U0001f4ca Matched Jobs</p>
      <p style="font-size:42px;font-weight:700;color:#4f46e5;margin:0;">{match_count}</p>
      <p style="font-size:13px;color:#888;margin:4px 0 0;">jobs matched your profile</p>
    </div>

    <div style="text-align:center;margin-bottom:28px;">
      <a href="{sheet_url}"
         style="background:#4f46e5;color:white;padding:14px 36px;border-radius:8px;text-decoration:none;font-size:16px;font-weight:600;display:inline-block;">
        View My Job Matches &rarr;
      </a>
      <p style="font-size:12px;color:#aaa;margin:10px 0 0;">Sheet tab: <em>{tab_name}</em></p>
    </div>

    <div style="background:#fafafa;border-left:4px solid #4f46e5;padding:14px 16px;border-radius:0 8px 8px 0;margin-bottom:20px;">
      <p style="margin:0 0 6px;font-size:14px;color:#444;">\u2705 Jobs are sorted by <strong>match score</strong> &mdash; best matches are at the top.</p>
      <p style="margin:0;font-size:14px;color:#444;">\U0001f4cc Use the <strong>Apply Link</strong> column to apply directly to each job.</p>
    </div>

    <hr style="border:none;border-top:1px solid #eee;margin:24px 0;">
    <p style="color:#888;font-size:12px;text-align:center;margin:0;">
      Good luck with your job search! \U0001f680<br>
      <strong style="color:#555;">NavGurukul Job Matching System</strong>
    </p>
  </div>
</body>
</html>"""

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = from_email or smtp_user
    msg["To"] = student_email
    msg.set_content(plain_text)
    msg.add_alternative(html_content, subtype="html")

    with smtplib.SMTP(settings.SMTP_HOST, settings.SMTP_PORT) as server:
        server.starttls()
        server.login(smtp_user, smtp_password)
        server.send_message(msg)


def main():
    parser = argparse.ArgumentParser(description="Generate student match v2 from Google Sheets")
    parser.add_argument("--spreadsheet-url", default="", help="Google Sheet URL or ID")
    parser.add_argument("--resume-tab", default=DEFAULT_RESUME_TAB)
    parser.add_argument("--source-tab", default="", help="Override source tab (default: yesterday_YYYY-MM-DD_v2)")
    parser.add_argument("--target-tab", default="", help="Override target tab (default: today_YYYY-MM-DD_student_match_v2)")
    parser.add_argument("--run-date", default="", help="Run date in YYYY-MM-DD (IST), used for auto tab naming")
    parser.add_argument("--min-score", type=int, default=35)
    parser.add_argument("--top-k", type=int, default=5)
    parser.add_argument("--credentials-file", default="", help="Path to Google service-account credentials JSON")
    parser.add_argument("--email-to", default="navgurukul10@gmail.com")
    parser.add_argument("--email-from", default="navgurukul10@gmail.com")
    parser.add_argument("--skip-email", action="store_true")
    args = parser.parse_args()

    sheet_source = args.spreadsheet_url or settings.SHEET_ID or DEFAULT_SPREADSHEET_ID
    spreadsheet_id = parse_spreadsheet_id(sheet_source)
    sheet_url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/edit"
    source_tab, target_tab = resolve_tab_names(args.source_tab, args.target_tab, args.run_date)

    print("=" * 80)
    print("📊 STUDENT MATCH V2 GENERATOR")
    print("=" * 80)
    print(f"Spreadsheet: {sheet_url}")
    print(f"Resume tab: {args.resume_tab}")
    print(f"Source tab: {source_tab}")
    print(f"Target tab: {target_tab}")

    try:
        matcher = StudentJobMatcher(
            spreadsheet_id,
            credentials_file=args.credentials_file or None,
        )

        resume_header, resume_rows = matcher.read_tab(args.resume_tab)
        source_header, source_rows = matcher.read_tab(source_tab)

        if not resume_header:
            raise RuntimeError(f"Tab '{args.resume_tab}' is empty or not found")
        if not source_header:
            raise RuntimeError(f"Tab '{source_tab}' is empty or not found")

        students = build_students(resume_header, resume_rows)
        jobs = build_jobs(source_header, source_rows)

        print(f"Students found: {len(students)}")
        print(f"Jobs found: {len(jobs)}")

        match_rows, student_match_counts = generate_match_rows(
            students=students,
            jobs=jobs,
            min_score=args.min_score,
            top_k=args.top_k,
        )

        matcher.ensure_target_tab(target_tab)
        matcher.write_tab(target_tab, match_rows)

        total_matches = max(0, len(match_rows) - 1)
        print(f"✅ Wrote {total_matches} matched rows to tab '{target_tab}'")

        # Build sheet URL with direct link to the target tab
        gid = matcher.get_sheet_gid(target_tab)
        sheet_tab_url = f"{sheet_url}#gid={gid}" if gid is not None else sheet_url

        if not args.skip_email:
            # --- Individual student notification emails ---
            sent_count = 0
            failed_count = 0
            print(f"\n📧 Sending individual emails to {len(student_match_counts)} students...")
            for student_email, (student_name, match_count) in student_match_counts.items():
                try:
                    send_student_notification_email(
                        student_name=student_name,
                        student_email=student_email,
                        match_count=match_count,
                        sheet_url=sheet_tab_url,
                        tab_name=target_tab,
                        from_email=args.email_from,
                    )
                    print(f"   ✅ {student_name} <{student_email}> — {match_count} match(es)")
                    sent_count += 1
                except Exception as exc:
                    print(f"   ❌ Failed to email {student_email}: {exc}")
                    failed_count += 1

            print(f"\n📬 Student emails: {sent_count} sent, {failed_count} failed")

            # --- Admin summary email ---
            subject = f"Student Match V2 completed - {target_tab}"
            body = (
                f"Student matching completed successfully.\n\n"
                f"Spreadsheet: {sheet_tab_url}\n"
                f"Source tab: {source_tab}\n"
                f"Resume tab: {args.resume_tab}\n"
                f"Target tab: {target_tab}\n"
                f"Students processed: {len(students)}\n"
                f"Jobs processed: {len(jobs)}\n"
                f"Matches written: {total_matches}\n"
                f"Students notified: {sent_count}\n"
                f"Min score: {args.min_score}\n"
                f"Top K per student: {args.top_k}\n"
            )
            send_summary_email(
                to_email=args.email_to,
                subject=subject,
                body=body,
                from_email=args.email_from,
            )
            print(f"📧 Summary email sent to {args.email_to}")

        print("✅ Done")

    except HttpError as e:
        print(f"❌ Google Sheets API error: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
