#!/usr/bin/env python3
"""Match ML-verified job-board jobs to students by skills; write markdown + JSON report.

Loads from Postgres by default (source=job_board, IST date window). Pass --jobs for a JSON file."""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

BAD_URL = re.compile(r"trainings\.internshala\.com|placement-guarantee-course|/jobs/jobs-in-.*-loc")
BAD_TITLE = re.compile(
    r"\(\d+[kK]|\d+\.?\d*k\s*\)|jobs\)\s*$|jobs in |^gurugram|^noida|^ahmedabad|^view all|"
    r"^work from home (jobs|customer service|web development|sales|finance|telecalling)",
    re.I,
)

STUDENTS: dict[str, dict] = {
    "Tanuja Bisht": {
        "skills_summary": "Data entry, formatting, filtering, sorting; teamwork, leadership, time management, problem-solving",
        "title_hit": re.compile(
            r"data\s*entr|data\s*analyst|analyst(?!.*derivatives)|customer\s*support|bpo|kpo|"
            r"collection|telecaller|back\s*office|documentation|operations\s*executive|process|"
            r"quality\s*assurance|qa\s*data|excel|typist|clerk|online\s*data\s*analyst",
            re.I,
        ),
        "text_boost": re.compile(r"data entry|excel|spreadsheet|typing|records|documentation", re.I),
        "title_avoid": re.compile(
            r"developer|full.stack|software|finance\s*derivatives|stock\s*market",
            re.I,
        ),
    },
    "Gayatri Sahu": {
        "skills_summary": "Google Workspace, CRM/Salesforce, Canva; communication, teamwork, leadership, time management",
        "title_hit": re.compile(
            r"business\s*development|sales|crm|customer\s*support|coordination|executive|"
            r"graphic\s*design|canva|marketing|content|client",
            re.I,
        ),
        "text_boost": re.compile(r"salesforce|crm|google|workspace|canva|customer", re.I),
        "title_avoid": re.compile(r"c\+\+|embedded|kernel|cybersecurity|full.stack\s*ai|nlp|iot", re.I),
    },
    "Yogyata Dangwal": {
        "skills_summary": "GA4, GSC, Google/Meta Ads, SEO/SEM/SMM, Canva, Google Workspace",
        "title_hit": re.compile(
            r"digital\s*marketing|seo|sem|social\s*media|content|marketing|growth|ads|"
            r"performance|brand|events?\s+and\s+marketing|social\s*media\s*manager|internet\s*ads",
            re.I,
        ),
        "text_boost": re.compile(r"google ads|meta|facebook|ga4|analytics|seo|sem|ppc", re.I),
        "title_avoid": re.compile(r"full.stack|cybersecurity|iot|nlp|machine translation", re.I),
    },
    "Mamta Udayan": {
        "skills_summary": "Adobe, Canva, CapCut, Premiere, HTML/CSS, social media, video, design",
        "title_hit": re.compile(
            r"graphic|design|video|visual|ui|ux|creative|videography|editing|motion|"
            r"content\s*creator|social\s*media|photoshop|illustrator|web\s*develop|html|merchandising",
            re.I,
        ),
        "text_boost": re.compile(r"adobe|photoshop|premiere|canva|instagram|youtube|portfolio", re.I),
        "title_avoid": re.compile(r"full.stack\s*ai|cybersecurity|nlp|machine translation|iot", re.I),
    },
}


def best_url(x: dict) -> str:
    u = x.get("url") or ""
    au = x.get("apply_url") or ""
    # Keep tracking params for aggregators (e.g. ambitionbox ?rid=)
    if "ambitionbox.com" in u and u.startswith("http"):
        return u.split("#")[0]
    if u.startswith("http") and "internshala.com/internship/detail" in u:
        return u.split("?")[0]
    if au.startswith("http"):
        return au.split("?")[0]
    if u.startswith("http") and au.startswith("/"):
        return "https://www.internshala.com" + au.split("?")[0]
    return u if u.startswith("http") else ""


def title_ok(t: str) -> bool:
    if not t or len(t) < 8:
        return False
    return not BAD_TITLE.search(t)


def url_ok(url: str) -> bool:
    return bool(url.startswith("http") and not BAD_URL.search(url))


def blob(x: dict) -> str:
    return " ".join([x.get("title") or "", (x.get("description") or "")[:2500]]).lower()


def score(sk: str, x: dict) -> float:
    cfg = STUDENTS[sk]
    title = x.get("title") or ""
    b = blob(x)
    s = 0.0
    if cfg["title_hit"].search(title):
        s += 40
    if cfg["text_boost"].search(b):
        s += 15
    if cfg["title_avoid"].search(title):
        s -= 35
    if re.search(r"intern|fresher|trainee|entry|graduate|part.time|work from home", b):
        s += 5
    mv = (x.get("ml_verification") or {}).get("confidence")
    if mv is None and x.get("ml_confidence") not in (None, ""):
        try:
            mv = float(x.get("ml_confidence"))
        except (TypeError, ValueError):
            mv = 0
    mv = mv if mv is not None else 0
    s += float(mv) * 3
    return s


def verdict_and_notes(sk: str, title: str, category: str) -> tuple[str, str]:
    title = title or ""
    if sk == "Tanuja Bisht":
        if re.search(r"Finance|Stock Market|Derivatives", title, re.I):
            return "stretch", "Finance/stock analytics — stronger math/market context than pure Excel data entry."
        if re.search(
            r"Urdu|Tamil|Gujarati|Marathi|Malayalam|Punjabi|Bengali|Kannada|Telugu",
            title,
            re.I,
        ):
            return "moderate", "Language-tagged role — only if she reads/writes that language fluently."
        if re.search(r"Data Analyst|Operations Executive|Customer Support", title, re.I):
            return "strong", "Excel/ops/customer — aligns with data handling + soft skills."
    if sk == "Gayatri Sahu":
        if "Manager" in title and "Trainee" not in title and "Executive" not in title:
            return "moderate", "Title says Manager — confirm years of experience required."
        if re.search(r"Business Development|Sales|Customer|Client|Graphic Design", title, re.I):
            return "strong", "Sales/CRM/client + optional Canva/design listings."
    if sk == "Yogyata Dangwal":
        if re.search(r"Ads Assessor|Internet Ads|Personalized", title, re.I):
            return "strong", "Ad evaluation — close to Google/Meta ads skill set."
        if re.search(r"Digital Marketing|SEO|SEM|Social Media|Performance Marketing|Content", title, re.I):
            return "strong", "Core digital marketing stack."
        if re.search(r"Sales and Marketing", title, re.I):
            return "moderate", "May be sales-heavy; check JD for marketing vs targets."
    if sk == "Mamta Udayan":
        if "Merchandising" in title:
            return "moderate", "Retail visual merchandising — less digital; check if she wants store/display work."
        if re.search(r"Graphic|Video|Design|Creative|Social Media|Content Creator|UI|UX", title, re.I):
            return "strong", "Creative / video / social — matches portfolio."
    return "moderate", "Review full JD for tools (Adobe, Canva, etc.) and location."


def load_clean(jobs: list[dict]) -> list[tuple[dict, str]]:
    out: list[tuple[dict, str]] = []
    for x in jobs:
        url = best_url(x)
        if not title_ok(x.get("title") or ""):
            continue
        if not url_ok(url):
            continue
        out.append((x, url))
    return out


def _sync_db_url() -> str:
    from app.config import settings

    local_db_url = os.getenv("LOCAL_DATABASE_URL")
    if local_db_url:
        sync_database_url = local_db_url
    else:
        sync_database_url = str(settings.DATABASE_URL).replace("+asyncpg", "")
        sync_database_url = sync_database_url.replace("?ssl=require", "?sslmode=require")
        sync_database_url = sync_database_url.replace("&ssl=require", "&sslmode=require")
    return sync_database_url


def load_jobs_from_postgres(*, date_str: str, source: str = "job_board") -> list[dict]:
    """Jobs for IST calendar date from Postgres, shaped like ingest/JSON rows.

    Uses reflected ``jobs`` columns only (avoids ORM/schema drift e.g. ``experience`` vs ``experience_required``).
    """
    import warnings

    from sqlalchemy import MetaData, Table, and_, create_engine, inspect as sa_inspect, or_, select
    from sqlalchemy.orm import sessionmaker

    from app.services.job_board_sheets_service import JobBoardSheetsService
    from app.utils.timezone import ist_today_utc_window

    ref_dt = datetime.strptime(date_str, "%Y-%m-%d")
    start_utc, end_utc, _ = ist_today_utc_window(ref_dt)

    engine = create_engine(_sync_db_url(), pool_pre_ping=True)
    SessionLocal = sessionmaker(bind=engine)
    db = SessionLocal()
    try:
        from sqlalchemy.exc import OperationalError

        try:
            with warnings.catch_warnings():
                warnings.filterwarnings("ignore", message=".*Did not recognize type 'vector'.*")
                job_cols = {str(c.get("name")) for c in sa_inspect(engine).get_columns("jobs")}
                jobs_table = Table("jobs", MetaData(), autoload_with=engine)

            has_source = "source" in job_cols
            has_created = "created_at" in job_cols
            has_updated = "updated_at" in job_cols
            want = ("title", "description", "source_url", "source_channel_name", "ml_confidence")
            select_cols = [jobs_table.c[n] for n in want if n in job_cols]
            if not select_cols:
                return []

            stmt = select(*select_cols)
            wc = []
            if has_source:
                wc.append(jobs_table.c.source == source)
            c_in = (
                and_(jobs_table.c.created_at >= start_utc, jobs_table.c.created_at < end_utc)
                if has_created
                else None
            )
            u_in = (
                and_(jobs_table.c.updated_at >= start_utc, jobs_table.c.updated_at < end_utc)
                if has_updated
                else None
            )
            if c_in is not None and u_in is not None:
                wc.append(or_(c_in, u_in))
            elif c_in is not None:
                wc.append(c_in)
            elif u_in is not None:
                wc.append(u_in)
            if wc:
                stmt = stmt.where(and_(*wc))

            rows = db.execute(stmt).mappings().all()
        except OperationalError as e:
            err = str(getattr(e, "orig", e))
            hint = ""
            if "does not exist" in err and "database" in err.lower():
                hint = (
                    "\n  Hint: the database name in DATABASE_URL / LOCAL_DATABASE_URL must exist on the server. "
                    "On Neon the default is often `neondb` — replace `/placement_db` with `/neondb` in the URL, "
                    "or create a database named `placement_db` in the Neon console."
                )
            print(f"Postgres error (student report): {err}{hint}", file=sys.stderr)
            raise SystemExit(1) from e
        out: list[dict] = []
        for j in rows:
            u = (j.get("source_url") or "").strip()
            domain = (j.get("source_channel_name") or "").strip()
            if not domain and u:
                domain = urlparse(u).netloc
            _, category = JobBoardSheetsService._classify_job(j.get("title") or "", domain)
            conf = None
            mc = j.get("ml_confidence")
            if mc not in (None, ""):
                try:
                    conf = float(mc)
                except (TypeError, ValueError):
                    conf = None
            row = {
                "title": j.get("title") or "",
                "description": j.get("description") or "",
                "url": u,
                "apply_url": u,
                "category": category,
                "ml_verification": {"confidence": conf} if conf is not None else {},
            }
            if mc not in (None, ""):
                row["ml_confidence"] = mc
            out.append(row)
        return out
    finally:
        db.close()
        engine.dispose()


def assign(clean: list[tuple[dict, str]], sk: str, limit: int = 10, min_score: float = 25.0) -> list[dict]:
    ranked: list[tuple[float, dict, str]] = []
    seen: set[str] = set()
    for x, url in clean:
        sc = score(sk, x)
        if sc < min_score:
            continue
        if url in seen:
            continue
        seen.add(url)
        ranked.append((sc, x, url))
    ranked.sort(key=lambda z: -z[0])
    rows = []
    for sc, x, url in ranked[:limit]:
        title = (x.get("title") or "").replace("\n", " ").strip()
        cat = x.get("category") or ""
        v, note = verdict_and_notes(sk, title, cat)
        rows.append(
            {
                "title": title,
                "category": cat,
                "url": url,
                "match_score": round(sc, 1),
                "fit": v,
                "verification_note": note,
                "ml_confidence": (x.get("ml_verification") or {}).get("confidence"),
            }
        )
    return rows


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--jobs",
        type=Path,
        default=None,
        help="Optional JSON (e.g. legacy jobs_verified). Omit to load from Postgres.",
    )
    ap.add_argument(
        "--postgres-source",
        type=str,
        default="job_board",
        help="Postgres Job.source filter when --jobs is omitted (default: job_board)",
    )
    ap.add_argument(
        "--date",
        type=str,
        default=None,
        help="Report date in IST (YYYY-MM-DD). Defaults to today's IST date.",
    )
    ap.add_argument("--out-md", type=Path, default=None)
    ap.add_argument("--out-json", type=Path, default=None)
    args = ap.parse_args()

    if not args.date:
        from app.utils.timezone import ist_today_utc_window

        _, _, args.date = ist_today_utc_window()

    data_meta = None
    if args.jobs is not None:
        jobs_path = args.jobs if args.jobs.is_absolute() else ROOT / args.jobs
        data = json.loads(jobs_path.read_text())
        jobs = data.get("jobs") or []
        data_meta = data.get("meta")
        source_label = str(jobs_path)
        jobs_name = jobs_path.name
        report_kind = "verified" if "jobs_verified" in jobs_name else "jobs_master"
    else:
        jobs = load_jobs_from_postgres(date_str=args.date, source=args.postgres_source)
        source_label = f"postgres:{args.postgres_source}"
        jobs_name = source_label
        report_kind = "postgres_job_board"

    clean = load_clean(jobs)

    if args.out_md is None:
        args.out_md = ROOT / f"app/data/student_job_verification_{args.date}.md"
    if args.out_json is None:
        args.out_json = ROOT / f"app/data/student_job_verification_{args.date}.json"

    report = {
        "meta": {
            "source": source_label,
            "exported_meta": data_meta,
            "jobs_in_file": len(jobs),
            "after_title_url_filter": len(clean),
            "date": args.date,
            "report_kind": report_kind,
        },
        "students": {},
    }

    lines = [
        f"# Student ↔ {report_kind.replace('_', ' ')} jobs ({args.date})",
        "",
        f"- Source: `{jobs_name}` — **{len(jobs)}** rows; **{len(clean)}** after removing listing/course junk titles & bad URLs.",
        "- **Fit:** strong / moderate / stretch — read JD before sharing.",
        "",
    ]

    for sk, cfg in STUDENTS.items():
        rows = assign(clean, sk, 10, 25.0)
        # Tanuja knows only Hindi + English: exclude jobs that explicitly require other languages.
        # (We only have title text in the recommendation rows, so language checks are title-based.)
        if sk == "Tanuja Bisht":
            disallowed_langs = re.compile(
                r"\b(urdu|tamil|gujarati|marathi|malayalam|punjabi|bengali|telugu|kannada|odia|assamese|marwadi|kashmiri|nepali|arabic|french|german|spanish)\b",
                re.I,
            )
            rows = [r for r in rows if not disallowed_langs.search(r.get("title") or "")]
        report["students"][sk] = {"skills": cfg["skills_summary"], "recommendations": rows}
        lines.append(f"## {sk}")
        lines.append(f"*{cfg['skills_summary']}*")
        lines.append("")
        if not rows:
            lines.append("*(No rows scored ≥25 — lower threshold or expand job source.)*")
            lines.append("")
            continue
        for i, r in enumerate(rows, 1):
            lines.append(f"{i}. **{r['title']}** — `{r['category']}` — **{r['fit']}**")
            lines.append(f"   - {r['url']}")
            lines.append(f"   - *{r['verification_note']}* (score {r['match_score']}, ML {r['ml_confidence']})")
            lines.append("")

    args.out_json.write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n")
    args.out_md.write_text("\n".join(lines))
    print(args.out_md)
    print(args.out_json)


if __name__ == "__main__":
    main()
