"""
Merge multiple jobs_run_*.json files into a single jobs_master.json,
deduplicating by job URL and computing per-source statistics.
Also writes top_sources_by_jobs.json for quick inspection.
"""
import json
import sys
import re
import argparse
from collections import Counter
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
JOBLEAD_ROOT = SCRIPT_DIR.parent
if str(JOBLEAD_ROOT) not in sys.path:
    sys.path.insert(0, str(JOBLEAD_ROOT))

from app.utils.job_parser import parse_experience
from scripts.discovery.base import load_pilot_cities
from scripts.crawl_jobs_from_sources import _derive_segment_category


def find_job_run_files(jobs_dir: Path) -> list[Path]:
    """Return run files sorted oldest first. Caller should process in reverse so newest wins per URL."""
    return sorted(jobs_dir.glob("jobs_run_*.json"))


def _is_non_job_or_spam(title: str, url: str, combined_text: str) -> bool:
    t = (title or "").strip().lower()
    u = (url or "").strip().lower()
    if not t or len(t) < 6:
        return True

    non_job_markers = (
        "see open roles",
        "see open positions",
        "open positions",
        "open roles",
        "learn more",
        "benefits",
        "life at",
        "university",
        "general application",
        "apply now",
        "view job",
        "view jobs",
        "post a job",
    )
    if any(m in t for m in non_job_markers):
        return True

    # Crypto / interview-coaching / supplier spam patterns.
    spam_patterns = [
        re.compile(
            r"\b(?:USDT|bitcoin|BTC|ETH|ethereum|crypto\s+earn|earn\s+USDT|\d+\s*USDT\s*=|buy\s+USDT|sell\s+USDT|IMPS.*UPI.*(?:rupee|INR|RS))\b",
            re.IGNORECASE,
        ),
        re.compile(
            r"\b(?:job\s+support|interview\s+support|interview\s+preparation\s+service|mock\s+interview|interview\s+coaching|interview\s+assist|we\s+provide\s+structured\s+interview|training\s+support\s+for\s+IT\s+professionals)\b",
            re.IGNORECASE,
        ),
        re.compile(
            r"(?:youtube|instagram|telegram)\s+chann?el.*task|promote.*chann?el|online.*youtube.*task|earn.*(?:like|subscribe|view|share)",
            re.IGNORECASE,
        ),
        re.compile(
            r"24\s*\*\s*365.*(?:all\.weather|supplier|work)|reliable.*supplier.*(?:earn|income)|IMPS|UPI.*bank\s+card",
            re.IGNORECASE,
        ),
    ]

    txt = (combined_text or "").lower()
    if any(p.search(txt) for p in spam_patterns):
        return True

    # Extra crypto noise filter: many crypto-related posts are noisy promotions.
    if "crypto" in txt and any(
        k in txt
        for k in (
            "usdt",
            "bitcoin",
            "btc",
            "eth",
            "defi",
            "trader",
            "spot trading",
            "exchange",
            "token",
        )
    ):
        return True

    # listing/search hubs
    if any(h in u for h in ("/jobs/search", "/jobs/all", "/jobs?")) and "/job/" not in u:
        return True

    return False


def _profile_filters(jobs: list[dict]) -> list[dict]:
    """
    Apply your daily target profile rules:
    - Remote/Hybrid OR India + pilot cities
    - work_type: Internship/Part-time/Full-time (or empty)
    - experience: fresher or min/max does not exceed 2 years
    - reject senior titles
    - reject obvious non-job/spam
    """
    cities = load_pilot_cities()
    india_cities = [c.strip().lower() for c in (cities.get("india") or []) if c and c.strip()]

    # Keep only jobs that match your tech/non-tech intent.
    TECH_WORDS = (
        "mern",
        "pern",
        "react",
        "javascript",
        "nodejs",
        "node.js",
        "express",
        "mongodb",
        "mongo",
        "python",
        "html",
        "css",
        "full stack",
        "fullstack",
        "full-stack",
        "software engineer",
        "software developer",
        "developer",
        "engineer",
        "backend",
        "frontend",
        "microservices",
        "programmer",
        "programming",
        "code",
    )
    NONTECH_WORDS = (
        "data analyst",
        "data analytics",
        "data analysis",
        "data manager",
        "data entry",
        "marketing",
        "digital marketing",
        "sales",
        "account executive",
        "business development",
        "hr",
        "human resources",
        "recruiter",
        "talent acquisition",
        "customer support",
        "customer care",
        "customer success",
        "management",
        "project manager",
        "product manager",
        "admin",
        "recruitment",
        "analyst",
        "coordinator",
    )

    def experience_ok(desc: str) -> bool:
        exp = parse_experience((desc or "").lower())
        if exp.get("is_fresher"):
            return True
        mn = exp.get("min")
        mx = exp.get("max")
        if mn is None and mx is None:
            return True
        if mn is not None and mn > 2:
            return False
        if mx is not None and mx > 2:
            return False
        return True

    def location_ok(job: dict) -> bool:
        loc_type = (job.get("location_type") or "").strip()
        remote_ok = loc_type in ("Remote", "Hybrid")
        country = (job.get("country") or "").strip().lower()

        desc = (job.get("description") or "") or ""
        loc_detail = (job.get("location_detail") or job.get("location") or "") or ""
        combined_loc = f"{loc_detail} {desc}".lower()
        city_ok = any(city in combined_loc for city in india_cities)

        if remote_ok or country == "india" or city_ok:
            return True

        # Fallback: when location fields are missing/malformed,
        # still allow early-career roles you can use (fresher/junior + intern types).
        wt = (job.get("work_type") or "").strip()
        seniority = (job.get("seniority") or "").strip()
        if not loc_detail.strip() and not city_ok and country != "india":
            # If we cannot confirm location at all, prefer recall:
            # keep the job and let experience/spam filters do the heavy lifting.
            return True
        return seniority in ("Fresher / Entry", "Junior") or wt in ("Internship", "Part-time", "Full-time")

    def work_ok(job: dict) -> bool:
        wt = (job.get("work_type") or "").strip()
        if not wt:
            return True
        return wt in ("Internship", "Part-time", "Full-time")

    def seniority_ok(job: dict) -> bool:
        if (job.get("seniority") or "").strip() == "Senior":
            return False
        title = (job.get("title") or "").lower()
        # Keep only strong seniority markers; allow "Manager" roles to pass
        # because some of them are early-career (and are still covered by
        # your experience <= 2 years rule).
        return not any(s in title for s in ("senior", "sr.", "staff", "lead"))

    out: list[dict] = []
    for job in jobs:
        title = job.get("title") or ""
        url = job.get("apply_url") or job.get("url") or ""
        desc = job.get("description") or ""

        combined = " ".join(
            [
                title,
                desc,
                job.get("location_detail") or job.get("location") or "",
                job.get("company") or "",
                url or "",
            ]
        )

        if _is_non_job_or_spam(title, url, combined):
            continue
        text = f"{title} {desc}".lower()
        if not (any(w in text for w in TECH_WORDS) or any(w in text for w in NONTECH_WORDS)):
            continue
        if not location_ok(job):
            continue
        if not work_ok(job):
            continue
        if not seniority_ok(job):
            continue
        if not experience_ok(desc):
            continue
        out.append(job)

    return out


def _classify_tech_nontech(job: dict) -> str:
    # Prefer crawler-derived segment/category if present.
    seg = (job.get("segment") or "").strip()
    if seg == "Tech":
        return "tech"
    if seg == "Non-tech":
        return "nontech"

    t = (job.get("title") or "").lower()
    d = (job.get("description") or "").lower()
    text = f"{t} {d}"

    tech_keywords = (
        "mern",
        "pern",
        "react",
        "nodejs",
        "node.js",
        "express",
        "mongodb",
        "mongo",
        "python",
        "javascript",
        "typescript",
        "html",
        "css",
        "full stack",
        "fullstack",
        "software engineer",
        "software developer",
        "developer",
        "engineer",
        "backend",
        "frontend",
        "devops",
        "sre",
    )
    nontech_keywords = (
        "data analyst",
        "data analytics",
        "data manager",
        "data entry",
        "marketing",
        "sales",
        "account executive",
        "business development",
        "bdm",
        "hr",
        "human resources",
        "recruiter",
        "recruitment",
        "talent acquisition",
        "people operations",
        "customer support",
        "customer care",
        "customer success",
        "call center",
        "bpo",
        "management",
        "project manager",
        "product manager",
        "operations",
    )

    tech_hits = sum(1 for kw in tech_keywords if kw in text)
    non_hits = sum(1 for kw in nontech_keywords if kw in text)

    if non_hits > tech_hits:
        return "nontech"
    if tech_hits > non_hits:
        return "tech"
    # Tie bias to non-tech.
    return "nontech"


def _quality_score(job: dict) -> float:
    desc = (job.get("description") or "") or ""
    exp = parse_experience(desc.lower())

    score = 0.0
    if job.get("apply_url"):
        score += 10.0
    wt = (job.get("work_type") or "").strip()
    if wt in ("Internship", "Part-time", "Full-time"):
        score += 4.0
    if (job.get("seniority") or "").strip() in ("Fresher / Entry", "Junior", ""):
        score += 3.0
    if exp.get("is_fresher"):
        score += 10.0

    if desc:
        score += min(8.0, len(desc) / 600.0)
    if job.get("salary"):
        score += 2.0
    return score


def _balance_ratio(jobs: list[dict], max_master_jobs: int) -> list[dict]:
    if not jobs:
        return []

    # Cap how many jobs we keep for the master export.
    cap = len(jobs) if max_master_jobs <= 0 else min(max_master_jobs, len(jobs))
    if cap <= 0:
        return []

    tech_jobs: list[dict] = []
    nontech_jobs: list[dict] = []
    for j in jobs:
        if _classify_tech_nontech(j) == "nontech":
            nontech_jobs.append(j)
        else:
            tech_jobs.append(j)

    tech_jobs.sort(key=_quality_score, reverse=True)
    nontech_jobs.sort(key=_quality_score, reverse=True)

    # Your desired approx: tech 5, non-tech 8-9 => non-tech ~ 0.64 of total.
    target_nontech = int(round(cap * 0.64))
    target_tech = cap - target_nontech

    selected_non = nontech_jobs[:target_nontech]
    # Extra safety cap for tech relative to selected non-tech.
    max_tech_due_to_ratio = int(len(selected_non) * 5 / 9) if selected_non else 0
    if max_tech_due_to_ratio <= 0 and tech_jobs and selected_non:
        max_tech_due_to_ratio = 1

    selected_tech = tech_jobs[: min(target_tech, max_tech_due_to_ratio if max_tech_due_to_ratio > 0 else target_tech)]
    selected: list[dict] = selected_non + selected_tech

    # Fill any remaining slots preferring non-tech.
    remaining = cap - len(selected)
    if remaining > 0:
        already_non = len(selected_non)
        selected.extend(nontech_jobs[already_non : already_non + remaining])

    # Still short? Fill from tech as last resort.
    if len(selected) < cap:
        remaining = cap - len(selected)
        already_tech = len(selected_tech)
        selected.extend(tech_jobs[already_tech : already_tech + remaining])

    # Output: non-tech first for easier sheet review.
    selected.sort(key=lambda j: (0 if _classify_tech_nontech(j) == "nontech" else 1, -_quality_score(j)))
    return selected


def main() -> int:
    jobs_dir = JOBLEAD_ROOT / "app" / "data" / "jobs"
    jobs_dir.mkdir(parents=True, exist_ok=True)

    parser = argparse.ArgumentParser(description="Merge job runs → jobs_master.json")
    parser.add_argument(
        "--max-master-jobs",
        type=int,
        default=0,
        help="Cap jobs_master size after profile filter + tech/non-tech balance (default: 0 = no cap; use e.g. 60 for a small daily sample)",
    )
    args = parser.parse_args()

    run_files = find_job_run_files(jobs_dir)
    if not run_files:
        print("No jobs_run_*.json files found.")
        return 0

    print(f"Found {len(run_files)} run file(s).")

    all_jobs: list[dict] = []
    seen_urls: set[str] = set()

    # Process newest runs first so that when we dedupe by URL we keep the latest crawl (today's data).
    for f in reversed(run_files):
        data = json.loads(f.read_text(encoding="utf-8"))
        jobs = data.get("jobs") or []
        print(f"  {f.name}: {len(jobs)} jobs")
        for j in jobs:
            url = (j.get("url") or "").strip()
            if not url or url in seen_urls:
                continue
            seen_urls.add(url)
            all_jobs.append(j)

    print(f"Total unique jobs after merge: {len(all_jobs)}")

    # Apply your daily target profile + tech/non-tech balance selection.
    before_filter = len(all_jobs)
    all_jobs = _profile_filters(all_jobs)
    print(f"Profile filter: {len(all_jobs)}/{before_filter} jobs kept.")

    # Re-derive segment/category using the current crawler rules.
    for job in all_jobs:
        seg, cat = _derive_segment_category(job.get("title") or "", job.get("source_domain") or "")
        job["segment"] = seg
        job["category"] = cat

    all_jobs = _balance_ratio(all_jobs, max_master_jobs=int(args.max_master_jobs))

    # Per-source stats
    per_source = Counter(j.get("source_domain") or "unknown" for j in all_jobs)

    master_path = jobs_dir / "jobs_master.json"
    master_payload = {
        "meta": {
            "runs_merged": [f.name for f in run_files],
            "total_jobs_unique": len(all_jobs),
            "max_master_jobs": int(args.max_master_jobs),
        },
        "jobs": all_jobs,
    }
    master_path.write_text(json.dumps(master_payload, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"Wrote merged jobs -> {master_path}")

    top_sources = sorted(per_source.items(), key=lambda x: x[1], reverse=True)
    top_payload = {
        "meta": {
            "total_sources": len(per_source),
            "description": "Sources ranked by number of unique jobs in jobs_master.json",
        },
        "sources": [
            {"source_domain": domain, "job_count": count} for domain, count in top_sources
        ],
    }
    top_path = jobs_dir / "top_sources_by_jobs.json"
    top_path.write_text(json.dumps(top_payload, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"Wrote per-source stats -> {top_path}")

    # Print top 10 summary to console
    print("Top 10 sources by job count:")
    for domain, count in top_sources[:10]:
        print(f"  {domain:30} {count}")

    return 0


if __name__ == "__main__":
    sys.exit(main())

