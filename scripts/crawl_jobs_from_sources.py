"""
Phase 2 (pilot): crawl jobs from crawl-ready sources discovered in Phase 1.

For now, this uses a generic HTML extractor (extract_jobs_from_html) on each
source's crawl_strategy.entry_urls, and stores a JSON file with the schema:

  title, company, location, url,
  source_domain, source_discovered_date,
  job_posted_at_raw, crawled_at_utc

This is a pilot crawler: start with a small number of sources (e.g. 5–10)
before scaling.
"""
import argparse
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

SCRIPT_DIR = Path(__file__).resolve().parent
JOBLEAD_ROOT = SCRIPT_DIR.parent
if str(JOBLEAD_ROOT) not in sys.path:
    sys.path.insert(0, str(JOBLEAD_ROOT))

import httpx
from bs4 import BeautifulSoup  # not heavily used here but available for future refinements

from app.utils.job_parser import parse_experience
from scripts.discovery.domain_rate_limiter import rate_limit_before_request
from scripts.discovery.base import load_pilot_cities
from scripts.discovery.proxy_pool import get_next_proxy
from scripts.scrape_all_jobs import (
    extract_jobs_from_html,
    REQUEST_TIMEOUT,
    BROWSER_HEADERS,
)


def load_crawl_ready_sources(path: Path) -> list[dict]:
    if not path.exists():
        return []
    data = json.loads(path.read_text(encoding="utf-8"))
    return data.get("sources") or []


def iso_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def extract_job_details_from_page(html: str, page_url: str) -> dict:
    """
    Best-effort extraction of richer job fields from a job detail page.
    Keeps things generic so it works across many boards.
    """
    soup = BeautifulSoup(html, "html.parser")
    details: dict = {}

    # 1) Description: concatenate paragraphs and list items, trimmed
    text_blocks: list[str] = []
    for node in soup.select("section, div, article"):
        cls = " ".join(node.get("class", [])).lower()
        if any(k in cls for k in ["description", "job-body", "job_body", "content", "jd"]):
            text_blocks.append(node.get_text(" ", strip=True))
    if not text_blocks:
        # fallback: all <p> and <li>
        for node in soup.find_all(["p", "li"]):
            txt = node.get_text(" ", strip=True)
            if len(txt) > 40:
                text_blocks.append(txt)
    if text_blocks:
        desc = " ".join(text_blocks)
        details["description"] = desc[:2000]

    # 2) Salary: look for common patterns
    full_text = (details.get("description") or soup.get_text(" ", strip=True) or "")[:5000]
    salary_match = re.search(
        r"(₹\s?[\d.,]+(?:\s*-\s*₹?\s*[\d.,]+)?\s*(?:lpa|per month|per annum)?|"
        r"\$[\d.,]+(?:\s*-\s*\$?[\d.,]+)?|"
        r"\b\d+\s*(?:k|K)\s*(?:-\s*\d+\s*(?:k|K))?\b)",
        full_text,
        re.IGNORECASE,
    )
    if salary_match:
        details["salary"] = salary_match.group(0).strip()

    # 3) Skills: bullets under headings like Skills / Requirements
    skills: list[str] = []
    for heading in soup.find_all(["h2", "h3", "h4"]):
        htxt = heading.get_text(" ", strip=True).lower()
        if any(k in htxt for k in ["skill", "requirement", "qualification", "responsibil"]):
            ul = heading.find_next(["ul", "ol"])
            if ul:
                for li in ul.find_all("li"):
                    txt = li.get_text(" ", strip=True)
                    if 2 < len(txt) < 120:
                        skills.append(txt)
    if skills:
        details["skills"] = skills[:25]

    # 4) Degree / education
    degree_match = re.search(
        r"(b\.?tech|bachelor['’]s?|bsc|b\.sc|mca|m\.tech|master['’]s?|b\.e\.|be in [^,.;]+)",
        full_text,
        re.IGNORECASE,
    )
    if degree_match:
        details["degree"] = degree_match.group(0).strip()

    # 5) Apply URL (if there is a clear apply button/link)
    for a in soup.find_all("a", href=True):
        txt = a.get_text(" ", strip=True).lower()
        if any(k in txt for k in ["apply", "submit application", "apply now"]):
            details["apply_url"] = a["href"]
            break

    return details


# Full set of keys we want in every job JSON (matches Google Sheet columns).
JOB_KEYS = [
    "title", "company", "location", "url",
    "source_domain", "source_discovered_date", "job_posted_at_raw", "crawled_at_utc",
    "segment", "category",
    "location_type", "location_detail", "country",
    "work_type", "seniority",
    "salary", "skills", "degree",
    "description", "apply_url",
]


def _derive_segment_category(title: str, source_domain: str) -> tuple[str, str]:
    """Derive Segment (Tech/Non-tech) and Category from title and domain."""
    t = (title or "").lower()
    domain = (source_domain or "").lower()
    tech_kw = ["developer", "engineer", "software", "backend", "frontend", "full stack", "devops", "sre", "qa engineer", "mobile developer"]
    data_kw = [
        "data analyst",
        "data analytics",
        "data analysis",
        "data manager",
        "data entry",
        "business analyst",
        "data scientist",
        "data science",
        "analyst",
        "analytics",
    ]
    sales_kw = ["sales", "account executive", "business development", "bdm"]
    marketing_kw = ["marketing", "growth", "seo", "content", "performance"]
    support_kw = ["customer support", "customer success", "support specialist"]
    hr_kw = ["hr ", "talent acquisition", "recruiter", "recruitment"]
    finance_kw = ["finance", "accountant", "controller", "fp&a", "audit"]
    product_kw = ["product manager", "product owner"]
    management_kw = ["management", "manager", "operations", "project manager", "account manager", "people operations"]
    design_kw = ["designer", "ux", "ui", "product design", "graphic design"]
    def any_kw(kws): return any(kw in t for kw in kws)
    if any_kw(tech_kw): return "Tech", "Software / Engineering"
    if any_kw(data_kw): return "Non-tech", "Data / Analytics"
    if any_kw(design_kw): return "Tech", "Design / UX"
    if any_kw(product_kw): return "Non-tech", "Product Management"
    if any_kw(management_kw): return "Non-tech", "Management / Operations"
    if any_kw(sales_kw): return "Non-tech", "Sales"
    if any_kw(marketing_kw): return "Non-tech", "Marketing / Growth"
    if any_kw(support_kw): return "Non-tech", "Customer Support / Success"
    if any_kw(hr_kw): return "Non-tech", "HR / Talent"
    if any_kw(finance_kw): return "Non-tech", "Finance / Accounting"
    if any(d in domain for d in ["github", "remoteintech", "stackoverflow"]): return "Tech", "Other / Unknown"
    return "Unknown", "Other / Unknown"


def _derive_location_work_seniority(job: dict) -> dict:
    """Derive location_type, location_detail, country, work_type, seniority from job text."""
    title = (job.get("title") or "").lower()
    location = (job.get("location") or "").strip()
    desc = (job.get("description") or "").lower()
    combined = " ".join([location, desc])
    out = {}
    out["location_type"] = "Remote" if "remote" in combined else ("Hybrid" if "hybrid" in combined else ("Onsite" if location else ""))
    out["location_detail"] = location
    out["country"] = ""
    for c in ["india", "usa", "united states", "uk", "germany", "canada", "australia"]:
        if c in combined:
            out["country"] = c.title()
            break
    if any(w in title for w in ["intern", "internship"]): out["work_type"] = "Internship"
    elif "part-time" in desc or "part time" in desc: out["work_type"] = "Part-time"
    elif "contract" in desc: out["work_type"] = "Contract"
    elif "full-time" in desc or "full time" in desc: out["work_type"] = "Full-time"
    else: out["work_type"] = ""
    if any(w in title for w in ["intern", "fresher", "graduate", "entry level", "entry-level"]): out["seniority"] = "Fresher / Entry"
    elif "junior" in title: out["seniority"] = "Junior"
    elif "senior" in title or "lead" in title: out["seniority"] = "Senior"
    else: out["seniority"] = ""
    return out


def _normalize_job(job: dict) -> dict:
    """Ensure job has all JOB_KEYS; derive segment/category/location/work/seniority if missing."""
    segment, category = _derive_segment_category(job.get("title") or "", job.get("source_domain") or "")
    job.setdefault("segment", segment)
    job.setdefault("category", category)
    derived = _derive_location_work_seniority(job)
    for k, v in derived.items():
        job.setdefault(k, v)
    job.setdefault("description", None)
    job.setdefault("apply_url", job.get("url") or "")
    job.setdefault("salary", None)
    job.setdefault("degree", None)
    if "skills" not in job:
        job["skills"] = []
    elif isinstance(job["skills"], str):
        job["skills"] = [s.strip() for s in job["skills"].split(",") if s.strip()] if job["skills"] else []
    result = {}
    for k in JOB_KEYS:
        result[k] = job.get(k)
        if result[k] is None and k not in ("skills", "company", "location", "source_discovered_date", "job_posted_at_raw"):
            result[k] = ""
        if result[k] is None and k == "skills":
            result[k] = []
    return result


def _is_non_job_or_spam(title: str, url: str, combined_text: str) -> bool:
    t = (title or "").strip().lower()
    u = (url or "").strip().lower()
    if not t or len(t) < 6:
        return True

    # Non-job listings / chrome that many boards expose as "job cards".
    NON_JOB_TITLE_MARKERS = (
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
    if any(m in t for m in NON_JOB_TITLE_MARKERS):
        return True

    # Spam patterns adapted from your earlier Telegram-style prefilter.
    SPAM_PATTERNS = [
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
    if any(p.search(txt) for p in SPAM_PATTERNS):
        return True

    # Extra crypto noise filter: many "crypto jobs" are scammy promotions.
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

    # Some sources expose listing/search hubs as "job-like" URLs; skip them.
    if any(h in u for h in ("/jobs/search", "/jobs/all", "/jobs?")) and "/job/" not in u:
        return True

    return False


def _filter_jobs_for_target_profile(jobs: list[dict]) -> list[dict]:
    """
    Keep only jobs suitable for:
    - Remote/Hybrid (global) OR India + pilot cities
    - Work types: Internship / Part-time / Full-time (or unknown)
    - Experience: fresher OR parseable min/max does not exceed 2 years
    - Reject senior titles + common non-job/spam listings
    """
    cities = load_pilot_cities()
    india_cities = [c.strip().lower() for c in (cities.get("india") or []) if c and c.strip()]

    # Keep only jobs that match your tech/non-tech intent (reduces "Unknown" junk).
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
        exp = parse_experience(desc.lower())
        if exp.get("is_fresher"):
            return True
        mn = exp.get("min")
        mx = exp.get("max")
        # If we cannot parse, keep it (better recall).
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
        india_ok = country == "india"
        if remote_ok or india_ok or city_ok:
            return True

        # Fallback: if location fields are missing, keep early-career roles.
        wt = (job.get("work_type") or "").strip()
        seniority = (job.get("seniority") or "").strip()
        if not loc_detail.strip() and not city_ok and country != "india":
            # If we cannot confirm location at all, prefer recall.
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
        # (experience parsing will still gate by <= 2 years).
        return not any(s in title for s in ("senior", "sr.", "staff", "lead"))

    filtered: list[dict] = []
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

        filtered.append(job)

    return filtered


def main() -> int:
    parser = argparse.ArgumentParser(description="Crawl jobs from crawl-ready sources (pilot)")
    parser.add_argument(
        "--sources-file",
        type=Path,
        default=Path("app/data/crawl_ready_sources.json"),
        help="Input crawl-ready sources JSON",
    )
    parser.add_argument(
        "--max-sources",
        type=int,
        default=10,
        help="Max number of sources to crawl (pilot)",
    )
    parser.add_argument(
        "--max-jobs-per-source",
        type=int,
        default=100,
        help="Soft cap on jobs per source",
    )
    parser.add_argument(
        "--no-profile-filter",
        action="store_true",
        help="Disable target profile filtering (for debugging)",
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=None,
        help="Output jobs JSON (default: app/data/jobs/jobs_run_<timestamp>.json)",
    )
    args = parser.parse_args()

    sources_file = args.sources_file
    if not sources_file.is_absolute():
        sources_file = JOBLEAD_ROOT / sources_file
    sources = load_crawl_ready_sources(sources_file)
    if not sources:
        print(f"No sources in {sources_file}")
        return 0

    to_crawl = sources[: args.max_sources]
    print(f"Crawling {len(to_crawl)} sources (of {len(sources)} total crawl-ready).")

    run_ts = iso_now().replace(":", "").replace("-", "")
    default_out = Path(f"app/data/jobs/jobs_run_{run_ts}.json")
    out_path = args.out if args.out is not None else default_out
    if not out_path.is_absolute():
        out_path = JOBLEAD_ROOT / out_path
    out_path.parent.mkdir(parents=True, exist_ok=True)

    proxies = get_next_proxy()
    client = httpx.Client(timeout=REQUEST_TIMEOUT, follow_redirects=True, headers=BROWSER_HEADERS, proxies=proxies)

    jobs: list[dict] = []
    seen_urls: set[str] = set()

    try:
        for idx, source in enumerate(to_crawl, 1):
            m = source.get("metadata") or {}
            strategy = m.get("crawl_strategy") or {}
            entry_urls = strategy.get("entry_urls") or [source.get("url")]
            source_domain = source.get("domain") or (urlparse(source.get("url") or "").netloc or "")
            source_discovered_date = m.get("discovered_date")

            print(f"[{idx}/{len(to_crawl)}] {source_domain} ... ", end="", flush=True)
            source_jobs_before = len(jobs)

            for entry_url in entry_urls:
                if len(jobs) - source_jobs_before >= args.max_jobs_per_source:
                    break
                if not entry_url:
                    continue

                try:
                    rate_limit_before_request(entry_url)
                    resp = client.get(entry_url)
                    resp.raise_for_status()
                except Exception as e:
                    print(f"\n  entry {entry_url} FAILED: {e}")
                    continue

                raw_jobs = extract_jobs_from_html(
                    resp.text,
                    entry_url,
                    source_name=source_domain,
                    source_id=str(source.get("id")),
                )
                crawled_at = iso_now()
                for j in raw_jobs:
                    url = j.get("url")
                    if not url or url in seen_urls:
                        continue
                    seen_urls.add(url)
                    # Fetch job detail page for richer fields (best-effort, but skip on error)
                    extra: dict = {}
                    try:
                        rate_limit_before_request(url)
                        detail_resp = client.get(url)
                        detail_resp.raise_for_status()
                        extra = extract_job_details_from_page(detail_resp.text, url)
                    except Exception:
                        extra = {}
                    job = {
                        "title": j.get("title"),
                        "company": j.get("company"),
                        "location": j.get("location"),
                        "url": url,
                        "source_domain": source_domain,
                        "source_discovered_date": source_discovered_date,
                        "job_posted_at_raw": None,
                        "crawled_at_utc": crawled_at,
                    }
                    job.update(extra)
                    if not job.get("apply_url"):
                        job["apply_url"] = url
                    job = _normalize_job(job)
                    jobs.append(job)
                    if len(jobs) - source_jobs_before >= args.max_jobs_per_source:
                        break

            print(f"{len(jobs) - source_jobs_before} jobs")
    finally:
        client.close()

    if not args.no_profile_filter:
        before = len(jobs)
        jobs = _filter_jobs_for_target_profile(jobs)
        print(f"Target profile filter: {len(jobs)}/{before} jobs kept.")

    payload = {
        "meta": {
            "generated_at_utc": iso_now(),
            "sources_used": len(to_crawl),
            "total_jobs": len(jobs),
        },
        "jobs": jobs,
    }
    out_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"Saved {len(jobs)} jobs -> {out_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

