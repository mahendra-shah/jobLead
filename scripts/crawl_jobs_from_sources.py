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
import random
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urljoin, urlparse

SCRIPT_DIR = Path(__file__).resolve().parent
JOBLEAD_ROOT = SCRIPT_DIR.parent
if str(JOBLEAD_ROOT) not in sys.path:
    sys.path.insert(0, str(JOBLEAD_ROOT))

import httpx
from bs4 import BeautifulSoup  # not heavily used here but available for future refinements

from app.config import settings
from app.utils.job_parser import parse_experience
from app.utils.source_classifier import classify_source
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


def load_crawl_ready_sources_from_mongo(
    *,
    limit: int,
    student_pipeline_priority: bool,
    student_pipeline_only: bool,
) -> list[dict]:
    from app.services.mongodb_job_board_source_service import MongoJobBoardSourcesService

    svc = MongoJobBoardSourcesService()
    return svc.get_phase2_crawl_queue(
        limit=limit,
        student_pipeline_priority=student_pipeline_priority,
        student_pipeline_only=student_pipeline_only,
    )


def load_crawl_ready_sources_from_mongo_slice(
    *,
    offset: int,
    limit: int,
    student_pipeline_priority: bool,
    student_pipeline_only: bool,
) -> list[dict]:
    from app.services.mongodb_job_board_source_service import MongoJobBoardSourcesService

    svc = MongoJobBoardSourcesService()
    return svc.get_phase2_crawl_queue_slice(
        offset=offset,
        limit=limit,
        student_pipeline_priority=student_pipeline_priority,
        student_pipeline_only=student_pipeline_only,
    )


def iso_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


SOURCE_PERF_PATH = JOBLEAD_ROOT / "app" / "data" / "pipeline" / "source_yield_state.json"


def _load_source_perf() -> dict:
    if not SOURCE_PERF_PATH.exists():
        return {"sources": {}}
    try:
        data = json.loads(SOURCE_PERF_PATH.read_text(encoding="utf-8"))
        if isinstance(data, dict):
            data.setdefault("sources", {})
            return data
    except Exception:
        pass
    return {"sources": {}}


def _save_source_perf(data: dict) -> None:
    SOURCE_PERF_PATH.parent.mkdir(parents=True, exist_ok=True)
    SOURCE_PERF_PATH.write_text(json.dumps(data, indent=2), encoding="utf-8")


def extract_job_details_from_page(html: str, page_url: str) -> dict:
    """
    Best-effort extraction of richer job fields from a job detail page.
    Keeps things generic so it works across many boards.
    """
    soup = BeautifulSoup(html, "html.parser")
    details: dict = {}

    # Structured data first (JSON-LD JobPosting), then generic HTML fallbacks.
    structured = _extract_jobposting_jsonld(soup, page_url)
    if structured:
        details.update(structured)

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
        if len((details.get("description") or "").strip()) < 400:
            details["description"] = desc[:8000]

    # 2) Salary: look for common patterns
    full_text = (details.get("description") or soup.get_text(" ", strip=True) or "")[:12000]
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

    # 4.5) Experience hints (for fresher/experienced filtering downstream)
    exp_match = re.search(
        r"(\d+\s*(?:\+|to|-)\s*\d*\s*(?:years?|yrs?)|fresher|entry[-\s]*level|0[-\s]*2\s*(?:years?|yrs?))",
        full_text,
        re.IGNORECASE,
    )
    if exp_match:
        details["experience_required"] = exp_match.group(0).strip()

    # 5) Apply URL (if there is a clear apply button/link)
    for a in soup.find_all("a", href=True):
        txt = a.get_text(" ", strip=True).lower()
        if any(k in txt for k in ["apply", "submit application", "apply now"]):
            details["apply_url"] = a["href"]
            break

    # 6) Location hints and work mode
    if not details.get("location"):
        loc_match = re.search(
            r"\b(?:location|job location)\s*[:\-]\s*([A-Za-z][A-Za-z0-9 ,./()-]{2,80})",
            full_text,
            re.IGNORECASE,
        )
        if loc_match:
            details["location"] = loc_match.group(1).strip()

    mode_text = full_text.lower()
    if "work_type" not in details:
        if any(k in mode_text for k in ("work from home", "wfh", "remote")):
            details["work_type"] = "Remote"
        elif "hybrid" in mode_text:
            details["work_type"] = "Hybrid"
        elif any(k in mode_text for k in ("onsite", "on-site", "work from office", "office only")):
            details["work_type"] = "Onsite"

    # 7) Company fallback from common metadata / visible labels
    if not str(details.get("company") or "").strip():
        for sel in (
            'meta[name="application-name"]',
            'meta[name="author"]',
        ):
            tag = soup.select_one(sel)
            if not tag:
                continue
            val = str(tag.get("content") or "").strip()
            if val and "." not in val and len(val) >= 2:
                details["company"] = val[:120]
                break
    if not str(details.get("company") or "").strip():
        txt = soup.get_text(" ", strip=True)[:12000]
        m = re.search(
            r"(?:company|hiring\s+organization|hiring\s+company)\s*[:\-]\s*([A-Za-z0-9][A-Za-z0-9 .,&()\-]{1,90})",
            txt,
            re.IGNORECASE,
        )
        if m:
            details["company"] = m.group(1).strip(" -")[:120]

    return details


def _extract_jobposting_jsonld(soup: BeautifulSoup, page_url: str) -> dict:
    """Extract high-confidence fields from schema.org JobPosting blocks."""
    details: dict = {}

    def _iter_nodes(node):
        if isinstance(node, list):
            for x in node:
                yield from _iter_nodes(x)
            return
        if isinstance(node, dict):
            typ = str(node.get("@type") or "").lower()
            if typ == "jobposting" or "jobposting" in typ:
                yield node
            for v in node.values():
                if isinstance(v, (dict, list)):
                    yield from _iter_nodes(v)

    for script in soup.find_all("script", attrs={"type": "application/ld+json"}):
        txt = (script.string or script.get_text() or "").strip()
        if not txt:
            continue
        try:
            payload = json.loads(txt)
        except Exception:
            continue
        for jp in _iter_nodes(payload):
            if not isinstance(jp, dict):
                continue
            desc = str(jp.get("description") or "").strip()
            if desc:
                details["description"] = BeautifulSoup(desc, "html.parser").get_text(" ", strip=True)[:8000]

            emp = jp.get("employmentType")
            if isinstance(emp, list):
                emp = ", ".join(str(x) for x in emp if x)
            if emp:
                details["work_type"] = str(emp)[:80]

            dpost = jp.get("datePosted")
            if dpost:
                details["job_posted_at_raw"] = str(dpost)[:40]

            org = jp.get("hiringOrganization")
            if isinstance(org, dict):
                org_name = str(org.get("name") or "").strip()
                if org_name:
                    details["company"] = org_name[:120]
            elif isinstance(org, str):
                org_name = str(org).strip()
                if org_name:
                    details["company"] = org_name[:120]

            # salary from baseSalary object
            bs = jp.get("baseSalary")
            if isinstance(bs, dict):
                val = bs.get("value")
                cur = bs.get("currency")
                if isinstance(val, dict):
                    mn = val.get("minValue")
                    mx = val.get("maxValue")
                    unit = val.get("unitText")
                    if mn is not None or mx is not None:
                        details["salary"] = f"{cur or ''} {mn or ''}-{mx or ''} {unit or ''}".strip()
            elif isinstance(bs, (str, int, float)):
                details["salary"] = str(bs)[:80]

            # location + country
            loc = jp.get("jobLocation")
            if isinstance(loc, list):
                loc = loc[0] if loc else None
            if isinstance(loc, dict):
                adr = loc.get("address") or {}
                city = adr.get("addressLocality")
                region = adr.get("addressRegion")
                country = adr.get("addressCountry")
                chunks = [str(x).strip() for x in (city, region) if x]
                if chunks:
                    details["location"] = ", ".join(chunks)[:120]
                    details["location_detail"] = details["location"]
                if country:
                    details["country"] = str(country)[:60]

            apl = jp.get("url") or jp.get("applyUrl")
            if apl:
                details["apply_url"] = _to_absolute_url(str(apl), page_url)

            quals = jp.get("qualifications")
            if quals:
                details["degree"] = str(BeautifulSoup(str(quals), "html.parser").get_text(" ", strip=True))[:120]

            skills = jp.get("skills")
            if isinstance(skills, str):
                parts = [x.strip() for x in re.split(r",|;|\|", skills) if x.strip()]
                if parts:
                    details["skills"] = parts[:25]
            elif isinstance(skills, list):
                parts = [str(x).strip() for x in skills if str(x).strip()]
                if parts:
                    details["skills"] = parts[:25]

            break
        if details:
            break
    return details


def _detail_field_score(extra: dict) -> int:
    """Rough completeness score for detail extraction quality."""
    score = 0
    desc = (extra.get("description") or "").strip()
    if len(desc) >= 300:
        score += 2
    elif len(desc) >= 120:
        score += 1
    if extra.get("salary"):
        score += 1
    if extra.get("experience_required"):
        score += 1
    if extra.get("skills") and isinstance(extra.get("skills"), list) and len(extra.get("skills")) >= 3:
        score += 1
    if extra.get("location") or extra.get("location_detail"):
        score += 1
    if extra.get("work_type"):
        score += 1
    if extra.get("apply_url"):
        score += 1
    return score


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

DIGITAL_MARKETING_TERMS = (
    "digital marketing",
    "marketing",
    "seo",
    "sem",
    "ppc",
    "google ads",
    "meta ads",
    "social media",
    "performance marketing",
    "content marketing",
    "growth marketing",
    "brand marketing",
)

ALWAYS_EXCLUDED_SOURCE_TOKENS = (
    "internshala.com",
    # Many listing URLs resolve to thin/category pages with duplicate generic titles.
    "webpulseindia.com",
    "webpulse.co.in",
    # Noisy global aggregator for this student-focused pipeline.
    "moaijobs.com",
)


def _to_absolute_url(value: str, base_url: str) -> str:
    v = (value or "").strip()
    if not v:
        return ""
    if v.startswith(("http://", "https://")):
        return v
    return urljoin(base_url, v)


def _is_popular_source_domain(domain: str, url: str = "", name: str = "") -> bool:
    return classify_source(domain=domain, url=url, name=name).is_popular


def _is_always_excluded_source(domain: str, url: str = "", name: str = "") -> bool:
    hay = " ".join(
        [
            (domain or "").strip().lower(),
            (url or "").strip().lower(),
            (name or "").strip().lower(),
        ]
    )
    return any(tok in hay for tok in ALWAYS_EXCLUDED_SOURCE_TOKENS)


def _is_digital_marketing_job(job: dict) -> bool:
    text = " ".join(
        [
            str(job.get("title") or ""),
            str(job.get("description") or ""),
            str(job.get("category") or ""),
        ]
    ).lower()
    return any(k in text for k in DIGITAL_MARKETING_TERMS)


def _sleep_source_delay(base_delay: float, jitter_max: float) -> None:
    """Slow down requests inside a single source crawl (anti-ban pacing)."""
    d = max(0.0, float(base_delay))
    j = max(0.0, float(jitter_max))
    if d <= 0 and j <= 0:
        return
    time.sleep(d + (random.uniform(0.0, j) if j > 0 else 0.0))


def _normalize_company_name(raw_company: str, title: str, source_domain: str) -> str:
    c = (raw_company or "").strip()
    if c and c.lower() not in {"n/a", "na", "unknown", "none"}:
        return c
    t = (title or "").strip()
    m = re.search(r"\b(?:at|@\s*)\s+([A-Za-z0-9][A-Za-z0-9 .,&()\-]{1,70})$", t, re.IGNORECASE)
    if m:
        guess = m.group(1).strip(" -")
        if len(guess) >= 2:
            return guess
    # Do not default to source domain (e.g. "apna.co") as company.
    # If we can't infer a real employer name, keep it empty.
    return ""


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
    if re.search(r"\bindia\b", combined):
        out["country"] = "India"
    elif re.search(r"\b(united states|usa|u\.s\.a?)\b", combined):
        out["country"] = "United States"
    elif re.search(r"\b(united kingdom|uk|u\.k\.)\b", combined):
        out["country"] = "United Kingdom"
    elif re.search(r"\bgermany\b", combined):
        out["country"] = "Germany"
    elif re.search(r"\bcanada\b", combined):
        out["country"] = "Canada"
    elif re.search(r"\baustralia\b", combined):
        out["country"] = "Australia"
    if any(w in title for w in ["intern", "internship"]): out["work_type"] = "Internship"
    elif "part-time" in desc or "part time" in desc: out["work_type"] = "Part-time"
    elif "contract" in desc: out["work_type"] = "Contract"
    elif "full-time" in desc or "full time" in desc: out["work_type"] = "Full-time"
    else: out["work_type"] = ""
    if any(w in title for w in ["intern", "fresher", "graduate", "entry level", "entry-level"]):
        out["seniority"] = "Fresher / Entry"
    elif "junior" in title or "associate" in title:
        out["seniority"] = "Junior"
    elif "senior" in title or "lead" in title or "staff" in title or "principal" in title:
        out["seniority"] = "Senior"
    else:
        exp = parse_experience(desc.lower())
        mn = exp.get("min")
        mx = exp.get("max")
        is_fresher = bool(exp.get("is_fresher"))
        if is_fresher:
            out["seniority"] = "Fresher / Entry"
        elif mn is not None and mn <= 1:
            out["seniority"] = "Junior"
        elif mx is not None and mx >= 5:
            out["seniority"] = "Senior"
        else:
            out["seniority"] = ""
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
        # Many job cards have short titles; avoid killing all jobs on short text alone.
        # Prefer URL-based signals when title is short/empty.
        if not t:
            return True
        if any(k in u for k in ("/job/", "/jobs/", "/career", "/careers", "apply")):
            return False
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
            # Match scammy "earn/buy/sell USDT" style text.
            # Avoid false positives from pages that only mention bitcoin/crypto
            # as part of categories (e.g., remote job listing pages).
            r"\b(?:USDT|crypto\s+earn|earn\s+USDT|\d+\s*USDT\s*=|buy\s+USDT|sell\s+USDT|IMPS.*UPI.*(?:rupee|INR|RS))\b",
            re.IGNORECASE,
        ),
        re.compile(
            r"\b(?:job\s+support|interview\s+support|interview\s+preparation\s+service|mock\s+interview|interview\s+coaching|interview\s+assist|we\s+provide\s+structured\s+interview|training\s+support\s+for\s+IT\s+professionals)\b",
            re.IGNORECASE,
        ),
        re.compile(
            # Avoid false positives where "learn" contains substring "earn".
            r"(?:youtube|instagram|telegram)\s+chann?el.*task|promote.*chann?el|online.*youtube.*task|\bear\b.*(?:like|subscribe|view|share)",
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
            # Keep this strict: require obvious scam cues.
            "usdt",
            "imps",
            "upi",
            "buy",
            "sell",
            "trader",
            "spot trading",
        )
    ):
        return True

    # Some sources expose listing/search hubs as "job-like" URLs; skip them.
    # Keep this conservative: many real job URLs do not include "/job/" in the path.
    if any(h in u for h in ("/jobs/search", "/jobs/all", "/jobs?")):
        return True
    if any(
        token in u
        for token in (
            "/blog",
            "/blogs",
            "/category/",
            "/categories/",
            "/resources/",
            "/events",
            "/about",
            "/contact",
            "/privacy",
            "/terms",
            "/pricing",
            "/login",
            "/signup",
            "/register",
            "/auth",
            "/candidate/login",
            "/employer/login",
        )
    ):
        return True

    return False


def _filter_jobs_for_target_profile(jobs: list[dict], *, focus_digital_marketing: bool = False) -> list[dict]:
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

    def experience_ok(desc: str, title: str) -> bool:
        cap = float(settings.JOB_BOARD_CRAWL_PROFILE_MAX_EXPERIENCE_YEARS)
        txt = f"{title or ''} {desc or ''}".lower()
        has_entry_signal = any(
            kw in txt for kw in ("fresher", "entry", "entry-level", "junior", "intern", "internship")
        )
        exp = parse_experience(txt)
        if exp.get("is_fresher"):
            return True
        mn = exp.get("min")
        mx = exp.get("max")
        # If we cannot parse, keep only if explicit entry-level signal exists.
        if mn is None and mx is None:
            return has_entry_signal
        try:
            if mn is not None and float(mn) > cap:
                return False
            if mx is not None and float(mx) > cap:
                return False
        except (TypeError, ValueError):
            return has_entry_signal
        return True if (has_entry_signal or (mn is not None or mx is not None)) else False

    def location_ok(job: dict) -> bool:
        loc_type = (job.get("location_type") or "").strip()
        remote_ok = loc_type in ("Remote", "Hybrid")
        country = (job.get("country") or "").strip().lower()

        desc = (job.get("description") or "") or ""
        loc_detail = (job.get("location_detail") or job.get("location") or "") or ""
        combined_loc = f"{loc_detail} {desc}".lower()
        city_ok = any(city in combined_loc for city in india_cities)
        india_ok = country == "india"
        # Keep remote/hybrid jobs from anywhere; otherwise require India signal.
        if remote_ok or india_ok or city_ok:
            return True
        return False

    def work_ok(job: dict) -> bool:
        wt = (job.get("work_type") or "").strip()
        if not wt:
            return True
        # Some sources populate work_type with remote/location mode (Remote/Hybrid/Onsite).
        # Treat those as acceptable; remote-ness is enforced by location_ok.
        if wt in ("Remote", "Hybrid", "Onsite"):
            return True
        return wt in ("Internship", "Part-time", "Full-time", "Freelance", "Contract")

    def seniority_ok(job: dict) -> bool:
        if (job.get("seniority") or "").strip() == "Senior":
            return False
        title = (job.get("title") or "").lower()
        # Hard reject obvious senior/experienced roles for student-only targeting.
        return not any(
            s in title for s in ("senior", "sr.", "staff", "director", "manager", "lead", "principal")
        )

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
        if focus_digital_marketing and not any(k in text for k in DIGITAL_MARKETING_TERMS):
            continue
        if not location_ok(job):
            continue
        if not work_ok(job):
            continue
        if not seniority_ok(job):
            continue
        if not experience_ok(desc, title):
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
    parser.add_argument(
        "--no-json-output",
        action="store_true",
        help="Do not write jobs JSON artifact (Mongo ingest runs only).",
    )
    parser.add_argument(
        "--from-mongo",
        action="store_true",
        help="Load sources from MongoDB job_board_sources. If Mongo is down, the run fails unless --mongo-fallback-json.",
    )
    parser.add_argument(
        "--mongo-fallback-json",
        action="store_true",
        help="If --from-mongo fails, fall back to --sources-file (default crawl_ready_sources.json).",
    )
    parser.add_argument(
        "--no-student-pipeline-priority",
        action="store_true",
        help="With --from-mongo: do not put India/remote (student_pipeline_eligible) sources first",
    )
    parser.add_argument(
        "--student-pipeline-only",
        action="store_true",
        help="With --from-mongo: only student_pipeline_eligible sources (India/remote boards)",
    )
    parser.add_argument(
        "--source-offset",
        type=int,
        default=0,
        help="With --from-mongo: skip this many sources in the ordered queue (batched daily crawl)",
    )
    parser.add_argument(
        "--write-job-ingest",
        action="store_true",
        help="Upsert each crawled job into Mongo job_ingest (before profile filter, for ML pipeline)",
    )
    parser.add_argument(
        "--crawl-batch-id",
        type=str,
        default=None,
        help="Tag for job_ingest.source_ref.crawl_batch_id (default: auto timestamp)",
    )
    parser.add_argument(
        "--ingest-source-platform",
        type=str,
        default="job_board",
        help="job_ingest.source_platform value (default: job_board)",
    )
    parser.add_argument(
        "--prefer-less-known-sources",
        action="store_true",
        help="Reorder selected sources so less-known domains are crawled first.",
    )
    parser.add_argument(
        "--exclude-popular-sources",
        action="store_true",
        help="Skip major/common boards (Internshala/LinkedIn/Naukri/Foundit/AmbitionBox/Jobsora/Cutshort).",
    )
    parser.add_argument(
        "--focus-digital-marketing",
        action="store_true",
        help="Keep only digital-marketing oriented roles in profile filter step.",
    )
    parser.add_argument(
        "--popular-source-max-jobs",
        type=int,
        default=10,
        help="Per-source cap for popular domains (used unless --exclude-popular-sources).",
    )
    parser.add_argument(
        "--source-request-delay",
        type=float,
        default=2.5,
        help="Extra delay (seconds) before each request within a source crawl (entry + detail pages).",
    )
    parser.add_argument(
        "--source-request-jitter",
        type=float,
        default=2.0,
        help="Random extra delay 0..N seconds added to --source-request-delay for each request.",
    )
    parser.add_argument(
        "--min-jobs-per-source",
        type=int,
        default=0,
        help="Performance threshold per source; sources below this are tracked as low-yield.",
    )
    parser.add_argument(
        "--auto-pause-low-yield",
        action="store_true",
        help="With --from-mongo: auto-pause sources below --min-jobs-per-source for consecutive runs.",
    )
    parser.add_argument(
        "--low-yield-runs-threshold",
        type=int,
        default=3,
        help="Consecutive low-yield runs required before auto-pause.",
    )
    args = parser.parse_args()

    run_ts_compact = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    crawl_batch_id = args.crawl_batch_id or f"crawl_{run_ts_compact}"

    if args.from_mongo:
        try:
            if int(args.source_offset or 0) > 0:
                sources = load_crawl_ready_sources_from_mongo_slice(
                    offset=int(args.source_offset),
                    limit=args.max_sources,
                    student_pipeline_priority=not args.no_student_pipeline_priority,
                    student_pipeline_only=bool(args.student_pipeline_only),
                )
            else:
                sources = load_crawl_ready_sources_from_mongo(
                    limit=args.max_sources,
                    student_pipeline_priority=not args.no_student_pipeline_priority,
                    student_pipeline_only=bool(args.student_pipeline_only),
                )
        except Exception as e:
            if args.mongo_fallback_json:
                print(f"Mongo load failed ({e}). Falling back to JSON file.")
                sources_file = args.sources_file
                if not sources_file.is_absolute():
                    sources_file = JOBLEAD_ROOT / sources_file
                sources_all = load_crawl_ready_sources(sources_file)
                off = int(args.source_offset or 0)
                if off > 0:
                    sources = sources_all[off : off + args.max_sources]
                else:
                    sources = sources_all[: args.max_sources]
            else:
                print(
                    f"Mongo load failed ({e}).\n"
                    "  JSON-only workflow: run without --from-mongo (uses app/data/crawl_ready_sources.json).\n"
                    "  Or pass --mongo-fallback-json to use JSON when Mongo is unavailable.",
                    file=sys.stderr,
                )
                return 1
    else:
        sources_file = args.sources_file
        if not sources_file.is_absolute():
            sources_file = JOBLEAD_ROOT / sources_file
        sources_all = load_crawl_ready_sources(sources_file)
        off = int(args.source_offset or 0)
        if off > 0:
            sources = sources_all[off : off + args.max_sources]
        else:
            sources = sources_all

    if not sources:
        print("No sources to crawl (empty list or missing file).")
        return 0

    # Always remove blocked sources regardless of flags.
    before_block = len(sources)
    sources = [
        s
        for s in sources
        if not _is_always_excluded_source(
            str(s.get("domain") or ""),
            str(s.get("url") or ""),
            str(s.get("name") or ""),
        )
    ]
    blocked_count = before_block - len(sources)

    if args.exclude_popular_sources:
        sources = [
            s
            for s in sources
            if not _is_popular_source_domain(
                str(s.get("domain") or ""),
                str(s.get("url") or ""),
                str(s.get("name") or ""),
            )
        ]
    if args.prefer_less_known_sources:
        sources = sorted(
            sources,
            key=lambda s: (
                1
                if _is_popular_source_domain(
                    str(s.get("domain") or ""),
                    str(s.get("url") or ""),
                    str(s.get("name") or ""),
                )
                else 0,
                str(s.get("domain") or ""),
            ),
        )

    to_crawl = sources[: args.max_sources]
    popular_cnt = 0
    for s in to_crawl:
        if _is_popular_source_domain(
            str(s.get("domain") or ""),
            str(s.get("url") or ""),
            str(s.get("name") or ""),
        ):
            popular_cnt += 1
    niche_cnt = len(to_crawl) - popular_cnt
    src_note = "MongoDB (student_pipeline first)" if args.from_mongo and not args.no_student_pipeline_priority else (
        "MongoDB" if args.from_mongo else str(sources_file)
    )
    print(
        f"Crawling {len(to_crawl)} sources from {src_note} (max_sources={args.max_sources}). "
        f"[niche={niche_cnt}, popular={popular_cnt}, popular_cap={args.popular_source_max_jobs}, blocked={blocked_count}]"
    )
    if float(args.source_request_delay) > 0 or float(args.source_request_jitter) > 0:
        print(
            f"Crawl pacing: {args.source_request_delay}s + jitter 0..{args.source_request_jitter}s "
            "per HTTP request, and the same between sources (except after the last).",
            flush=True,
        )

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

    per_source_jobs: dict[str, int] = {}
    try:
        for idx, source in enumerate(to_crawl, 1):
            m = source.get("metadata") or {}
            strategy = m.get("crawl_strategy") or {}
            entry_urls = strategy.get("entry_urls")
            if not entry_urls:
                # Some sources don't have a crawl_strategy entry_urls in the exported JSON,
                # but they do include discovered job page URLs.
                entry_urls = m.get("job_page_urls")
            if not entry_urls:
                entry_urls = [source.get("url")]

            source_domain = source.get("domain") or (urlparse(source.get("url") or "").netloc or "")
            source_discovered_date = m.get("discovered_date")
            src_class = classify_source(
                domain=str(source_domain or ""),
                url=str(source.get("url") or ""),
                name=str(source.get("name") or ""),
            )
            source_job_cap = (
                min(int(args.max_jobs_per_source), int(args.popular_source_max_jobs))
                if src_class.is_popular
                else int(args.max_jobs_per_source)
            )

            print(f"[{idx}/{len(to_crawl)}] {source_domain} ... ", end="", flush=True)
            source_jobs_before = len(jobs)

            base_for_abs = source.get("url") or ""
            normalized_entry_urls = [
                _to_absolute_url(u, base_for_abs) for u in (entry_urls or []) if u
            ]
            for entry_url in normalized_entry_urls:
                if len(jobs) - source_jobs_before >= source_job_cap:
                    break
                if not entry_url:
                    continue

                try:
                    _sleep_source_delay(args.source_request_delay, args.source_request_jitter)
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
                    url = _to_absolute_url(j.get("url") or "", entry_url)
                    if not url or url in seen_urls:
                        continue
                    title = (j.get("title") or "").strip()
                    company = _normalize_company_name(j.get("company") or "", title, source_domain)
                    early_combined = " ".join(
                        [
                            title,
                            company,
                            (j.get("location") or "").strip(),
                            url,
                        ]
                    )
                    if _is_non_job_or_spam(title, url, early_combined):
                        continue
                    seen_urls.add(url)
                    # Layer 2: detail crawl on job URL (and alternate apply URL if present).
                    extra: dict = {}
                    raw_apply = _to_absolute_url(str(j.get("apply_url") or ""), entry_url)
                    # Prefer apply page first for detail extraction since Step 2 validates
                    # final job quality against the apply-flow content.
                    detail_candidates = []
                    if raw_apply:
                        detail_candidates.append(raw_apply)
                    if url and url not in detail_candidates:
                        detail_candidates.append(url)
                    best_score = -1
                    for du in detail_candidates:
                        try:
                            _sleep_source_delay(args.source_request_delay, args.source_request_jitter)
                            rate_limit_before_request(du)
                            detail_resp = client.get(du)
                            detail_resp.raise_for_status()
                            cand = extract_job_details_from_page(detail_resp.text, du)
                            score = _detail_field_score(cand)
                            if score > best_score:
                                best_score = score
                                extra = cand
                            # Good enough detail quality, no need to try more URLs.
                            if score >= 4:
                                break
                        except Exception:
                            continue
                    job = {
                        "title": title,
                        "company": company,
                        "location": j.get("location"),
                        "url": url,
                        "source_domain": source_domain,
                        "source_discovered_date": source_discovered_date,
                        "job_posted_at_raw": None,
                        "crawled_at_utc": crawled_at,
                    }
                    job.update(extra)
                    job["apply_url"] = _to_absolute_url(job.get("apply_url") or "", url) or url
                    job = _normalize_job(job)
                    jobs.append(job)
                    if len(jobs) - source_jobs_before >= source_job_cap:
                        break

            source_jobs_count = len(jobs) - source_jobs_before
            per_source_jobs[str(source_domain or "").strip().lower()] = source_jobs_count
            print(f"{source_jobs_count} jobs")
            # Pace between sources (and after sources with zero HTTP attempts — no inner-loop sleeps).
            if idx < len(to_crawl):
                _sleep_source_delay(args.source_request_delay, args.source_request_jitter)
    finally:
        client.close()

    perf_state = _load_source_perf()
    perf_sources = perf_state.setdefault("sources", {})
    min_jobs_threshold = max(0, int(args.min_jobs_per_source or 0))
    pause_after = max(1, int(args.low_yield_runs_threshold or 3))
    to_pause_domains: list[str] = []
    for domain, jobs_count in per_source_jobs.items():
        if not domain:
            continue
        row = dict(perf_sources.get(domain) or {})
        row["last_jobs"] = int(jobs_count)
        row["last_run_at"] = iso_now()
        row["runs"] = int(row.get("runs") or 0) + 1
        row["total_jobs"] = int(row.get("total_jobs") or 0) + int(jobs_count)
        if min_jobs_threshold > 0 and int(jobs_count) < min_jobs_threshold:
            row["consecutive_low_yield"] = int(row.get("consecutive_low_yield") or 0) + 1
        else:
            row["consecutive_low_yield"] = 0
        perf_sources[domain] = row

        if (
            args.auto_pause_low_yield
            and args.from_mongo
            and min_jobs_threshold > 0
            and int(row.get("consecutive_low_yield") or 0) >= pause_after
        ):
            to_pause_domains.append(domain)
    _save_source_perf(perf_state)

    if to_pause_domains and args.from_mongo:
        from app.services.mongodb_job_board_source_service import MongoJobBoardSourcesService

        svc = MongoJobBoardSourcesService()
        paused_n = svc.pause_sources_by_domain(
            to_pause_domains,
            reason=f"low_yield_below_{min_jobs_threshold}_for_{pause_after}_runs",
        )
        print(
            "Auto-paused low-yield sources: "
            f"{paused_n} domains (threshold={min_jobs_threshold}, runs={pause_after})"
        )

    if args.write_job_ingest:
        from app.services.mongodb_job_ingest_service import MongoJobIngestService

        ingest = MongoJobIngestService()
        ing_ok = 0
        dup_n = 0
        fail_n = 0
        for j in jobs:
            try:
                ingest.upsert_from_crawl(
                    j,
                    crawl_batch_id=crawl_batch_id,
                    source_platform=str(args.ingest_source_platform or "job_board"),
                )
                ing_ok += 1
            except Exception as ex:
                msg = str(ex)
                if "E11000 duplicate key error" in msg:
                    dup_n += 1
                    continue
                fail_n += 1
                print(f"  job_ingest upsert failed: {msg}")
        print(
            "Mongo job_ingest: "
            f"upserted={ing_ok} duplicates={dup_n} failed={fail_n} total={len(jobs)} "
            f"(batch={crawl_batch_id})."
        )

    if args.no_json_output:
        print("Skipping JSON output (--no-json-output).")
        return 0

    if not args.no_profile_filter:
        before = len(jobs)
        jobs = _filter_jobs_for_target_profile(
            jobs,
            focus_digital_marketing=bool(args.focus_digital_marketing),
        )
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

