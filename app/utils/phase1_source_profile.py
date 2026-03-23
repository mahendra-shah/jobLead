"""
Phase 1 — infer how well a discovered job-board source matches your audience:
- India-local vs global remote
- Fresher / internship signals in URL or name
- Tech vs non-tech (bias non-tech on ties so non-tech share grows)

Used when importing crawl-ready sources into Mongo `job_board_sources`.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

# Domains / patterns that rarely produce fresher-friendly India/remote student jobs
HIGH_NOISE_DOMAIN_FRAGMENTS = (
    "crypto.jobs",
    "cryptojobs",
    "cryptocurrency",
    "bitcoin",
    "blockchain",
    "web3.",
    "nft",
    "defi",
    "trader",
    "forex",
    "gambling",
)

# Strong India job-ecosystem signals (domain or URL path)
INDIA_URL_SIGNALS = (
    ".co.in",
    ".in/",
    "naukri.com",
    "shine.com",
    "foundit.in",
    "instahyre.com",
    "cutshort.io",
    "apna.co",
    "freshersworld.com",
    "internshala.com",
    "indeed.co.in",
    "glassdoor.co.in",
    "timesjobs.com",
    "iimjobs.com",
    "hirist.com",
    "hackerearth.com",
    "unstop.com",
)

INDIA_TEXT_TOKENS = (
    "india",
    "indian",
    "bangalore",
    "bengaluru",
    "hyderabad",
    "mumbai",
    "pune",
    "chennai",
    "delhi",
    "gurgaon",
    "gurugram",
    "noida",
    "kolkata",
    "ahmedabad",
)

REMOTE_SIGNALS = (
    "remote",
    "wfh",
    "work from home",
    "work-from-home",
    "distributed",
    "anywhere",
    "worldwide",
    "globally",
    "fully remote",
)

FRESHER_SIGNALS = (
    "fresher",
    "freshers",
    "intern",
    "internship",
    "graduate",
    "entry level",
    "entry-level",
    "campus",
    "trainee",
    "apprentice",
    "early career",
    "first job",
    "0-1 year",
    "0 to 1 year",
    "no experience",
    "full time",
    "full-time",
    "part time",
    "part-time",
)

# Non-tech: longer list so more sources classify as non-tech when ambiguous
NONTECH_KEYWORDS = (
    "marketing",
    "sales",
    "hr",
    "human resources",
    "recruit",
    "recruiter",
    "talent",
    "people ops",
    "operations",
    "admin",
    "administration",
    "accounting",
    "finance",
    "legal",
    "compliance",
    "customer support",
    "customer success",
    "call center",
    "bpo",
    "content writer",
    "copywriter",
    "seo",
    "social media",
    "data entry",
    "data analyst",
    "business analyst",
    "analyst",
    "supply chain",
    "logistics",
    "procurement",
    "teacher",
    "teaching",
    "nurse",
    "healthcare",
    "receptionist",
    "office assistant",
    "executive assistant",
    "digital marketing",
    "data analytics",
    "crm",
    "salesforce",
    "hubspot",
    "zoho",
    "people operations",
)

TECH_KEYWORDS = (
    "developer",
    "engineer",
    "engineering",
    "software",
    "devops",
    "sre",
    "full stack",
    "fullstack",
    "frontend",
    "backend",
    "html",
    "css",
    "javascript",
    "react",
    "mern",
    "pern",
    "mean stack",
    "mongodb",
    "postgres",
    "postgresql",
    "express.js",
    "expressjs",
    "nodejs",
    "node.js",
    "python",
    "java",
    "node",
    "golang",
    "rust",
    "kubernetes",
    "docker",
    "ml ",
    "machine learning",
    "data scientist",
    "data science",
    "ai ",
    "cloud",
    "security engineer",
    "qa ",
    "test automation",
    "ios",
    "android",
    "mobile",
)


def _load_pilot_india_cities() -> list[str]:
    path = Path(__file__).resolve().parent.parent / "data" / "pilot_cities.json"
    if not path.exists():
        return []
    data = json.loads(path.read_text(encoding="utf-8"))
    return [c.strip().lower() for c in (data.get("india") or []) if c and str(c).strip()]


_INDIA_CITIES = _load_pilot_india_cities()


def _combined_text(source: dict[str, Any]) -> str:
    name = (source.get("name") or "").lower()
    url = (source.get("url") or "").lower()
    domain = (source.get("domain") or "").lower()
    return f"{name} {url} {domain}"


def is_high_noise_domain(domain: str, url: str) -> bool:
    d = (domain or "").lower()
    u = (url or "").lower()
    blob = f"{d} {u}"
    return any(x in blob for x in HIGH_NOISE_DOMAIN_FRAGMENTS)


def infer_india_focus(source: dict[str, Any]) -> bool:
    raw_country = (source.get("country") or "").strip()
    if raw_country.lower() == "india":
        return True
    city = (source.get("city") or "").strip().lower()
    if city and city in _INDIA_CITIES:
        return True
    url = (source.get("url") or "").lower()
    name = (source.get("name") or "").lower()
    blob = f"{url} {name}"
    if any(sig in url for sig in INDIA_URL_SIGNALS):
        return True
    if any(tok in blob for tok in INDIA_TEXT_TOKENS):
        return True
    return False


def infer_remote_focus(source: dict[str, Any]) -> bool:
    blob = _combined_text(source)
    return any(sig in blob for sig in REMOTE_SIGNALS)


def infer_fresher_signals(source: dict[str, Any]) -> bool:
    blob = _combined_text(source)
    return any(sig in blob for sig in FRESHER_SIGNALS)


def classify_category(source: dict[str, Any]) -> str:
    """
    Return 'non-tech' or 'tech'. On tie, prefer non-tech (your product goal).
    """
    blob = _combined_text(source)
    n_score = sum(1 for kw in NONTECH_KEYWORDS if kw in blob)
    t_score = sum(1 for kw in TECH_KEYWORDS if kw in blob)
    if n_score > t_score:
        return "non-tech"
    if t_score > n_score:
        return "tech"
    return "non-tech"


def infer_region_label(source: dict[str, Any]) -> str:
    """India vs Global for reporting (student pipeline uses india_focus + remote_focus)."""
    if infer_india_focus(source):
        return "India"
    return "Global"


def student_pipeline_eligible(source: dict[str, Any]) -> bool:
    """
    Keep sources that plausibly list remote OR India-relevant roles for students.
    Drops obvious crypto/noise domains.
    """
    if is_high_noise_domain(source.get("domain") or "", source.get("url") or ""):
        return False
    india = infer_india_focus(source)
    remote = infer_remote_focus(source)
    if not (india or remote):
        return False
    # Fresher-friendly boards often say so in URL; if neither India nor remote, already out.
    # For India-only city boards without "remote" in URL, india_focus is enough.
    return True


def build_phase1_metadata_extra(source: dict[str, Any]) -> dict[str, Any]:
    """Merge into Mongo metadata for transparency / future ML."""
    return {
        "phase1": {
            "india_focus": infer_india_focus(source),
            "remote_focus": infer_remote_focus(source),
            "fresher_signals": infer_fresher_signals(source),
            "student_pipeline_eligible": student_pipeline_eligible(source),
            "category_inferred": classify_category(source),
        }
    }
