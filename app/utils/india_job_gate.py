"""
Strict India relevance for job rows (tech + non-tech).

- On-site / hybrid office: India country, Indian city, explicit India text, or India-focused board URL.
- Remote / hybrid WFH: must still show India intent (city, \"India\", INR/₹, or known India board),
  not generic global remote with no India tie.
"""

from __future__ import annotations

from typing import Any, Dict

# Strong signals in URL (India-focused boards / TLDs)
INDIA_URL_HINTS = (
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
    "hirist.tech",
    "unstop.com",
    "hackerearth.com",
    "jobhai.com",
    "careerjet.co.in",
    "simplyhired.co.in",
)

# Phrases that tie a role to India (remote included)
INDIA_PHRASES = (
    " india",
    "(india)",
    ", india",
    "across india",
    "pan india",
    "pan-india",
    "anywhere in india",
    "all india",
    "indian ",
    "india-based",
    "based in india",
    "work from anywhere in india",
    "remote india",
    "remote (india",
    "wfh india",
)


def _load_india_cities() -> list[str]:
    from scripts.discovery.base import load_pilot_cities

    cities = load_pilot_cities()
    out = [c.strip().lower() for c in (cities.get("india") or []) if c and str(c).strip()]
    # Common variants
    extra = ("gurugram", "gurgaon", "noida", "bengaluru")
    for e in extra:
        if e not in out:
            out.append(e)
    return out


def _inr_signal(job: Dict[str, Any], blob: str) -> bool:
    if "₹" in (job.get("description") or "") or "₹" in (job.get("title") or ""):
        return True
    if " inr" in blob or "inr " in blob or "lakh" in blob or "lpa" in blob:
        return True
    return False


def passes_india_relevance(job: Dict[str, Any]) -> bool:
    """True if job is plausibly India (on-site in India or remote tied to India)."""
    india_cities = _load_india_cities()

    title = (job.get("title") or "").lower()
    desc = ((job.get("description") or "")[:8000]).lower()
    loc_detail = (job.get("location_detail") or job.get("location") or "").lower()
    country = (job.get("country") or "").strip().lower()
    url = (job.get("url") or job.get("apply_url") or "").lower()
    loc_type = (job.get("location_type") or "").strip()

    blob = f"{title} {desc} {loc_detail} {url}"

    if country == "india":
        return True

    for city in india_cities:
        if city and city in blob:
            return True

    for phrase in INDIA_PHRASES:
        if phrase in blob:
            return True

    if _inr_signal(job, blob):
        return True

    url_india_board = any(h in url for h in INDIA_URL_HINTS)

    # Remote / hybrid: require India tie (not generic US/EU-only remote)
    if loc_type in ("Remote", "Hybrid"):
        if url_india_board:
            return True
        if "india" in blob or "indian" in blob:
            return True
        for city in india_cities:
            if city and city in blob:
                return True
        if _inr_signal(job, blob):
            return True
        return False

    # On-site / unknown type: India geography or India board
    if url_india_board:
        return True
    if "india" in blob or "indian" in blob:
        return True
    for city in india_cities:
        if city and city in blob:
            return True

    return False

