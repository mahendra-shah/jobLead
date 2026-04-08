"""Quality gates: junk listing detection, apply URL resolution."""

from __future__ import annotations

import re
from typing import Any
from urllib.parse import urljoin, urlparse

# Hub / nav / aggregation noise (aligned with student verify script + gap analysis)
_JUNK_TITLE = re.compile(
    r"^\s*("
    r"view\s*&\s*apply|apply\s*now|categories|candidate\s*login|"
    r"jobs\s*for\s+women|customer\s*support$"
    r")\s*$",
    re.I,
)
_HUB_TITLE = re.compile(
    r"\(\d+[kK]|\d+\.?\d*k\s*\)\s*$|"  # "Ahmedabad (22.2k jobs)"
    r"jobs\)\s*$|"
    r"^jobs\s+in\s+|^jobs\s+for\s+|"  # listing pages
    r"^work from home jobs\s*$|^sales\s*&\s*bd\s*$|"
    r"^marketing\s*/\s*brand\b|^admin\s*/\s*back\s*office\b|"
    r"^view\s+all|^\s*\d+\.?\d*k\s+jobs",
    re.I,
)
_BAD_URL_PATH = re.compile(
    r"trainings\.internshala\.com|placement-guarantee-course|/jobs/jobs-in-.*-loc|/student/interstitial/",
    re.I,
)


def resolve_apply_url(job: dict[str, Any]) -> str:
    """
    Make apply URL absolute when crawlers only store a path.
    Uses `source_domain` (e.g. apna.co → https://apna.co).
    """
    raw_apply = str(job.get("apply_url") or "").strip()
    raw_url = str(job.get("url") or "").strip()
    u = raw_apply or raw_url
    if u.startswith("http://") or u.startswith("https://"):
        return u.split("#", 1)[0]

    if not u:
        return ""

    path = u if u.startswith("/") else f"/{u}"
    domain = str(job.get("source_domain") or "").strip().lower()
    if not domain:
        return u

    base = domain if domain.startswith("http") else f"https://{domain}"
    base = base.rstrip("/")
    joined = urljoin(f"{base}/", path.lstrip("/"))
    return joined.split("#", 1)[0]


def looks_like_job_board_listing_page(url: str) -> bool:
    if not url:
        return True
    p = urlparse(url)
    path = (p.path or "").lower()
    if "/jobs-in-" in path and path.rstrip("/").count("/") <= 2:
        return True
    return bool(_BAD_URL_PATH.search(url))


def quality_check(job: dict[str, Any]) -> tuple[bool, str]:
    """
    Fast pre-ML checks. Returns (ok, reason).
    """
    title = str(job.get("title") or "").strip()
    if len(title) < 5:
        return False, "title_too_short"
    if _JUNK_TITLE.match(title):
        return False, "junk_nav_title"
    if _HUB_TITLE.search(title):
        return False, "hub_or_listing_title"

    u = resolve_apply_url(job)
    if not u.startswith("http"):
        return False, "missing_absolute_url"
    if looks_like_job_board_listing_page(u):
        return False, "listing_or_bad_url_pattern"
    if _BAD_URL_PATH.search(u):
        return False, "blocked_url_pattern"

    return True, ""
