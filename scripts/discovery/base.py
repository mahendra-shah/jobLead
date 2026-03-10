"""
Phase 1 Discovery: shared helpers, JSON source schema, scoring, domain/type inference.
All output goes to JSON for testing; import to DB when ready.

Source JSON schema:
  id, url, domain, type, city, country, confidence_score, first_seen, last_checked, status, discovered_date
"""
import json
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse, urlunparse

# jobLead root
SCRIPT_DIR = Path(__file__).resolve().parent
JOBLEAD_ROOT = SCRIPT_DIR.parent.parent
if str(JOBLEAD_ROOT) not in sys.path:
    sys.path.insert(0, str(JOBLEAD_ROOT))

DATA_DIR = JOBLEAD_ROOT / "app" / "data"
DISCOVERY_JSON_PATH = DATA_DIR / "discovery_sources_test.json"

# Source types we assign
SOURCE_TYPES = (
    "job_board", "telegram", "discord", "slack", "github_repo",
    "forum", "company_career", "startup_website", "website",
)

# Domain -> type (for inference)
DOMAIN_TYPE_MAP = {
    "t.me": "telegram",
    "telegram.me": "telegram",
    "telegram.dog": "telegram",
    "discord.gg": "discord",
    "discord.com": "discord",
    "github.com": "github_repo",
    "reddit.com": "forum",
    "news.ycombinator.com": "forum",
    "dev.to": "forum",
    "hashnode.dev": "forum",
    "indiehackers.com": "forum",
    "medium.com": "website",
}

# Blog/article paths -> penalize
BLOG_INDICATORS = ("/blog/", "/articles/", "/post/", "/news/", "medium.com", "dev.to")


def normalize_url(url: str) -> str:
    """
    Canonical form for dedup: lowercase scheme+netloc, no fragment, no trailing slash,
    default port stripped. Path/query preserved except fragment.
    """
    if not url or not url.strip().startswith(("http://", "https://")):
        return (url or "").strip()
    try:
        parsed = urlparse(url.strip())
        scheme = (parsed.scheme or "https").lower()
        netloc = (parsed.netloc or "").lower()
        if netloc.endswith(":80") and scheme == "http":
            netloc = netloc[:-3]
        if netloc.endswith(":443") and scheme == "https":
            netloc = netloc[:-4]
        path = (parsed.path or "/").rstrip("/") or "/"
        query = (parsed.query or "").strip()
        normalized = urlunparse((scheme, netloc, path, parsed.params or "", query, ""))
        return normalized
    except Exception:
        return (url or "").strip()


def extract_domain(url: str) -> str:
    """e.g. https://t.me/python_jobs -> t.me; https://jobs.lever.co/company -> jobs.lever.co"""
    try:
        parsed = urlparse(url.strip().lower())
        netloc = (parsed.netloc or "").strip()
        if not netloc:
            return ""
        return netloc
    except Exception:
        return ""


def infer_source_type(url: str, name: str = "") -> str:
    """Infer type from domain and path. name/label can be used later for extra signals."""
    domain = extract_domain(url)
    path = (urlparse(url).path or "").lower()

    # Domain-based
    for d, t in DOMAIN_TYPE_MAP.items():
        if d in domain:
            return t

    # Path-based
    if "/careers" in path or "/career/" in path or "/jobs" in path or "/job/" in path:
        if "career" in path or "careers" in path:
            return "company_career"
        if "job" in domain or "jobs" in domain:
            return "job_board"
        return "job_board"

    if "job" in domain or "jobs" in domain or "hiring" in domain:
        return "job_board"

    if any(x in path or x in domain for x in BLOG_INDICATORS):
        return "website"

    return "website"


def score_source(url: str, name: str = "", source_type: str = "") -> float:
    """
    Quality score for a source. Raw points; score > 5 = good source.
    Rules: contains 'jobs' +3, 'hiring' +2, domain contains 'jobs' +3, github_repo +1, blog article -1.
    """
    score = 0.0
    text = f"{(url or '').lower()} {(name or '').lower()}"
    domain = extract_domain(url)

    if "jobs" in text:
        score += 3
    if "hiring" in text:
        score += 2
    if "job" in domain or "jobs" in domain:
        score += 3
    if source_type == "github_repo" or "github.com" in domain:
        score += 1
    if any(x in url.lower() for x in ["/blog/", "/articles/", "/post/", "medium.com/", "dev.to/"]):
        score -= 1

    return round(min(10.0, max(0.0, score)), 2)


def next_source_id(sources: list[dict]) -> int:
    """Next numeric id. sources may have int or str ids."""
    if not sources:
        return 1
    nums = []
    for s in sources:
        try:
            nums.append(int(s.get("id", 0)))
        except (TypeError, ValueError):
            pass
    return max(nums, default=0) + 1


def iso_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def load_pilot_cities() -> dict:
    with open(DATA_DIR / "pilot_cities.json", "r", encoding="utf-8") as f:
        return json.load(f)


def load_fresher_keywords() -> list:
    with open(DATA_DIR / "fresher_keywords.json", "r", encoding="utf-8") as f:
        return json.load(f)["keywords"]


def load_discovery_sources_json(path: Path | None = None) -> list[dict]:
    """Load sources from JSON. Returns list of source dicts (id, url, domain, type, ...)."""
    p = path or DISCOVERY_JSON_PATH
    if not p.exists():
        return []
    with open(p, "r", encoding="utf-8") as f:
        data = json.load(f)
    return data.get("sources") or []


def save_discovery_sources_json(sources: list[dict], path: Path | None = None) -> None:
    """Write sources to JSON. Each source must have id, url, domain, type, confidence_score, first_seen, last_checked, status."""
    p = path or DISCOVERY_JSON_PATH
    p.parent.mkdir(parents=True, exist_ok=True)
    data = {
        "meta": {
            "description": "Phase 1 discovery sources (test); import to DB when ready",
            "schema": "id, url, domain, type, city, country, confidence_score, first_seen, last_checked, status, discovered_date",
        },
        "sources": sources,
    }
    with open(p, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def make_source(
    url: str,
    *,
    name: str = "",
    source_type: str | None = None,
    city: str | None = None,
    country: str | None = None,
    discovery_origin: str = "seed",
    metadata_extra: dict | None = None,
) -> dict:
    """Build one source dict with domain, type, confidence_score, first_seen, last_checked, status, discovered_date."""
    url = (url or "").strip()
    if not url or not url.startswith(("http://", "https://")):
        raise ValueError("Invalid url")
    domain = extract_domain(url)
    st = source_type or infer_source_type(url, name)
    score = score_source(url, name, st)
    now = iso_now()
    discovered_date = now.split("T", 1)[0]
    meta = dict(metadata_extra or {})
    meta["discovery_origin"] = discovery_origin
    meta.setdefault("discovered_date", discovered_date)
    return {
        "id": 0,  # caller sets after next_source_id
        "url": url,
        "domain": domain,
        "type": st,
        "name": (name or url)[:500],
        "city": city,
        "country": country,
        "confidence_score": score,
        "first_seen": now,
        "last_checked": now,
        "status": "active",
        "metadata": meta,
    }


def upsert_discovery_source_into_list(
    sources: list[dict],
    url: str,
    name: str = "",
    *,
    source_type: str | None = None,
    city: str | None = None,
    country: str | None = None,
    discovery_origin: str = "seed",
    metadata_extra: dict | None = None,
    dedup_by_domain: bool = False,
) -> bool:
    """
    If url (normalized) not already in sources, append one source dict. Return True if inserted.
    dedup_by_domain: if True, skip when another source with same domain exists (keeps first/higher score).
    """
    url = (url or "").strip()
    if not url or not url.startswith(("http://", "https://")):
        return False
    norm = normalize_url(url)
    for s in sources:
        if normalize_url(s.get("url") or "") == norm:
            return False
        if dedup_by_domain and extract_domain(url) == (s.get("domain") or extract_domain(s.get("url") or "")):
            return False
    rec = make_source(
        norm,
        name=name,
        source_type=source_type,
        city=city,
        country=country,
        discovery_origin=discovery_origin,
        metadata_extra=metadata_extra,
    )
    rec["id"] = next_source_id(sources)
    sources.append(rec)
    return True


def dedup_sources(sources: list[dict], keep: str = "first") -> list[dict]:
    """
    Remove duplicates by normalized URL. Optionally by domain (keep one per domain).
    keep: "first" | "highest_score" — which source to keep when dup.
    Returns new list (does not mutate).
    """
    by_norm: dict[str, dict] = {}
    for s in sources:
        u = s.get("url") or ""
        if not u:
            continue
        n = normalize_url(u)
        existing = by_norm.get(n)
        if existing is None:
            by_norm[n] = dict(s)
            continue
        if keep == "highest_score":
            if (s.get("confidence_score") or 0) > (existing.get("confidence_score") or 0):
                by_norm[n] = dict(s)
    return list(by_norm.values())


def dedup_sources_in_file(path: Path | None = None, keep: str = "highest_score") -> int:
    """Load JSON, dedup by normalized URL, save. Returns number of sources after dedup."""
    p = path or DISCOVERY_JSON_PATH
    sources = load_discovery_sources_json(p)
    before = len(sources)
    deduped = dedup_sources(sources, keep=keep)
    save_discovery_sources_json(deduped, p)
    return len(deduped)


def rate_limit_sleep(seconds: float, simulation: bool = False) -> None:
    if simulation:
        return
    time.sleep(seconds)
