"""Dedupe keys for job_ingest: URL normalization + composite hash across sources."""

from __future__ import annotations

import hashlib
import re
from typing import Any, Dict
from urllib.parse import urlparse, urlunparse


_URL_IN_TEXT_RE = re.compile(r"https?://[^\s<>'\"]+", re.IGNORECASE)
_LEADING_URL_NOISE = "*_`'\"([{<"
_TRAILING_URL_NOISE = "*_`'\"),.;:!?]}>"


def clean_url_candidate(url: str) -> str:
    """Remove markdown/punctuation noise around a URL candidate."""
    if not url or not isinstance(url, str):
        return ""

    u = url.strip()
    if not u:
        return ""

    # If URL is embedded in text like "****https://.../**", extract URL part first.
    m = _URL_IN_TEXT_RE.search(u)
    if m:
        u = m.group(0)

    while u and u[0] in _LEADING_URL_NOISE:
        u = u[1:]
    while u and u[-1] in _TRAILING_URL_NOISE:
        u = u[:-1]

    # Handle common markdown tail such as "/**" or "/__".
    u = re.sub(r"/[*_`~]+$", "/", u)
    # Strip any remaining trailing markdown markers.
    u = re.sub(r"[*_`~]+$", "", u)

    return u.strip()


def normalize_url(url: str) -> str:
    if not url or not isinstance(url, str):
        return ""
    u = clean_url_candidate(url)
    if not u:
        return ""
    u = u.lower()
    u = u.split("#", 1)[0]
    parsed = urlparse(u)
    # Strip query on common tracking params only — keep path
    path = (parsed.path or "").rstrip("/") or "/"
    netloc = (parsed.netloc or "").lower()
    if netloc.startswith("www."):
        netloc = netloc[4:]
    clean = urlunparse((parsed.scheme or "https", netloc, path, "", "", ""))
    return clean.rstrip("/") if clean.endswith("/") and len(path) > 1 else clean


_WS = re.compile(r"\s+")


def _norm_text(s: Any, max_len: int = 400) -> str:
    if s is None:
        return ""
    t = _WS.sub(" ", str(s).strip().lower())
    return t[:max_len]


def compute_dedupe_key(job: Dict[str, Any]) -> str:
    """
    Cross-board identity for ingest upsert.

    Primary key is normalized apply URL because title/company/location can drift
    across recrawls for the same posting. If URL is unavailable, fall back to a
    composite hash.
    """
    apply_u = normalize_url(
        str(job.get("apply_url") or job.get("url") or ""),
    )
    if apply_u:
        return hashlib.sha256(apply_u.encode("utf-8")).hexdigest()

    title = _norm_text(job.get("title"), 300)
    company = _norm_text(job.get("company"), 200)
    loc = _norm_text(
        job.get("location_detail") or job.get("location"),
        200,
    )
    blob = f"{title}|{company}|{loc}|{apply_u}"
    if blob.strip("|") == "":
        apply_u = normalize_url(str(job.get("url") or ""))
        blob = apply_u or "empty"
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def build_text_for_ml(job: Dict[str, Any]) -> str:
    parts = [
        job.get("title") or "",
        job.get("company") or "",
        job.get("location") or job.get("location_detail") or "",
        (job.get("description") or "")[:4000],
    ]
    return _WS.sub(" ", " ".join(str(p) for p in parts if p)).strip()
