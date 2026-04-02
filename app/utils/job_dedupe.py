"""Dedupe keys for job_ingest: URL-first + secondary identity hash."""

from __future__ import annotations

import hashlib
import re
from typing import Any, Dict
from urllib.parse import urlparse, urlunparse


def normalize_url(url: str) -> str:
    if not url or not isinstance(url, str):
        return ""
    u = url.strip()
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


def compute_primary_url_key(job: Dict[str, Any]) -> str:
    """Primary dedupe key from normalized apply URL (empty when URL missing)."""
    apply_u = normalize_url(str(job.get("apply_url") or job.get("url") or ""))
    if not apply_u:
        return ""
    return hashlib.sha256(apply_u.encode("utf-8")).hexdigest()


def compute_secondary_identity_key(job: Dict[str, Any]) -> str:
    """Secondary dedupe key from title+company+location when URL variants differ."""
    title = _norm_text(job.get("title"), 300)
    company = _norm_text(job.get("company"), 200)
    loc = _norm_text(
        job.get("location_detail") or job.get("location"),
        200,
    )
    blob = f"{title}|{company}|{loc}"
    if blob.strip("|") == "":
        apply_u = normalize_url(str(job.get("apply_url") or job.get("url") or ""))
        blob = apply_u or "empty"
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def compute_dedupe_key(job: Dict[str, Any]) -> str:
    """
    Backward-compatible alias used by existing pipeline code.
    Returns secondary identity key; primary URL key is handled separately.
    """
    return compute_secondary_identity_key(job)


# Long enough for classifier + India gate; aligned with Mongo payload trim cap.
_ML_DESCRIPTION_MAX_CHARS = 32000


def build_text_for_ml(job: Dict[str, Any]) -> str:
    desc = job.get("description") or ""
    if isinstance(desc, str) and len(desc) > _ML_DESCRIPTION_MAX_CHARS:
        desc = desc[:_ML_DESCRIPTION_MAX_CHARS]
    parts = [
        job.get("title") or "",
        job.get("company") or "",
        job.get("location") or job.get("location_detail") or "",
        desc,
    ]
    return _WS.sub(" ", " ".join(str(p) for p in parts if p)).strip()
