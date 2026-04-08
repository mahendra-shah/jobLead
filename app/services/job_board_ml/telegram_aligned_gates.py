"""
Gates aligned with ``MLProcessorService`` Telegram flow: spam regex prefilter,
apply/contact checks, fresher experience cap, and payload shape checks.

Kept separate from ``ml_processor_service`` so job-board ingest can share rules
without importing the Telegram processor.
"""

from __future__ import annotations

import re
from typing import Any, Optional

from app.utils.job_parser import parse_experience

# ── Copied from ml_processor_service: non-job content patterns ───────────────
_RE = re

_NON_JOB_PATTERNS = [
    (
        "crypto_scam",
        _RE.compile(
            r"\b(?:USDT|bitcoin|BTC|ETH|ethereum|crypto\s+earn|earn\s+USDT"
            r"|\d+\s*USDT\s*=|buy\s+USDT|sell\s+USDT"
            r"|IMPS.*UPI.*(?:rupee|INR|RS)"
            r")\b",
            _RE.IGNORECASE,
        ),
    ),
    (
        "interview_coaching",
        _RE.compile(
            r"\b(?:job\s+support|interview\s+support|interview\s+preparation\s+service"
            r"|mock\s+interview|interview\s+coaching|interview\s+assist"
            r"|we\s+provide\s+structured\s+interview"
            r"|training\s+support\s+for\s+IT\s+professionals"
            r")\b",
            _RE.IGNORECASE,
        ),
    ),
    (
        "social_media_promo",
        _RE.compile(
            r"(?:youtube|instagram|telegram)\s+chann?el.*task"
            r"|promote.*chann?el"
            r"|online.*youtube.*task"
            r"|earn.*(?:like|subscribe|view|share)",
            _RE.IGNORECASE,
        ),
    ),
    (
        "supplier_spam",
        _RE.compile(
            r"24\s*\*\s*365.*(?:all.weather|supplier|work)"
            r"|reliable.*supplier.*(?:earn|income)"
            r"|IMPS|UPI.*bank\s+card",
            _RE.IGNORECASE,
        ),
    ),
]


def is_non_job_spam(text: str) -> Optional[str]:
    """Return label if ``text`` matches a non-job spam pattern, else ``None``."""
    if not text:
        return None
    for label, pattern in _NON_JOB_PATTERNS:
        if pattern.search(text):
            return label
    return None


def has_obfuscated_email(text: str) -> bool:
    """Detect ``name at domain dot com`` / ``[at]`` style emails."""
    patterns = [
        r"[A-Za-z0-9._%+-]+\s*(?:\[at\]|\(at\)|at)\s*[A-Za-z0-9.-]+\s*(?:\[dot\]|\(dot\)|dot|\.)\s*[A-Za-z]{2,}",
    ]
    for p in patterns:
        if _RE.search(p, text, flags=_RE.IGNORECASE):
            return True
    return False


def payload_has_apply_contact(payload: dict[str, Any], full_text: str) -> bool:
    """True if there is an http(s) apply URL or a plain/obfuscated email in text."""
    u = str(payload.get("apply_url") or payload.get("url") or "").strip()
    if u.startswith("http://") or u.startswith("https://"):
        return True
    if _RE.search(
        r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}",
        full_text or "",
    ):
        return True
    return has_obfuscated_email(full_text or "")


def board_requires_title_or_company(payload: dict[str, Any]) -> tuple[bool, str]:
    """Require at least job title or company (legacy)."""
    title = str(payload.get("title") or "").strip()
    company = str(payload.get("company") or "").strip()
    if not title and not company:
        return False, "missing_title_and_company"
    return True, ""


def board_requires_title_and_company(payload: dict[str, Any]) -> tuple[bool, str]:
    """Require both non-empty title and company (job board product rule)."""
    title = str(payload.get("title") or "").strip()
    company = str(payload.get("company") or "").strip()
    if len(title) < 3:
        return False, "missing_or_short_title"
    if len(company) < 2:
        return False, "missing_or_short_company"
    return True, ""


def _experience_blob(payload: dict[str, Any]) -> str:
    parts = [
        payload.get("experience"),
        payload.get("experience_raw"),
        payload.get("experience_text"),
    ]
    if payload.get("experience_min") is not None:
        parts.append(str(payload.get("experience_min")))
    return " ".join(str(p) for p in parts if p not in (None, "")).strip()


def experience_exceeds_fresher_cap(payload: dict[str, Any], max_years: int) -> bool:
    """
    True when parsed experience min/max clearly exceeds ``max_years`` (student/fresher cap).
    If experience is missing/unparseable, returns False (let quality scorer handle).
    """
    blob = _experience_blob(payload)
    if not blob:
        return False
    parsed = parse_experience(blob)
    min_exp = parsed.get("min")
    max_exp = parsed.get("max")

    def _as_float(v: Any) -> Optional[float]:
        if v is None:
            return None
        try:
            return float(v)
        except (TypeError, ValueError):
            return None

    min_val = _as_float(min_exp)
    max_val = _as_float(max_exp)
    cap = float(max_years)

    if max_val is not None and max_val > cap:
        return True
    if min_val is not None and min_val > cap:
        return True
    return False


def build_job_data_for_quality_scorer(
    payload: dict[str, Any], max_fresher_years: int
) -> dict[str, Any]:
    exp = (
        payload.get("experience")
        or payload.get("experience_raw")
        or _experience_blob(payload)
        or None
    )
    parsed = parse_experience(str(exp)) if exp else {"min": None, "max": None, "is_fresher": False}
    skills = payload.get("skills_required") or payload.get("skills") or []
    if isinstance(skills, str):
        skills = [s.strip() for s in skills.split(",") if s.strip()]
    apply_u = str(payload.get("apply_url") or payload.get("url") or "").strip()
    max_v = parsed.get("max")
    try:
        max_f = float(max_v) if max_v is not None else None
    except (TypeError, ValueError):
        max_f = None
    is_fresher_flag = bool(parsed.get("is_fresher")) or (
        max_f is not None and max_f <= float(max_fresher_years)
    )
    return {
        "title": payload.get("title"),
        "description": (payload.get("description") or "")[:8000],
        "skills": skills,
        "skills_required": skills,
        "experience": exp,
        "is_fresher": is_fresher_flag,
        "work_type": payload.get("work_type"),
        "salary": payload.get("salary") or payload.get("salary_raw"),
        "source_url": apply_u,
        "apply_link": apply_u,
        "company": payload.get("company"),
        "location": payload.get("location") or payload.get("location_detail"),
        "location_data": payload.get("location_data") or {},
    }
