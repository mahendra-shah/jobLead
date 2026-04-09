"""
Deep profile filter using full title + company + description + location + experience text.

Targets: fresher-friendly (student) roles, remote/hybrid/WFH signals, India relevance (caller),
and at least one of: web/MERN-style stack (HTML/CSS/JS/React/Node/MERN…), digital marketing, or sales.

Tech is intentionally **not** generic “software engineer” (no broad engineering track).

This is rule-based (keyword + experience parsing), not a second sklearn model.
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Tuple

from app.utils.job_parser import parse_experience

_RE = re

# Remote / distributed work (India remote is OK)
_REMOTE_SIGNAL = _RE.compile(
    r"\b(?:"
    r"remote|wfh|work\s+from\s+home|hybrid|work\s+from\s+anywhere|"
    r"anywhere\s+in\s+india|distributed\s+team|telecommute|virtual\s+office"
    r")\b",
    _RE.IGNORECASE,
)

# Strong senior-only signals in description/title (reject for fresher pipeline)
_SENIOR_STRONG = _RE.compile(
    r"\b(?:"
    r"principal\s+engineer|distinguished\s+engineer|staff\s+engineer|engineering\s+manager|"
    r"director\s+of\s+engineering|vp\s+engineering|head\s+of\s+engineering|"
    r"\bcto\b|chief\s+technology|15\s*\+\s*years?|12\s*\+\s*years?|"
    r"10\s*\+\s*years?\s+(?:of\s+)?exp|minimum\s+8\s*years|minimum\s+10\s*years|"
    r"minimum\s+(?:[6-9]|[1-9]\d)\s*years?|at\s+least\s+(?:[6-9]|[1-9]\d)\s*years?"
    r")\b",
    _RE.IGNORECASE,
)

# "Minimum N years" / "At least N years" when N is above fresher cap (handled with cap in caller).
_MIN_YEARS_EXPLICIT = _RE.compile(
    r"\b(?:minimum|min\.?|at\s+least)\s*(\d{1,2})\s*\+?\s*years?\b",
    _RE.IGNORECASE,
)

# Tracks: job must match at least one — **only** DM, sales, or web/MERN stack (no generic “engineer” track).
_TRACKS: Dict[str, List[str]] = {
    "tech_web": [
        "developer",
        "web developer",
        "frontend developer",
        "full stack developer",
        "html",
        "css",
        "js",
        "javascript",
        "typescript",
        "react",
        "react.js",
        "node.js",
        "nodejs",
        "node js",
        "mern",
        "mern stack",
        "pern",
        "express.js",
        "express js",
        "mongodb",
        "mongo db",
        "full stack",
        "fullstack",
        "full-stack",
        "frontend",
        "front-end",
        "next.js",
        "vite",
        "tailwind",
        "python",
        "mongodb",
    ],
    "digital_marketing": [
        "digital marketing",
        "performance marketing",
        "seo",
        "sem",
        "google ads",
        "meta ads",
        "facebook ads",
        "social media marketing",
        "content marketing",
        "growth marketing",
        "marketing analytics",
        "social media executive",
        "social media manager",
        "ads specialist",
        "campaign manager",
        "smo",
        "ppc",
    ],
    "sales": [
        "sales",
        "business development",
        "bdr",
        "account executive",
        "sales executive",
        "inside sales",
        "field sales",
        "telesales",
        "telecaller",
        "lead generation",
        "customer acquisition",
    ],
    "non_tech_ops": [
        "crm",
        "customer relationship management",
        "customer support",
        "customer success",
        "hr",
        "human resources",
        "recruiter",
        "recruitment",
        "talent acquisition",
        "hr executive",
        "hr intern",
    ],
}

_ALLOWED_WORK_TYPES = _RE.compile(
    r"\b(?:internship|intern|part[-\s]*time|full[-\s]*time|freelance|freelancer|contract)\b",
    _RE.IGNORECASE,
)


def _norm_blob(payload: dict[str, Any]) -> str:
    """All text we use for depth parsing (description is primary signal)."""
    parts: List[str] = [
        str(payload.get("title") or ""),
        str(payload.get("company") or ""),
        str(payload.get("location") or payload.get("location_detail") or ""),
        str(payload.get("experience") or ""),
        str(payload.get("experience_raw") or ""),
        str(payload.get("experience_text") or ""),
    ]
    desc = str(payload.get("description") or "")
    if desc:
        parts.append(desc[:12000])
    return _RE.sub(r"\s+", " ", " ".join(p for p in parts if p)).strip()


def _track_hits(lower_text: str) -> Dict[str, List[str]]:
    """Return matched keywords per track (for ml_scores)."""
    out: Dict[str, List[str]] = {}
    for track, kws in _TRACKS.items():
        hits: List[str] = []
        for kw in kws:
            k = kw.lower()
            if " " in k or "-" in k or "." in k:
                if k in lower_text:
                    hits.append(kw)
            else:
                # Avoid matching inside longer tokens (e.g. scss vs css) via boundaries
                if _RE.search(rf"(?<![a-z0-9]){re.escape(k)}(?![a-z0-9])", lower_text):
                    hits.append(kw)
        if hits:
            out[track] = hits[:20]
    return out


def _explicit_min_years_exceeds_cap(blob: str, cap: float) -> bool:
    """True if free text says e.g. 'Minimum 3 years' and that is above fresher cap."""
    for m in _MIN_YEARS_EXPLICIT.finditer(blob):
        try:
            if float(m.group(1)) > float(cap):
                return True
        except (TypeError, ValueError):
            continue
    return False


def experience_fits_fresher_cap(full_text: str, max_years: float) -> Tuple[bool, Dict[str, Any]]:
    """
    Parse experience from full blob (incl. description). True if clearly within fresher cap.
    If nothing parseable, still reject when text explicitly asks for more years than cap.
    """
    parsed = parse_experience(full_text)
    detail: Dict[str, Any] = {
        "parsed_min": parsed.get("min"),
        "parsed_max": parsed.get("max"),
        "is_fresher_flag": parsed.get("is_fresher"),
    }
    min_v = parsed.get("min")
    max_v = parsed.get("max")

    def _f(v: Any) -> Optional[float]:
        if v is None:
            return None
        try:
            return float(v)
        except (TypeError, ValueError):
            return None

    mn = _f(min_v)
    mx = _f(max_v)
    cap = float(max_years)

    if _explicit_min_years_exceeds_cap(full_text, cap):
        detail["explicit_min_years_above_cap"] = True
        return False, detail

    if mn is None and mx is None:
        return True, detail

    if mx is not None and mx > cap:
        return False, detail
    if mn is not None and mn > cap:
        return False, detail
    return True, detail


def evaluate_depth_profile(
    payload: dict[str, Any],
    *,
    max_fresher_years: float,
    require_remote_signal: bool,
    require_role_track: bool,
) -> Tuple[bool, str, Dict[str, Any]]:
    """
    Returns (ok, reason_code, details_for_ml_scores).

    reason_code examples: ok, missing_title_or_company, no_remote_signal,
    no_target_role_track, experience_not_fresher, senior_role_excluded, unsupported_work_type.
    """
    title = str(payload.get("title") or "").strip()
    company = str(payload.get("company") or "").strip()

    if len(title) < 3:
        return False, "missing_or_short_title", {"title_len": len(title)}
    if len(company) < 2:
        return False, "missing_or_short_company", {"company_len": len(company)}

    blob = _norm_blob(payload)
    lower = blob.lower()

    details: Dict[str, Any] = {
        "depth_text_chars": len(blob),
        "tracks": {},
    }

    # Role tracks (any one)
    hits = _track_hits(lower)
    details["tracks"] = hits
    if require_role_track and not hits:
        return False, "no_target_role_track", details

    # Work type / opportunity style (student-focused).
    work_blob = " ".join(
        [
            str(payload.get("work_type") or ""),
            str(payload.get("employment_type") or ""),
            str(payload.get("location_type") or ""),
            lower,
        ]
    )
    work_ok = bool(_ALLOWED_WORK_TYPES.search(work_blob))
    details["allowed_work_type"] = work_ok
    if not work_ok:
        return False, "unsupported_work_type", details

    # Remote-country rule:
    # - India jobs: allow onsite/hybrid/remote (no remote requirement)
    # - Non-India jobs: require remote/WFH signal
    country = str(payload.get("country") or "").strip().lower()
    location_detail = str(payload.get("location_detail") or payload.get("location") or "").strip().lower()
    location_type = str(payload.get("location_type") or "").strip().lower()
    work_type = str(payload.get("work_type") or "").strip().lower()
    is_india = country in {"india", "in"} or ("india" in location_detail)
    remote_ok = bool(_REMOTE_SIGNAL.search(blob))
    has_remote = remote_ok or any(sig in location_type for sig in ("remote", "work from home", "wfh")) or any(
        sig in work_type for sig in ("remote", "work from home", "wfh")
    )
    details["remote_signal"] = bool(has_remote)
    details["is_india_job"] = bool(is_india)
    if require_remote_signal and (not is_india) and (not has_remote):
        return False, "non_india_without_remote_signal", details

    # Fresher / experience from full description
    ok_exp, exp_detail = experience_fits_fresher_cap(blob, max_fresher_years)
    details["experience"] = exp_detail
    if not ok_exp:
        return False, "experience_not_fresher", details

    # Senior-only postings (extra guard when description screams senior)
    if _SENIOR_STRONG.search(blob) and not _RE.search(
        r"\b(?:fresher|graduate|intern|0\s*-\s*2\s*yrs?|entry\s*level)\b", lower
    ):
        return False, "senior_role_excluded", details

    details["depth_ok"] = True
    return True, "ok", details
