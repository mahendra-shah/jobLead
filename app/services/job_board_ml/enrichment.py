"""Rule-based enrichment for job_board rows (seniority, skills, taxonomy, education)."""

from __future__ import annotations

import re
from typing import Any

from app.services.job_board_sheets_service import JobBoardSheetsService

_DEGREE_PATTERNS = re.compile(
    r"\b("
    r"b\.?tech|b\.?e\.?|b\.e\.|m\.?tech|m\.?e\.|mba|b\.?com|m\.?com|b\.?sc|m\.?sc|"
    r"bachelor|master'?s?|ph\.?d|graduate|post[\s-]?graduate|"
    r"12th\s*pass|diploma|any\s+degree|b\.?a\.?|m\.?a\.?"
    r")\b",
    re.I,
)

# Lightweight skills lexicon for JD keyword scan (extend over time)
_SKILL_TOKENS = (
    "python",
    "javascript",
    "typescript",
    "react",
    "node.js",
    "nodejs",
    "sql",
    "salesforce",
    "excel",
    "seo",
    "sem",
    "ga4",
    "power bi",
    "figma",
    "photoshop",
    "linux",
    "mongodb",
    "postgresql",
    "html",
    "css",
)


def _extract_skills_from_text(text: str, existing: list[str] | str | None) -> list[str]:
    have: set[str] = set()
    if isinstance(existing, list):
        have.update(str(x).strip().lower() for x in existing if str(x).strip())
    elif isinstance(existing, str) and existing.strip():
        have.update(x.strip().lower() for x in existing.split(",") if x.strip())

    low = text.lower()
    found: list[str] = []
    for tok in _SKILL_TOKENS:
        if tok in low and tok not in have:
            found.append(tok.title() if tok not in ("c++", "sql", "seo", "sem", "ga4", "ios") else tok.upper())
            have.add(tok)
    return found


def _infer_degree(description: str, title: str) -> str:
    blob = f"{title} {description}"
    m = _DEGREE_PATTERNS.search(blob)
    return m.group(1).strip() if m else ""


def enrich_job_board_payload(job: dict[str, Any]) -> dict[str, Any]:
    """
    Returns fields to merge into Mongo `payload` (only non-empty overrides).
    Uses shared sheet helpers for segment/category/seniority consistency.
    """
    patch: dict[str, Any] = {}

    title = str(job.get("title") or "")
    desc = str(job.get("description") or "")
    domain = str(job.get("source_domain") or "")

    seg, cat = JobBoardSheetsService._classify_job(title, domain)
    cur_seg = str(job.get("segment") or "").strip()
    if cur_seg in ("", "Unknown") and seg and seg != "Unknown":
        patch["segment"] = seg
    cur_cat = str(job.get("category") or "").strip()
    if cur_cat in ("", "Other / Unknown") and cat and cat != "Other / Unknown":
        patch["category"] = cat

    cur_sen = str(job.get("seniority") or job.get("experience_required") or "").strip()
    seniority = JobBoardSheetsService._infer_seniority_value(title, desc, cur_sen, fallback=cur_sen)
    if seniority:
        low = cur_sen.lower()
        if not cur_sen or low in ("unknown", "n/a", "-", "not specified", "na"):
            patch["seniority"] = seniority

    blob = f"{title}\n{desc}"
    skills_existing = job.get("skills")
    if not skills_existing or (isinstance(skills_existing, list) and len(skills_existing) == 0):
        extra = _extract_skills_from_text(blob, skills_existing)
        if extra:
            patch["skills"] = extra

    if not str(job.get("degree") or job.get("education") or "").strip():
        deg = _infer_degree(desc, title)
        if deg:
            patch["degree"] = deg

    lt = str(job.get("location_type") or "").strip()
    if not lt:
        merged = {**job, **patch}
        loc = JobBoardSheetsService._derive_job_metadata(merged)
        # (location_type, location_detail, country, work_type, seniority_meta, salary, skills_meta, degree_meta)
        loc_type, loc_detail, country, work_type, _sm, _sal, _sk, deg_meta = loc
        if loc_type:
            patch["location_type"] = loc_type
        if loc_detail and not str(job.get("location_detail") or job.get("location") or "").strip():
            patch["location_detail"] = loc_detail
            if not job.get("location"):
                patch["location"] = loc_detail
        if country and not str(job.get("country") or "").strip():
            patch["country"] = country
        if work_type and not str(job.get("work_type") or "").strip():
            patch["work_type"] = work_type
        if deg_meta and "degree" not in patch:
            patch["degree"] = deg_meta

    return patch
