#!/usr/bin/env python3
"""
Assign jobs from jobs_master.json to fixed students (10 each), by skills + India filter.
Uses strict URL/title filters (real job posts only; no course / search listing URLs).
If fewer than 50 unique quality rows exist, remaining slots are filled with best-fit
duplicates (same URL may appear for another student) — flagged in JSON.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import re
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

JUNK_TITLE = re.compile(
    r"open jobs|jobs in |^\s*\d+\s*jobs|\(\d+\.?\d*k|\bjobs\)\s*$|view all jobs|^work from home$|^previous$|^next$",
    re.I,
)
SENIOR_TITLE = re.compile(
    r"\bsenior\b|\bstaff\b|\bprincipal\b|\blead\b|\bdirector\b|\bmanager\b|\bhead\b|\bvp\b|\bavp\b|\barchitect\b|"
    r"\bengineer\s+iii\b|\bengineer\s+ii\b|\banalyst\s+iii\b|\banalyst\s+ii\b|\bdeveloper\s+iii\b|"
    r"\biii\b|\bsde\s*2\b|\b2\s*-\s*5\s*yrs|workday\s+engineer\s*-\s*ii|\bengineer\s*-\s*ii\b",
    re.I,
)
INDIA = re.compile(
    r"\bindia\b|bengaluru|bangalore|hyderabad|mumbai|delhi|gurgaon|gurugram|noida|pune|chennai|kolkata|indian",
    re.I,
)

STUDENTS: dict[str, dict] = {
    "Tanuja Bisht": {
        "focus": "Data entry, operations, admin, Excel",
        "primary": re.compile(
            r"data\s*entr|data\s*analyst|operations|ops\b|admin|excel|back\s*office|field\s*collection|"
            r"collection\s*associate|clerical|documentation|coordinator|office|bpo|kpo|process\s*associate",
            re.I,
        ),
        "strong_title": re.compile(r"data\s*analyst|data\s*entr|collection|field\s*collection", re.I),
        "penalty": re.compile(
            r"software\s*engineer|backend|frontend|full\s*stack|sde\s|developer|java\s|python|soc\b|siem",
            re.I,
        ),
    },
    "Gayatri Sahu": {
        "focus": "CRM, customer support, sales ops",
        "primary": re.compile(
            r"crm|customer\s*support|helpdesk|relationship|client|sales|collection|telecaller|voice|"
            r"chat\s*support|moderator|associate",
            re.I,
        ),
        "strong_title": re.compile(r"collection|moderator|customer|support|relationship|sales", re.I),
        "penalty": re.compile(
            r"soc\b|siem\b|software\s*engineer|developer|sde\s|backend|frontend|full\s*stack|java\s|mysql|web\s*developer",
            re.I,
        ),
    },
    "Richa Yadav": {
        "focus": "Web: HTML/CSS/JS, software dev, analytics",
        "primary": re.compile(
            r"html|css|javascript|react|frontend|web\s*develop|software\s*engineer|developer|php|full\s*stack|"
            r"mysql|python|java\s|sde|junior|entry\s*level|trainee",
            re.I,
        ),
        "strong_title": re.compile(r"html|css|javascript|frontend|developer|software\s*engineer|web|junior|trainee", re.I),
        "penalty": re.compile(r"^field collection|soc\b|siem\b|moderator", re.I),
    },
    "Yogyata Dangwal": {
        "focus": "Digital marketing, content, social",
        "primary": re.compile(
            r"seo|sem|digital\s*marketing|content|social\s*media|marketing|growth|brand|copywriter",
            re.I,
        ),
        "strong_title": re.compile(r"marketing|content|moderator|social|seo|brand|growth", re.I),
        "penalty": re.compile(
            r"data\s*analyst|software\s*engineer|developer|sde\s|backend|frontend|full\s*stack|java\s|mysql|python\s*back|android",
            re.I,
        ),
    },
    "Mamta Udayan": {
        "focus": "Design, creative, UI, video",
        "primary": re.compile(
            r"graphic|design|figma|photoshop|video|creative|visual|ui|ux|multimedia|content\s*creator|web\s*developer",
            re.I,
        ),
        "strong_title": re.compile(r"graphic|design|ui|ux|video|creative|web\s*developer|frontend|php", re.I),
        "penalty": re.compile(r"soc\b|siem\b|software\s*engineer(?!\s*i\b)|java\s|mysql\s*only", re.I),
    },
}


def job_url(x: dict) -> str:
    return (x.get("apply_url") or x.get("url") or "").strip()


def blob(x: dict) -> str:
    return " ".join(
        [
            x.get("title") or "",
            x.get("description") or "",
            x.get("location") or "",
            x.get("location_detail") or "",
            x.get("category") or "",
            " ".join(x.get("skills") or []),
        ]
    )


def url_ok(url: str) -> bool:
    if not url.startswith("http"):
        return False
    if "trainings.internshala.com" in url or "placement-guarantee" in url:
        return False
    if "linkedin.com" in url and "/jobs/view/" not in url:
        return False
    if "ambitionbox.com" in url:
        if "jobs-in-" in url:
            return False
        if url.endswith("-cmp") and "rid=" not in url:
            return False
    return True


def india_ok(x: dict) -> bool:
    b = blob(x)
    if (x.get("country") or "").lower() == "india":
        return True
    return bool(INDIA.search(b))


def is_valid_candidate(x: dict) -> bool:
    t = (x.get("title") or "").strip()
    if len(t) < 6 or JUNK_TITLE.search(t) or SENIOR_TITLE.search(t):
        return False
    if not url_ok(job_url(x)):
        return False
    if not india_ok(x):
        return False
    return True


def job_key(x: dict) -> str:
    return hashlib.sha256(job_url(x).encode()).hexdigest()[:16]


def score(student_key: str, x: dict) -> int:
    cfg = STUDENTS[student_key]
    text = blob(x) + " " + (x.get("title") or "")
    title = x.get("title") or ""
    s = 0
    if cfg["primary"].search(text):
        s += 25
    if cfg["strong_title"].search(title):
        s += 15
    if cfg["penalty"].search(title):
        s -= 22
    if cfg["penalty"].search(text) and not cfg["primary"].search(title):
        s -= 8
    if re.search(r"fresher|intern|trainee|entry\s*level|junior|graduate|0\s*-\s*1", text, re.I):
        s += 4
    if re.search(r"remote|wfh|work from home", text, re.I):
        s += 2
    if (x.get("seniority") or "").strip() in ("Fresher / Entry", "Junior"):
        s += 3
    return s


def assign_jobs(candidates: list[dict], per_student: int = 10) -> dict[str, list[dict]]:
    # Score lists
    by_student: dict[str, list[tuple[int, dict]]] = {}
    for sk in STUDENTS:
        ranked: list[tuple[int, dict]] = []
        for x in candidates:
            sc = score(sk, x)
            ranked.append((sc, x))
        ranked.sort(key=lambda z: (-z[0], job_url(z[1])))
        by_student[sk] = ranked

    assigned: dict[str, list[dict]] = {sk: [] for sk in STUDENTS}
    used_keys: set[str] = set()

    def scarcity_order() -> list[str]:
        return sorted(
            STUDENTS.keys(),
            key=lambda sk: (
                sum(1 for sc, x in by_student[sk] if sc > 0 and job_key(x) not in used_keys),
                sk,
            ),
        )

    # Phase 1: unique jobs, rounds favour scarce students
    for _ in range(per_student):
        for sk in scarcity_order():
            if len(assigned[sk]) >= per_student:
                continue
            for sc, x in by_student[sk]:
                if sc < -3:
                    continue
                k = job_key(x)
                if k in used_keys:
                    continue
                assigned[sk].append(_row(x, sc, duplicate=False))
                used_keys.add(k)
                break

    # Phase 2: fill with duplicates (best remaining score)
    for sk in STUDENTS:
        need = per_student - len(assigned[sk])
        if need <= 0:
            continue
        for sc, x in by_student[sk]:
            if need <= 0:
                break
            if sc < -5:
                continue
            assigned[sk].append(_row(x, sc, duplicate=True))
            need -= 1

    # Dedupe URLs within each student list (order = best first)
    for sk in STUDENTS:
        seen: set[str] = set()
        deduped: list[dict] = []
        for r in assigned[sk]:
            u = r["url"]
            if u in seen:
                continue
            seen.add(u)
            deduped.append(r)
        assigned[sk] = deduped

    # Re-fill to per_student after dedupe (may add duplicates again)
    for sk in STUDENTS:
        need = per_student - len(assigned[sk])
        if need <= 0:
            continue
        have = {r["url"] for r in assigned[sk]}
        for sc, x in by_student[sk]:
            if need <= 0:
                break
            if sc < -5:
                continue
            row = _row(x, sc, duplicate=job_url(x) in have)
            assigned[sk].append(row)
            have.add(row["url"])
            need -= 1

    return assigned


def _row(x: dict, sc: int, duplicate: bool) -> dict:
    return {
        "title": (x.get("title") or "").replace("\n", " ").strip(),
        "company": x.get("company"),
        "location_type": x.get("location_type"),
        "country": x.get("country"),
        "category": x.get("category"),
        "seniority": x.get("seniority"),
        "url": job_url(x),
        "match_score": sc,
        "duplicate_url": duplicate,
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--master", type=Path, default=ROOT / "app/data/jobs/jobs_master.json")
    args = ap.parse_args()
    data = json.loads(args.master.read_text())
    jobs = data.get("jobs") or []
    candidates = [x for x in jobs if is_valid_candidate(x)]
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out = assign_jobs(candidates, 10)

    unique_urls = {r["url"] for rows in out.values() for r in rows}
    dup_count = sum(1 for rows in out.values() for r in rows if r.get("duplicate_url"))

    meta = {
        "generated_at_utc": ts,
        "source_master": str(args.master),
        "master_meta": data.get("meta"),
        "quality_candidates": len(candidates),
        "unique_urls_in_assignments": len(unique_urls),
        "duplicate_slots_filled": dup_count,
        "students": {k: v["focus"] for k, v in STUDENTS.items()},
        "note": "Real posting URLs only (no course/search pages). duplicate_url=true means same job link reused to reach 10 — OK to share but avoid duplicate applies across students when possible.",
    }

    jpath = ROOT / f"app/data/assignments_students_{ts}.json"
    jpath.write_text(json.dumps({"meta": meta, "assignments": out}, indent=2, ensure_ascii=False) + "\n")

    lines = [
        f"# Job assignments ({ts})",
        "",
        f"- Quality-filtered pool: **{len(candidates)}** India job postings with valid apply links.",
        f"- Unique URLs in this file: **{len(unique_urls)}**; **{dup_count}** rows are duplicate links (to reach 10 each).",
        "- **Ask students to verify** fresher eligibility, location, and remote/hybrid before applying.",
        "",
    ]
    for sk, rows in out.items():
        lines.append(f"## {sk}")
        lines.append(f"- *{STUDENTS[sk]['focus']}*")
        lines.append("")
        for n, r in enumerate(rows, 1):
            dup = " *(duplicate link)*" if r.get("duplicate_url") else ""
            lines.append(f"{n}. **{r['title']}** — {r.get('category') or '—'} | {r.get('location_type') or r.get('country') or '—'}{dup}")
            lines.append(f"   - {r['url']}")
        lines.append("")

    mpath = ROOT / f"app/data/assignments_students_{ts}.md"
    mpath.write_text("\n".join(lines))
    print(jpath)
    print(mpath)
    for sk, rows in out.items():
        print(sk, len(rows), "dups", sum(1 for r in rows if r.get("duplicate_url")))


if __name__ == "__main__":
    main()
