"""Google Sheets service for exporting Phase 1/2 job-board data.

This is intentionally similar to `GoogleSheetsService` but operates on a
separate Sheet (JOB_BOARD_SHEET_ID) and works with the JSON artifacts
produced by the discovery/crawling pipeline:

- discovery_sources_test.json  →  <date>_sources tab
- jobs/jobs_master.json        →  <date>_jobs tab
"""

import json
import logging
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple
from urllib.parse import urlparse

import httpx
from google.oauth2 import service_account
from googleapiclient.discovery import build
from sqlalchemy.orm import Session  # kept for future DB-based exports
from sqlalchemy import and_, desc, or_, select

import pytz

from app.config import settings
from app.models.job import Job
from app.utils.job_dedupe import normalize_url
from app.utils.timezone import IST, ist_today_utc_window

logger = logging.getLogger(__name__)


class JobBoardSheetsService:
    """Export Phase 1/2 discovery + job-board data to Google Sheets."""

    def __init__(self) -> None:
        if not settings.JOB_BOARD_SHEET_ID:
            raise ValueError("JOB_BOARD_SHEET_ID is not configured in settings/.env")

        self.sheet_id = settings.JOB_BOARD_SHEET_ID
        # Reuse the existing service-account credentials file
        self.credentials_path = (
            Path(__file__).parent.parent.parent / "credentials.json"
        )

        scopes = ["https://www.googleapis.com/auth/spreadsheets"]
        self.credentials = service_account.Credentials.from_service_account_file(
            str(self.credentials_path),
            scopes=scopes,
        )
        service = build("sheets", "v4", credentials=self.credentials)
        self.sheets = service.spreadsheets()

    # ── Column width presets (pixels): clear, aligned, readable ──────────────────
    SOURCE_COLUMN_WIDTHS = [50, 180, 140, 90, 80, 80, 70, 60, 100, 100, 90, 50, 200, 60, 90]
    JOB_COLUMN_WIDTHS = [
        130,  # Segment
        150,  # Category
        220,  # Job Title
        180,  # Company
        110,  # Location Type
        200,  # Location Detail
        120,  # Country
        120,  # Work Type
        130,  # Seniority
        130,  # Salary
        220,  # Skills
        160,  # Degree
        260,  # Job Description (short)
        220,  # Apply URL
        140,  # Source Domain
        140,  # Source Discovered Date
        130,  # Job Posted At (raw)
        150,  # Date & time (India)
        130,  # Crawled At (UTC)
    ]

    # ── Internal helpers ──────────────────────────────────────────────────────

    def _ensure_tab_with_headers(
        self, tab_name: str, headers: List[str], column_widths: Optional[List[int]] = None
    ) -> Optional[int]:
        """Create (or reuse) a tab, header row, and formatting. Returns sheet_id for data formatting."""
        sheet_metadata = self.sheets.get(spreadsheetId=self.sheet_id).execute()
        sheets = sheet_metadata.get("sheets", [])
        existing_sheets = {s["properties"]["title"] for s in sheets}
        sheet_id: Optional[int] = None

        if tab_name not in existing_sheets:
            logger.info("Creating job-board tab '%s'", tab_name)
            resp = self.sheets.batchUpdate(
                spreadsheetId=self.sheet_id,
                body={
                    "requests": [
                        {
                            "addSheet": {
                                "properties": {
                                    "title": tab_name,
                                    "gridProperties": {
                                        "rowCount": 5000,
                                        "columnCount": 26,
                                    },
                                }
                            }
                        }
                    ]
                },
            ).execute()
            added = resp["replies"][0]["addSheet"]["properties"]
            sheet_id = added["sheetId"]
        else:
            for s in sheets:
                if s["properties"]["title"] == tab_name:
                    sheet_id = s["properties"]["sheetId"]
                    break

        body = {"values": [headers]}
        end_col = chr(ord("A") + len(headers) - 1)
        self.sheets.values().update(
            spreadsheetId=self.sheet_id,
            range=f"{tab_name}!A1:{end_col}1",
            valueInputOption="RAW",
            body=body,
        ).execute()

        if sheet_id is not None:
            widths = column_widths if column_widths and len(column_widths) >= len(headers) else None
            requests = [
                {
                    "repeatCell": {
                        "range": {"sheetId": sheet_id, "startRowIndex": 0, "endRowIndex": 1},
                        "cell": {
                            "userEnteredFormat": {
                                "backgroundColor": {"red": 0.85, "green": 0.88, "blue": 0.92},
                                "textFormat": {"bold": True},
                            }
                        },
                        "fields": "userEnteredFormat(backgroundColor,textFormat)",
                    }
                },
                {
                    "updateSheetProperties": {
                        "properties": {"sheetId": sheet_id, "gridProperties": {"frozenRowCount": 1}},
                        "fields": "gridProperties.frozenRowCount",
                    }
                },
            ]
            if widths:
                for i, w in enumerate(widths):
                    if i >= len(headers):
                        break
                    requests.append({
                        "updateDimensionProperties": {
                            "range": {
                                "sheetId": sheet_id,
                                "dimension": "COLUMNS",
                                "startIndex": i,
                                "endIndex": i + 1,
                            },
                            "properties": {"pixelSize": w},
                            "fields": "pixelSize",
                        }
                    })
            else:
                requests.append({
                    "updateDimensionProperties": {
                        "range": {
                            "sheetId": sheet_id,
                            "dimension": "COLUMNS",
                            "startIndex": 0,
                            "endIndex": len(headers),
                        },
                        "properties": {"pixelSize": 180},
                        "fields": "pixelSize",
                    }
                })
            self.sheets.batchUpdate(spreadsheetId=self.sheet_id, body={"requests": requests}).execute()
        return sheet_id

    def _format_data_cells(
        self,
        tab_name: str,
        sheet_id: int,
        num_cols: int,
        num_rows: int,
        *,
        data_start_row_0based: int = 1,
    ) -> None:
        """Apply text wrap and left alignment to data area for readability."""
        if num_rows == 0:
            return
        requests = [
            {
                "repeatCell": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": data_start_row_0based,
                        "endRowIndex": data_start_row_0based + num_rows,
                        "startColumnIndex": 0,
                        "endColumnIndex": num_cols,
                    },
                    "cell": {
                        "userEnteredFormat": {
                            "wrapStrategy": "WRAP",
                            "horizontalAlignment": "LEFT",
                            "verticalAlignment": "TOP",
                        }
                    },
                    "fields": "userEnteredFormat(wrapStrategy,horizontalAlignment,verticalAlignment)",
                }
            }
        ]
        self.sheets.batchUpdate(spreadsheetId=self.sheet_id, body={"requests": requests}).execute()

    def _clear_data_rows(self, tab_name: str, num_cols: int = 26) -> None:
        """Clear all rows except the header (row 1)."""
        try:
            end_col = chr(ord("A") + min(max(num_cols - 1, 0), 25))
            self.sheets.values().clear(
                spreadsheetId=self.sheet_id,
                range=f"{tab_name}!A2:{end_col}100000",
            ).execute()
        except Exception as exc:  # pragma: no cover - defensive
            logger.warning("Could not clear rows for tab '%s': %s", tab_name, exc)

    def _next_append_row_1based(self, tab_name: str) -> int:
        """First empty row below existing content in column A (1-based). Assumes row 1 is header."""
        res = (
            self.sheets.values()
            .get(spreadsheetId=self.sheet_id, range=f"{tab_name}!A:A")
            .execute()
        )
        vals = res.get("values") or []
        return len(vals) + 1

    def _default_ist_date_str(self) -> str:
        _, _, ist_date_str = ist_today_utc_window()
        return ist_date_str

    @staticmethod
    def _job_row_key(row: List[str]) -> str:
        """
        Stable dedupe key for job rows written to sheet.
        Prefer canonical apply URL; fallback to title+company+location+source_domain.
        """
        apply_url = normalize_url(str(row[13] or "").strip()) if len(row) > 13 else ""
        if apply_url:
            return f"url:{apply_url}"
        title = str(row[2] or "").strip().lower() if len(row) > 2 else ""
        company = str(row[3] or "").strip().lower() if len(row) > 3 else ""
        location = str(row[5] or "").strip().lower() if len(row) > 5 else ""
        source_domain = str(row[14] or "").strip().lower() if len(row) > 14 else ""
        return f"fallback:{title}|{company}|{location}|{source_domain}"

    def _existing_job_row_keys(self, tab_name: str, num_cols: int) -> set[str]:
        """Read current sheet rows and return dedupe keys for existing jobs."""
        end_col = chr(ord("A") + min(max(num_cols - 1, 0), 25))
        res = (
            self.sheets.values()
            .get(spreadsheetId=self.sheet_id, range=f"{tab_name}!A2:{end_col}")
            .execute()
        )
        vals = res.get("values") or []
        out: set[str] = set()
        for raw in vals:
            row = [str(x) if x is not None else "" for x in raw]
            if len(row) < num_cols:
                row.extend([""] * (num_cols - len(row)))
            out.add(self._job_row_key(row))
        return out

    # ── Classification helpers ────────────────────────────────────────────────

    @staticmethod
    def _classify_job(title: str, source_domain: str, description: str = "") -> Tuple[str, str]:
        """Classify job as Tech / Non-tech + category from title, description, and domain."""
        t = f"{(title or '')} {(description or '')}".lower()
        domain = (source_domain or "").lower()

        tech_keywords = [
            "developer",
            "engineer",
            "software",
            "backend",
            "frontend",
            "full stack",
            "full-stack",
            "data scientist",
            "data engineer",
            "ml engineer",
            "ai engineer",
            "devops",
            "sre",
            "qa engineer",
            "ios developer",
            "android developer",
            "mobile developer",
            "platform engineer",
            "site reliability",
        ]
        sales_keywords = ["sales", "account executive", "business development", "bdm"]
        marketing_keywords = [
            "marketing",
            "digital marketing",
            "growth",
            "seo",
            "sem",
            "smo",
            "content",
            "performance",
            "ppc",
            "google ads",
            "meta ads",
            "social media marketing",
            "brand",
            "copywriter",
            "copywriting",
        ]
        support_keywords = ["customer support", "customer success", "support specialist"]
        hr_keywords = ["hr ", "talent acquisition", "recruiter", "recruitment"]
        finance_keywords = ["finance", "accountant", "controller", "fp&a", "audit"]
        product_keywords = ["product manager", "product owner"]
        design_keywords = ["designer", "ux", "ui", "product design", "graphic design"]
        data_keywords = [
            "data analyst",
            "data analytics",
            "data analysis",
            "data manager",
            "data entry",
            "business analyst",
        ]

        def any_kw(kws: List[str]) -> bool:
            return any(kw in t for kw in kws)

        # Category
        if any_kw(tech_keywords):
            segment = "Tech"
            category = "Software / Engineering"
        elif any_kw(data_keywords):
            segment = "Non-tech"
            category = "Data / Analytics"
        elif any_kw(product_keywords):
            segment = "Non-tech"
            category = "Product Management"
        elif any_kw(design_keywords):
            segment = "Tech"
            category = "Design / UX"
        elif any_kw(sales_keywords):
            segment = "Non-tech"
            category = "Sales"
        elif any_kw(marketing_keywords):
            segment = "Non-tech"
            category = "Marketing / Growth"
        elif any_kw(support_keywords):
            segment = "Non-tech"
            category = "Customer Support / Success"
        elif any_kw(hr_keywords):
            segment = "Non-tech"
            category = "HR / Talent"
        elif any_kw(finance_keywords):
            segment = "Non-tech"
            category = "Finance / Accounting"
        else:
            # Fallback: if domain clearly tech-focused, bias towards Tech
            if any(d in domain for d in ["github", "remoteintech", "stackoverflow"]):
                segment = "Tech"
                category = "Other / Unknown"
            else:
                tech_signals = (
                    "sql",
                    "javascript",
                    "python",
                    "api",
                    "typescript",
                )
                nontech_signals = (
                    "bpo",
                    "kpo",
                    "telecaller",
                    "voice process",
                    "data entry",
                    "back office",
                )
                th = sum(1 for s in tech_signals if s in t)
                nh = sum(1 for s in nontech_signals if s in t)
                if th > nh:
                    segment = "Tech"
                else:
                    segment = "Non-tech"
                category = "Other / Unknown"

        return segment, category

    @staticmethod
    def _apply_url_likely_valid(url: str) -> bool:
        """HEAD/GET check; on network errors assume valid to avoid dropping rows on transient failures."""
        u = (url or "").strip()
        if not u.startswith("http"):
            return False
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (compatible; PlacementJobBoardExporter/1.0; +https://example.invalid)"
            )
        }
        try:
            with httpx.Client(timeout=12.0, follow_redirects=True, headers=headers) as client:
                r = client.head(u)
                if r.status_code in (404, 410):
                    return False
                if r.status_code in (405, 501):
                    r = client.get(u)
                    return r.status_code not in (404, 410)
                if r.status_code in (401, 403, 429):
                    return True
                return r.status_code < 500
        except Exception:
            return True

    @classmethod
    def _invalid_apply_urls(cls, urls: List[str], *, max_workers: int = 6) -> Set[str]:
        uniq = sorted({normalize_url(x) for x in urls if x and x.startswith("http")})
        bad: Set[str] = set()
        if not uniq:
            return bad
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = {pool.submit(cls._apply_url_likely_valid, u): u for u in uniq}
            for fut in as_completed(futures):
                original = futures[fut]
                try:
                    if not fut.result():
                        bad.add(normalize_url(original))
                except Exception:
                    pass
        return bad

    @staticmethod
    def _crawled_at_ist_simple(utc_str: str) -> str:
        """Format crawled_at_utc (ISO) as India date and time, e.g. '16 March, 11:20 am'."""
        if not utc_str or not isinstance(utc_str, str):
            return ""
        s = utc_str.strip().replace("Z", "+00:00")
        try:
            dt = datetime.fromisoformat(s)
            if dt.tzinfo is None:
                dt = pytz.utc.localize(dt)
            ist_dt = dt.astimezone(IST)
            # e.g. "16 March, 11:20 am"
            out = ist_dt.strftime("%d %B, %I:%M %p")
            return out.replace("AM", "am").replace("PM", "pm")
        except Exception:
            return utc_str

    @staticmethod
    def _derive_job_metadata(job: dict) -> Tuple[str, str, str, str, str, str, str, str]:
        """Derive location type, location detail, country, work type, seniority, salary, skills, degree."""
        title = (job.get("title") or "").lower()
        location = (job.get("location") or "").strip()
        desc = (job.get("description") or "").lower()

        loc_combined = " ".join([location, desc])
        if "remote" in loc_combined:
            location_type = "Remote"
        elif "hybrid" in loc_combined:
            location_type = "Hybrid"
        elif location:
            location_type = "Onsite"
        else:
            location_type = ""

        location_detail = location
        country = ""
        for c in ["india", "usa", "united states", "uk", "germany", "canada", "australia"]:
            if c in loc_combined:
                country = c.title()
                break
        if not country and (
            re.search(r"\bindia\b", loc_combined)
            or re.search(
                r"\b(bangalore|bengaluru|hyderabad|chennai|mumbai|delhi|ncr|pune|kolkata"
                r"|noida|gurgaon|gurugram|ahmedabad|kochi|coimbatore|indore|jaipur)\b",
                loc_combined,
            )
        ):
            country = "India"

        work_type = ""
        if any(w in title for w in ["intern", "internship"]):
            work_type = "Internship"
        elif "part-time" in desc or "part time" in desc:
            work_type = "Part-time"
        elif "contract" in desc:
            work_type = "Contract"
        elif "full-time" in desc or "full time" in desc:
            work_type = "Full-time"

        seniority = ""
        if any(w in title for w in ["intern", "fresher", "graduate", "entry level", "entry-level", "trainee"]):
            seniority = "Fresher / Entry"
        elif "junior" in title:
            seniority = "Junior"
        elif "senior" in title or "lead" in title:
            seniority = "Senior"
        if not seniority and re.search(
            r"\b(fresher|fresh graduate|entry level|0\s*-\s*1\s*yr|walk-?in)\b",
            loc_combined,
        ):
            seniority = "Fresher / Entry"
        if not seniority:
            seniority = "Fresher / Entry"

        salary = job.get("salary") or job.get("salary_text") or ""
        skills = ""
        raw_skills = job.get("skills")
        if isinstance(raw_skills, list):
            skills = ", ".join(str(s) for s in raw_skills if s)
        elif isinstance(raw_skills, str) and raw_skills.strip():
            skills = raw_skills.strip()
        if not skills and desc:
            m = re.search(r"(?:key\s+)?skills?[:\s\-]+([^.;\n]{10,400})", desc, re.I)
            if m:
                chunk = m.group(1)
                parts = [x.strip() for x in re.split(r"[,|•\n]", chunk) if len(x.strip()) > 2]
                skills = ", ".join(parts[:20])
        degree = job.get("degree") or job.get("education") or ""
        if not degree and desc:
            dm = re.search(
                r"(b\.?tech|bachelor['’]s?|bsc|b\.sc|mca|m\.tech|master['’]s?|b\.e\.|mba|bba|any graduate)",
                desc,
                re.IGNORECASE,
            )
            if dm:
                degree = dm.group(0).strip()

        return location_type, location_detail, country, work_type, seniority, salary, skills, degree
    # ── Public API: JSON exports ──────────────────────────────────────────────

    def export_sources_from_json(
        self, json_path: Path, date_str: Optional[str] = None
    ) -> Dict:
        """Export discovery sources from JSON to a single 'sources' tab (no per-day tabs)."""
        # We keep one canonical sources tab that is refreshed every time.
        if not date_str:
            date_str = self._default_ist_date_str()
        tab_name = "sources"

        with open(json_path, encoding="utf-8") as f:
            payload = json.load(f)

        sources = payload.get("sources") or []
        if not sources:
            logger.info("No sources found in %s", json_path)
            return {
                "status": "no_sources",
                "date": date_str,
                "tab_name": tab_name,
                "sources_exported": 0,
            }

        headers = [
            "ID",
            "Name",
            "Domain",
            "Type",
            "City",
            "Country",
            "Status",
            "Confidence Score",
            "First Seen",
            "Last Checked",
            "Discovered Date",
            "Job Page Detected",
            "Sample Job Page URL",
            "Crawl Ready",
            "Discovery Origin",
        ]
        sheet_id = self._ensure_tab_with_headers(tab_name, headers, self.SOURCE_COLUMN_WIDTHS)
        self._clear_data_rows(tab_name)

        rows: List[List[str]] = []
        for src in sources:
            meta = src.get("metadata") or {}
            crawl = meta.get("crawl_strategy") or {}
            job_page_urls = meta.get("job_page_urls") or []

            rows.append(
                [
                    src.get("id"),
                    src.get("name") or "",
                    src.get("domain") or "",
                    src.get("type") or "",
                    src.get("city") or "",
                    src.get("country") or "",
                    src.get("status") or "",
                    meta.get("confidence_score", src.get("confidence_score", "")),
                    src.get("first_seen") or "",
                    src.get("last_checked") or "",
                    meta.get("discovered_date") or "",
                    str(meta.get("job_page_detected") or False),
                    (job_page_urls[0] if job_page_urls else ""),
                    str(crawl.get("crawl_ready") or False),
                    meta.get("discovery_origin") or "",
                ]
            )

        end_col = chr(ord("A") + len(headers) - 1)
        range_name = f"{tab_name}!A2:{end_col}{1 + len(rows)}"
        self.sheets.values().update(
            spreadsheetId=self.sheet_id,
            range=range_name,
            valueInputOption="RAW",
            body={"values": rows},
        ).execute()
        if sheet_id is not None:
            self._format_data_cells(tab_name, sheet_id, len(headers), len(rows))

        logger.info("Exported %d sources to '%s'", len(rows), tab_name)
        return {
            "status": "success",
            "date": date_str,
            "tab_name": tab_name,
            "sources_exported": len(rows),
        }

    def export_jobs_from_json(
        self,
        json_path: Path,
        date_str: Optional[str] = None,
        *,
        append: bool = False,
        validate_apply_urls: bool = True,
    ) -> Dict:
        """Export crawled jobs from JSON to a <date>_jobs tab.

        If append=False (default), existing data rows are cleared and replaced (full refresh).
        If append=True, new rows are written below existing data so the same IST date tab
        accumulates all verified/export batches for that day without overwriting.
        If validate_apply_urls=True, rows whose apply URL returns 404/410 are skipped.
        """
        if not date_str:
            date_str = self._default_ist_date_str()
        tab_name = f"{date_str}_jobs"

        with open(json_path, encoding="utf-8") as f:
            payload = json.load(f)

        jobs = payload.get("jobs") or []
        if not jobs:
            logger.info("No jobs found in %s", json_path)
            return {
                "status": "no_jobs",
                "date": date_str,
                "tab_name": tab_name,
                "jobs_exported": 0,
                "append": append,
                "apply_urls_skipped": 0,
            }

        invalid_apply: Set[str] = set()
        if validate_apply_urls:
            candidates = [str(job.get("apply_url") or job.get("url") or "") for job in jobs]
            invalid_apply = self._invalid_apply_urls(candidates)
            if invalid_apply:
                logger.info(
                    "Skipping %d jobs with unreachable apply URLs (404/410)",
                    sum(
                        1
                        for job in jobs
                        if normalize_url(str(job.get("apply_url") or job.get("url") or ""))
                        in invalid_apply
                    ),
                )

        headers = [
            "Segment (Tech / Non-tech)",
            "Category",
            "Job Title",
            "Company",
            "Location Type",
            "Location Detail",
            "Country",
            "Work Type",
            "Seniority Level",
            "Salary",
            "Skills",
            "Degree / Education",
            "Job Description (short)",
            "Apply URL",
            "Source Domain",
            "Source Discovered Date",
            "Job Posted At (raw)",
            "Date & time (India)",
            "Crawled At (UTC)",
        ]
        sheet_id = self._ensure_tab_with_headers(tab_name, headers, self.JOB_COLUMN_WIDTHS)
        num_cols = len(headers)
        if not append:
            self._clear_data_rows(tab_name, num_cols=num_cols)
            start_row_1based = 2
            seen_keys: set[str] = set()
        else:
            start_row_1based = self._next_append_row_1based(tab_name)
            if start_row_1based < 2:
                start_row_1based = 2
            seen_keys = self._existing_job_row_keys(tab_name, num_cols)

        rows: List[List[str]] = []
        skipped_bad_url = 0
        for job in jobs:
            title = job.get("title") or ""
            source_domain = job.get("source_domain") or ""
            desc_full = (job.get("description") or job.get("raw_text") or "")[:8000]
            segment, category = self._classify_job(title, source_domain, desc_full)
            meta_job = dict(job)
            meta_job["description"] = desc_full
            lt, ld, co, wt, sr, sal_der, sk_der, deg_der = self._derive_job_metadata(meta_job)
            location_type = job.get("location_type") or lt
            location_detail = job.get("location_detail") or job.get("location") or ld
            country = job.get("country") or co
            work_type = job.get("work_type") or wt
            seniority = (job.get("seniority") or "").strip() or sr
            salary = job.get("salary") or sal_der
            degree = job.get("degree") or deg_der
            skills_val = job.get("skills")
            if isinstance(skills_val, list):
                skills = ", ".join(str(s) for s in skills_val) if skills_val else sk_der
            else:
                skills = (skills_val or sk_der) if isinstance(skills_val, str) else sk_der
            description = desc_full[:240]
            apply_url = job.get("apply_url") or job.get("url") or ""
            if validate_apply_urls and invalid_apply:
                if normalize_url(str(apply_url)) in invalid_apply:
                    skipped_bad_url += 1
                    continue

            row = [
                segment,
                category,
                title,
                job.get("company") or "",
                location_type,
                location_detail,
                country,
                work_type,
                seniority,
                salary,
                skills,
                degree,
                description,
                apply_url,
                source_domain,
                job.get("source_discovered_date") or "",
                job.get("job_posted_at_raw") or "",
                self._crawled_at_ist_simple(job.get("crawled_at_utc") or ""),
                job.get("crawled_at_utc") or "",
            ]
            key = self._job_row_key(row)
            if key in seen_keys:
                continue
            seen_keys.add(key)
            rows.append(row)

        if not rows:
            return {
                "status": "no_new_jobs",
                "date": date_str,
                "tab_name": tab_name,
                "jobs_exported": 0,
                "append": append,
                "apply_urls_skipped": skipped_bad_url,
            }

        end_col = chr(ord("A") + len(headers) - 1)
        end_row = start_row_1based + len(rows) - 1
        range_name = f"{tab_name}!A{start_row_1based}:{end_col}{end_row}"
        self.sheets.values().update(
            spreadsheetId=self.sheet_id,
            range=range_name,
            valueInputOption="RAW",
            body={"values": rows},
        ).execute()
        if sheet_id is not None:
            self._format_data_cells(
                tab_name,
                sheet_id,
                len(headers),
                len(rows),
                data_start_row_0based=start_row_1based - 1,
            )

        logger.info(
            "Exported %d jobs to '%s' (append=%s, start_row=%s, apply_urls_skipped=%s)",
            len(rows),
            tab_name,
            append,
            start_row_1based,
            skipped_bad_url,
        )
        return {
            "status": "success",
            "date": date_str,
            "tab_name": tab_name,
            "jobs_exported": len(rows),
            "append": append,
            "start_row": start_row_1based,
            "apply_urls_skipped": skipped_bad_url,
        }

    def export_jobs_from_postgres(
        self,
        db: Session,
        *,
        date_str: Optional[str] = None,
        append: bool = False,
        source_value: str = "job_board",
        validate_apply_urls: bool = True,
    ) -> Dict:
        """Export Postgres jobs (filtered by source + IST date) to <date>_jobs tab."""
        if not date_str:
            date_str = self._default_ist_date_str()
        tab_name = f"{date_str}_jobs"

        ref_dt = datetime.strptime(date_str, "%Y-%m-%d")
        start_utc, end_utc, _ = ist_today_utc_window(ref_dt)

        query = (
            select(Job)
            .where(
                and_(
                    Job.source == source_value,
                    or_(
                        and_(Job.created_at >= start_utc, Job.created_at < end_utc),
                        and_(
                            Job.updated_at.is_not(None),
                            Job.updated_at >= start_utc,
                            Job.updated_at < end_utc,
                        ),
                    ),
                )
            )
            .order_by(desc(Job.updated_at).nulls_last(), desc(Job.created_at))
        )
        jobs = db.execute(query).scalars().all()
        if not jobs:
            return {
                "status": "no_jobs",
                "date": date_str,
                "tab_name": tab_name,
                "jobs_exported": 0,
                "append": append,
                "apply_urls_skipped": 0,
            }

        invalid_apply: Set[str] = set()
        if validate_apply_urls:
            invalid_apply = self._invalid_apply_urls([str(j.source_url or "") for j in jobs])

        headers = [
            "Segment (Tech / Non-tech)",
            "Category",
            "Job Title",
            "Company",
            "Location Type",
            "Location Detail",
            "Country",
            "Work Type",
            "Seniority Level",
            "Salary",
            "Skills",
            "Degree / Education",
            "Job Description (short)",
            "Apply URL",
            "Source Domain",
            "Source Discovered Date",
            "Job Posted At (raw)",
            "Date & time (India)",
            "Crawled At (UTC)",
        ]
        sheet_id = self._ensure_tab_with_headers(tab_name, headers, self.JOB_COLUMN_WIDTHS)

        if append:
            start_row_1based = self._next_append_row_1based(tab_name)
            seen_keys = self._existing_job_row_keys(tab_name, len(headers))
        else:
            self._clear_data_rows(tab_name, num_cols=len(headers))
            start_row_1based = 2
            seen_keys = set()

        rows: List[List[str]] = []
        skipped_bad_url = 0
        for j in jobs:
            source_url = j.source_url or ""
            source_domain = j.source_channel_name or (urlparse(source_url).netloc if source_url else "")
            if validate_apply_urls and invalid_apply:
                if normalize_url(str(source_url)) in invalid_apply:
                    skipped_bad_url += 1
                    continue

            desc_full = (j.description or "")[:8000]
            segment, category = self._classify_job(j.title or "", source_domain or "", desc_full)

            salary_raw = (j.salary or "").strip() if j.salary else ""
            salary_range = getattr(j, "salary_range", None)
            if isinstance(salary_range, dict) and not salary_raw:
                salary_raw = str(salary_range.get("raw") or "")
            salary_min = getattr(j, "salary_min", None)
            salary_max = getattr(j, "salary_max", None)
            if not salary_raw and (salary_min is not None or salary_max is not None):
                salary_raw = f"{salary_min or ''}-{salary_max or ''}".strip("-")

            pseudo = {
                "title": j.title or "",
                "location": j.location or "",
                "description": desc_full,
                "skills": j.skills_required if isinstance(j.skills_required, list) else [],
                "salary": salary_raw,
            }
            lt, ld, co, wt_meta, sr_meta, _sal_d, sk_der, deg_der = self._derive_job_metadata(pseudo)

            wt_db = (j.work_type or "").strip().lower()
            if wt_db in ("remote", "wfh", "work from home"):
                location_type = lt or "Remote"
            elif wt_db == "hybrid":
                location_type = lt or "Hybrid"
            elif wt_db in ("on-site", "onsite", "office"):
                location_type = lt or "Onsite"
            else:
                location_type = lt or ""

            location_detail = ld or (j.location or "") or ""
            country = co or ""

            emp = (j.employment_type or "").strip().lower()
            if emp in ("fulltime", "full_time", "full-time"):
                work_type_col = "Full-time"
            elif emp in ("parttime", "part_time", "part-time"):
                work_type_col = "Part-time"
            elif emp in ("contract", "contractor"):
                work_type_col = "Contract"
            elif emp in ("internship", "intern"):
                work_type_col = "Internship"
            elif j.employment_type:
                work_type_col = str(j.employment_type).strip()
            else:
                work_type_col = wt_meta or ""

            seniority = (getattr(j, "experience", None) or "").strip() or sr_meta
            skills = ", ".join(j.skills_required or []) if isinstance(j.skills_required, list) else ""
            if not skills.strip():
                skills = sk_der

            created_utc = j.created_at.isoformat() if j.created_at else ""
            created_ist = ""
            if j.created_at:
                created_ist = j.created_at.replace(tzinfo=pytz.utc).astimezone(IST).strftime("%Y-%m-%d %H:%M")

            row = [
                segment,
                category,
                j.title or "",
                j.company_name or "",
                location_type,
                location_detail,
                country,
                work_type_col,
                seniority,
                salary_raw,
                skills,
                deg_der,
                (j.description or "")[:500],
                source_url,
                source_domain,
                "",
                "",
                created_ist,
                created_utc,
            ]
            key = self._job_row_key(row)
            if key in seen_keys:
                continue
            seen_keys.add(key)
            rows.append(row)

        if not rows:
            return {
                "status": "no_new_jobs",
                "date": date_str,
                "tab_name": tab_name,
                "jobs_exported": 0,
                "append": append,
                "source": source_value,
                "apply_urls_skipped": skipped_bad_url,
            }

        end_col = chr(ord("A") + len(headers) - 1)
        end_row = start_row_1based + len(rows) - 1
        range_name = f"{tab_name}!A{start_row_1based}:{end_col}{end_row}"
        self.sheets.values().update(
            spreadsheetId=self.sheet_id,
            range=range_name,
            valueInputOption="RAW",
            body={"values": rows},
        ).execute()
        if sheet_id is not None:
            self._format_data_cells(
                tab_name,
                sheet_id,
                len(headers),
                len(rows),
                data_start_row_0based=start_row_1based - 1,
            )

        return {
            "status": "success",
            "date": date_str,
            "tab_name": tab_name,
            "jobs_exported": len(rows),
            "append": append,
            "start_row": start_row_1based,
            "source": source_value,
            "apply_urls_skipped": skipped_bad_url,
        }


# Placeholder for future DB-based exports; kept to mirror Telegram exporter API.
def export_today_job_board_data(db: Session) -> Dict:
    """Convenience wrapper for future DB-backed exports (not used yet)."""
    service = JobBoardSheetsService()
    _, _, ist_date_str = ist_today_utc_window()
    date_str = ist_date_str

    data_dir = Path(__file__).parent.parent / "data"
    sources_path = data_dir / "discovery_sources_test.json"
    jobs_path = data_dir / "jobs" / "jobs_master.json"

    sources_result = service.export_sources_from_json(sources_path, date_str)
    jobs_result = service.export_jobs_from_json(jobs_path, date_str)

    return {
        "sources": sources_result,
        "jobs": jobs_result,
    }

