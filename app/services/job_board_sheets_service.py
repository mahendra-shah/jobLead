"""Google Sheets service for exporting Phase 1/2 job-board data.

This is intentionally similar to `GoogleSheetsService` but operates on a
separate Sheet (JOB_BOARD_SHEET_ID) and works with the JSON artifacts
produced by the discovery/crawling pipeline:

- discovery_sources_test.json  →  <date>_sources tab
- jobs/jobs_master.json        →  <date>_jobs tab
"""

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from google.oauth2 import service_account
from googleapiclient.discovery import build
from sqlalchemy.orm import Session  # kept for future DB-based exports

import pytz

from app.config import settings
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

    # ── Classification helpers ────────────────────────────────────────────────

    @staticmethod
    def _classify_job(title: str, source_domain: str) -> Tuple[str, str]:
        """Roughly classify job as Tech / Non-tech + category from title."""
        t = (title or "").lower()
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
        marketing_keywords = ["marketing", "growth", "seo", "content", "performance"]
        support_keywords = ["customer support", "customer success", "support specialist"]
        hr_keywords = ["hr ", "talent acquisition", "recruiter", "recruitment"]
        finance_keywords = ["finance", "accountant", "controller", "fp&a", "audit"]
        product_keywords = ["product manager", "product owner"]
        design_keywords = ["designer", "ux", "ui", "product design", "graphic design"]

        def any_kw(kws: List[str]) -> bool:
            return any(kw in t for kw in kws)

        # Category
        if any_kw(tech_keywords):
            segment = "Tech"
            category = "Software / Engineering"
        elif any_kw(product_keywords):
            segment = "Tech"
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
            else:
                segment = "Unknown"
            category = "Other / Unknown"

        return segment, category

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
        if any(w in title for w in ["intern", "fresher", "graduate", "entry level", "entry-level"]):
            seniority = "Fresher / Entry"
        elif "junior" in title:
            seniority = "Junior"
        elif "senior" in title or "lead" in title:
            seniority = "Senior"

        salary = job.get("salary") or job.get("salary_text") or ""
        skills = ", ".join(job.get("skills") or []) if isinstance(job.get("skills"), list) else (job.get("skills") or "")
        degree = job.get("degree") or job.get("education") or ""

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
    ) -> Dict:
        """Export crawled jobs from JSON to a <date>_jobs tab.

        If append=False (default), existing data rows are cleared and replaced (full refresh).
        If append=True, new rows are written below existing data so the same IST date tab
        accumulates all verified/export batches for that day without overwriting.
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
            }

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
        else:
            start_row_1based = self._next_append_row_1based(tab_name)
            if start_row_1based < 2:
                start_row_1based = 2

        rows: List[List[str]] = []
        for job in jobs:
            title = job.get("title") or ""
            source_domain = job.get("source_domain") or ""
            # Prefer fields from JSON (crawler now fills them); fall back to derived
            segment = job.get("segment") or ""
            category = job.get("category") or ""
            if not segment or not category:
                s, c = self._classify_job(title, source_domain)
                segment = segment or s
                category = category or c
            lt, ld, co, wt, sr, sal_der, sk_der, deg_der = self._derive_job_metadata(job)
            location_type = job.get("location_type") or lt
            location_detail = job.get("location_detail") or job.get("location") or ld
            country = job.get("country") or co
            work_type = job.get("work_type") or wt
            seniority = job.get("seniority") or sr
            salary = job.get("salary") or sal_der
            degree = job.get("degree") or deg_der
            skills_val = job.get("skills")
            if isinstance(skills_val, list):
                skills = ", ".join(str(s) for s in skills_val) if skills_val else sk_der
            else:
                skills = (skills_val or sk_der) if isinstance(skills_val, str) else sk_der
            description = (job.get("description") or job.get("raw_text") or "")[:240]
            apply_url = job.get("apply_url") or job.get("url") or ""

            rows.append(
                [
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
            )

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
            "Exported %d jobs to '%s' (append=%s, start_row=%s)",
            len(rows),
            tab_name,
            append,
            start_row_1based,
        )
        return {
            "status": "success",
            "date": date_str,
            "tab_name": tab_name,
            "jobs_exported": len(rows),
            "append": append,
            "start_row": start_row_1based,
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

