"""Google Sheets service for exporting Phase 1/2 job-board data.

This is intentionally similar to `GoogleSheetsService` but operates on a
separate Sheet (JOB_BOARD_SHEET_ID) and works with the JSON artifacts
produced by the discovery/crawling pipeline:

- discovery_sources_test.json  →  <date>_sources tab
- jobs/jobs_master.json        →  <date>_jobs tab
"""

import json
import logging
import warnings
from collections.abc import Mapping
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse

from google.oauth2 import service_account
from googleapiclient.discovery import build
from sqlalchemy.orm import Session  # kept for future DB-based exports
from sqlalchemy import and_, func, or_, select

import pytz

from app.config import settings
from app.models.job import Job
from app.utils.timezone import IST, ist_today_utc_window

logger = logging.getLogger(__name__)

# Google Sheets per-cell character limit is ~50,000; stay under for safety.
SHEET_DESCRIPTION_MAX_CHARS = 49000


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
        420,  # Job Description (full)
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

    def _ensure_row_capacity(self, tab_name: str, required_end_row_1based: int) -> None:
        """Expand sheet rowCount when write range exceeds current grid size."""
        meta = self.sheets.get(spreadsheetId=self.sheet_id).execute()
        target = None
        for s in meta.get("sheets", []):
            props = s.get("properties", {})
            if props.get("title") == tab_name:
                target = props
                break
        if not target:
            return
        sheet_id = target.get("sheetId")
        grid = target.get("gridProperties", {}) or {}
        current_rows = int(grid.get("rowCount") or 0)
        if required_end_row_1based <= current_rows:
            return
        # Grow with buffer to avoid frequent API calls.
        new_rows = max(required_end_row_1based + 500, current_rows + 1000, 5000)
        self.sheets.batchUpdate(
            spreadsheetId=self.sheet_id,
            body={
                "requests": [
                    {
                        "updateSheetProperties": {
                            "properties": {
                                "sheetId": sheet_id,
                                "gridProperties": {"rowCount": int(new_rows)},
                            },
                            "fields": "gridProperties.rowCount",
                        }
                    }
                ]
            },
        ).execute()

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
        """First empty row below existing content in column A (1-based). Assumes row 1 is header.

        Only reads ``A1:A10000`` — never ``A:A`` (full-column reads hang on large tabs). Prefer
        :meth:`_append_rows_chunked` for appends so the next row is not needed.
        """
        res = (
            self.sheets.values()
            .get(
                spreadsheetId=self.sheet_id,
                range=f"{tab_name}!A1:A10000",
                majorDimension="ROWS",
            )
            .execute()
        )
        vals = res.get("values") or []
        n = len(vals)
        if n >= 10000:
            logger.warning(
                "Tab '%s': column A has >=10000 populated rows; append-row estimate may be low. "
                "Use chunked append API.",
                tab_name,
            )
        return n + 1

    def _sheet_grid_row_count(self, tab_name: str) -> int:
        meta = self.sheets.get(spreadsheetId=self.sheet_id).execute()
        for s in meta.get("sheets", []):
            props = s.get("properties", {})
            if props.get("title") == tab_name:
                return int((props.get("gridProperties") or {}).get("rowCount") or 0)
        return 0

    def _append_rows_chunked(
        self,
        tab_name: str,
        rows: List[List[str]],
        *,
        chunk_size: int = 25,
    ) -> None:
        """Append rows using values.append (no full-column read; avoids huge single POST bodies)."""
        if not rows:
            return
        for i in range(0, len(rows), chunk_size):
            chunk = rows[i : i + chunk_size]
            lo, hi = i + 1, min(i + chunk_size, len(rows))
            logger.info("Sheets append chunk rows %d-%d of %d", lo, hi, len(rows))
            print(f"  Sheets: append rows {lo}-{hi} of {len(rows)}", flush=True)
            self.sheets.values().append(
                spreadsheetId=self.sheet_id,
                range=f"{tab_name}!A1",
                valueInputOption="RAW",
                insertDataOption="INSERT_ROWS",
                body={"values": chunk},
            ).execute()

    def _update_rows_chunked(
        self,
        tab_name: str,
        num_cols: int,
        start_row_1based: int,
        rows: List[List[str]],
        *,
        chunk_size: int = 25,
    ) -> None:
        """Write rows via multiple values.update calls (smaller payloads, fewer timeouts)."""
        if not rows:
            return
        end_col = chr(ord("A") + num_cols - 1)
        for i in range(0, len(rows), chunk_size):
            chunk = rows[i : i + chunk_size]
            sr = start_row_1based + i
            er = sr + len(chunk) - 1
            logger.info("Sheets update chunk rows %d-%d of %d", sr, er, len(rows))
            print(f"  Sheets: update rows {sr}-{er} of {len(rows)} total", flush=True)
            self.sheets.values().update(
                spreadsheetId=self.sheet_id,
                range=f"{tab_name}!A{sr}:{end_col}{er}",
                valueInputOption="RAW",
                body={"values": chunk},
            ).execute()

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
    def _sheet_description_cell(text: Optional[str]) -> str:
        t = (text or "").strip()
        if len(t) <= SHEET_DESCRIPTION_MAX_CHARS:
            return t
        return t[: SHEET_DESCRIPTION_MAX_CHARS - 40] + "\n...[truncated: Sheets cell limit]"

    @staticmethod
    def _pretty_location_type(val: Optional[str]) -> str:
        v = (val or "").strip().lower()
        if not v:
            return ""
        if v in ("on-site", "on_site"):
            v = "onsite"
        return v[:1].upper() + v[1:] if len(v) > 1 else v.upper()

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
            "Job Description (full)",
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
            description = self._sheet_description_cell(
                job.get("description") or job.get("raw_text") or ""
            )
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

        if append:
            grid_n = self._sheet_grid_row_count(tab_name)
            self._ensure_row_capacity(tab_name, grid_n + len(rows) + 100)
            self._append_rows_chunked(tab_name, rows, chunk_size=25)
            start_row_1based = 0
        else:
            end_row = start_row_1based + len(rows) - 1
            self._ensure_row_capacity(tab_name, end_row)
            self._update_rows_chunked(tab_name, num_cols, start_row_1based, rows, chunk_size=25)
        if sheet_id is not None and not append and len(rows) <= 400:
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

    def export_jobs_from_postgres(
        self,
        db: Session,
        *,
        date_str: Optional[str] = None,
        append: bool = False,
        source_value: str = "job_board",
        ignore_date_filter: bool = False,
    ) -> Dict:
        """Export Postgres jobs (filtered by source + IST date) to <date>_jobs tab.

        Rows match the IST calendar day if **created_at** *or* **updated_at** falls in that day.
        (Sync often updates existing job_board rows without changing ``created_at``; without
        ``updated_at`` those would be missing from the daily sheet.)

        If ``ignore_date_filter=True``, exports **all** rows for ``source_value`` (no IST window).
        Use sparingly to repair an empty daily tab; with ``append=True`` you may duplicate rows.
        """
        if not date_str:
            date_str = self._default_ist_date_str()
        tab_name = f"{date_str}_jobs"

        ref_dt = datetime.strptime(date_str, "%Y-%m-%d")
        start_utc, end_utc, _ = ist_today_utc_window(ref_dt)

        # Reflect table at runtime to avoid ORM/schema drift errors
        # (e.g., DB has `experience_required` while model expects `experience`).
        from sqlalchemy import MetaData, Table
        from sqlalchemy import inspect as sa_inspect

        engine = db.get_bind()
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", message=".*Did not recognize type 'vector'.*")
            inspector = sa_inspect(engine)
            job_columns = {str(c.get("name")) for c in inspector.get_columns("jobs")}
            jobs_table = Table("jobs", MetaData(), autoload_with=engine)

        has_source_col = "source" in job_columns
        has_created_at_col = "created_at" in job_columns
        has_updated_at_col = "updated_at" in job_columns

        select_cols = [
            jobs_table.c[name]
            for name in (
                "title",
                "company_name",
                "description",
                "raw_text",
                "skills_required",
                "experience_required",
                "work_type",
                "job_type",
                "employment_type",
                "salary_range",
                "salary_min",
                "salary_max",
                "location",
                "quality_breakdown",
                "source_url",
                "source_channel_name",
                "created_at",
                "updated_at",
                "source",
            )
            if name in job_columns
        ]
        stmt = select(*select_cols)
        where_clauses = []
        if has_source_col:
            where_clauses.append(jobs_table.c.source == source_value)

        if not ignore_date_filter:
            created_in_day = (
                and_(jobs_table.c.created_at >= start_utc, jobs_table.c.created_at < end_utc)
                if has_created_at_col
                else None
            )
            updated_in_day = (
                and_(jobs_table.c.updated_at >= start_utc, jobs_table.c.updated_at < end_utc)
                if has_updated_at_col
                else None
            )
            if created_in_day is not None and updated_in_day is not None:
                where_clauses.append(or_(created_in_day, updated_in_day))
            elif created_in_day is not None:
                where_clauses.append(created_in_day)
            elif updated_in_day is not None:
                where_clauses.append(updated_in_day)

        if where_clauses:
            stmt = stmt.where(and_(*where_clauses))
        if has_updated_at_col and has_created_at_col:
            stmt = stmt.order_by(func.coalesce(jobs_table.c.updated_at, jobs_table.c.created_at).desc())
        elif has_created_at_col:
            stmt = stmt.order_by(jobs_table.c.created_at.desc())
        elif has_updated_at_col:
            stmt = stmt.order_by(jobs_table.c.updated_at.desc())

        jobs = db.execute(stmt).mappings().all()
        if not jobs:
            return {
                "status": "no_jobs",
                "date": date_str,
                "tab_name": tab_name,
                "jobs_exported": 0,
                "append": append,
                "ignore_date_filter": ignore_date_filter,
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
            "Job Description (full)",
            "Apply URL",
            "Source Domain",
            "Source Discovered Date",
            "Job Posted At (raw)",
            "Date & time (India)",
            "Crawled At (UTC)",
        ]
        sheet_id = self._ensure_tab_with_headers(tab_name, headers, self.JOB_COLUMN_WIDTHS)

        if not append:
            self._clear_data_rows(tab_name, num_cols=len(headers))
        start_row_1based = 2

        rows: List[List[str]] = []
        for j in jobs:
            # SQLAlchemy RowMapping is a Mapping but not a dict; do not use isinstance(..., dict).
            source_url = (j.get("source_url") or "") if isinstance(j, Mapping) else ""
            source_domain = (j.get("source_channel_name") or "") if isinstance(j, Mapping) else ""
            if not source_domain and source_url:
                source_domain = urlparse(source_url).netloc
            title = (j.get("title") or "") if isinstance(j, Mapping) else ""
            segment, category = self._classify_job(title, source_domain or "")
            if j.get("job_type"):
                wt = str(j.get("job_type")).strip()
            elif j.get("employment_type"):
                wt = str(j.get("employment_type")).strip()
            else:
                wt = ""
            salary_raw = ""
            if isinstance(j.get("salary_range"), dict):
                salary_raw = str((j.get("salary_range") or {}).get("raw") or "")
            if not salary_raw and (j.get("salary_min") is not None or j.get("salary_max") is not None):
                salary_raw = f"{j.get('salary_min') or ''}-{j.get('salary_max') or ''}".strip("-")
            skills = ", ".join(j.get("skills_required") or []) if isinstance(j.get("skills_required"), list) else ""

            jb: dict = {}
            if isinstance(j.get("quality_breakdown"), dict):
                inner = (j.get("quality_breakdown") or {}).get("job_board_export")
                if isinstance(inner, dict):
                    jb = inner

            fake = {
                "title": title,
                "location": j.get("location") or "",
                "description": j.get("description") or j.get("raw_text") or "",
                "skills": j.get("skills_required") if isinstance(j.get("skills_required"), list) else [],
                "salary": salary_raw,
            }
            lt_d, ld_d, co_d, wt_d, sr_d, sal_der, sk_der, deg_d = self._derive_job_metadata(fake)
            country_cell = (jb.get("country") or "").strip() or co_d
            degree_cell = (jb.get("degree") or "").strip() or deg_d
            discovered_cell = (jb.get("source_discovered_date") or "").strip()
            posted_cell = (jb.get("job_posted_at_raw") or "").strip()
            loc_type_disp = self._pretty_location_type(j.get("work_type")) or lt_d
            work_type_disp = wt or wt_d
            seniority_disp = (j.get("experience_required") or "").strip() or sr_d
            salary_disp = salary_raw or sal_der
            skills_disp = skills or sk_der
            location_detail = (j.get("location") or "").strip() or ld_d
            company_disp = (j.get("company_name") or "").strip() or source_domain or "Unknown"
            if not location_detail:
                location_detail = country_cell or ("Remote" if (work_type_disp or "").lower().find("remote") >= 0 else "")
            apply_url_cell = source_url.strip()
            if apply_url_cell and not apply_url_cell.startswith(("http://", "https://")) and source_domain:
                apply_url_cell = f"https://{source_domain}{apply_url_cell if apply_url_cell.startswith('/') else '/' + apply_url_cell}"

            created_at = j.get("created_at")
            created_utc = created_at.isoformat() if created_at else ""
            created_ist = ""
            if created_at:
                created_ist = created_at.replace(tzinfo=pytz.utc).astimezone(IST).strftime("%Y-%m-%d %H:%M")

            rows.append(
                [
                    segment,
                    category,
                    title,
                    company_disp,
                    loc_type_disp,
                    location_detail,
                    country_cell,
                    work_type_disp,
                    seniority_disp,
                    salary_disp,
                    skills_disp,
                    degree_cell,
                    self._sheet_description_cell(j.get("description") or j.get("raw_text") or ""),
                    apply_url_cell,
                    source_domain,
                    discovered_cell,
                    posted_cell,
                    created_ist,
                    created_utc,
                ]
            )

        num_cols = len(headers)
        if append:
            grid_n = self._sheet_grid_row_count(tab_name)
            self._ensure_row_capacity(tab_name, grid_n + len(rows) + 100)
            self._append_rows_chunked(tab_name, rows, chunk_size=25)
            start_row_1based = 0
        else:
            end_row = start_row_1based + len(rows) - 1
            self._ensure_row_capacity(tab_name, end_row)
            self._update_rows_chunked(tab_name, num_cols, start_row_1based, rows, chunk_size=25)
        if sheet_id is not None and not append and len(rows) <= 400:
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
            "ignore_date_filter": ignore_date_filter,
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

