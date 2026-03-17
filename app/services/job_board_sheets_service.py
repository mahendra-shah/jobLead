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
from urllib.parse import urlparse

from google.oauth2 import service_account
from googleapiclient.discovery import build
from sqlalchemy.orm import Session  # kept for future DB-based exports

from app.config import settings
from app.utils.timezone import ist_today_utc_window

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
                },
            ]

            self.sheets.batchUpdate(
                spreadsheetId=self.sheet_id,
                body={"requests": requests},
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
            "Location",
            "Job URL",
            "Source Domain",
            "Source Discovered Date",
            "Job Posted At (raw)",
            "Date & time (India)",
            "Crawled At (UTC)",
        ]
        self._ensure_tab_with_headers(tab_name, headers)
        self._clear_data_rows(tab_name)

        rows: List[List[str]] = []
        for job in jobs:
            title = job.get("title") or ""
            source_domain = job.get("source_domain") or ""
            segment, category = self._classify_job(title, source_domain)

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

    def export_jobs_from_postgres(
        self,
        db: Session,
        *,
        date_str: Optional[str] = None,
        append: bool = False,
        source_value: str = "job_board",
    ) -> Dict:
        """Export Postgres jobs (filtered by source + IST date) to <date>_jobs tab."""
        if not date_str:
            date_str = self._default_ist_date_str()
        tab_name = f"{date_str}_jobs"

        ref_dt = datetime.strptime(date_str, "%Y-%m-%d")
        start_utc, end_utc, _ = ist_today_utc_window(ref_dt)

        # Schema drift safety: we reflect the jobs table and only use columns
        # that actually exist in the current DB.
        from sqlalchemy import MetaData, Table
        from sqlalchemy import inspect as sa_inspect

        engine = db.get_bind()
        inspector = sa_inspect(engine)
        job_columns = {str(c.get("name")) for c in inspector.get_columns("jobs")}

        md = MetaData()
        jobs_table = Table("jobs", md, autoload_with=engine)

        has_source_col = "source" in job_columns
        has_created_at_col = "created_at" in job_columns

        # Columns we will attempt to read (only if present).
        candidate_cols = [
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
            "source_discovered_date",
            "job_posted_at_raw",
            "created_at",
            "source",
        ]

        selected = [jobs_table.c[c] for c in candidate_cols if c in job_columns]
        if not selected:
            return {
                "status": "no_jobs",
                "date": date_str,
                "tab_name": tab_name,
                "jobs_exported": 0,
                "append": append,
            }

        where_clauses = []
        if has_source_col:
            where_clauses.append(jobs_table.c.source == source_value)
        if has_created_at_col:
            where_clauses.append(jobs_table.c.created_at >= start_utc)
            where_clauses.append(jobs_table.c.created_at < end_utc)

        stmt = select(*selected)
        if where_clauses:
            stmt = stmt.where(and_(*where_clauses))
        if has_created_at_col:
            stmt = stmt.order_by(jobs_table.c.created_at.desc())

        jobs = db.execute(stmt).mappings().all()
        if not jobs:
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

        if append:
            start_row_1based = self._next_append_row_1based(tab_name)
        else:
            self._clear_data_rows(tab_name, num_cols=len(headers))
            start_row_1based = 2

        rows: List[List[str]] = []
        for j in jobs:
            source_url = (j.get("source_url") or "") or ""
            source_domain = (j.get("source_channel_name") or "") or (
                urlparse(source_url).netloc if source_url else ""
            )
            title = (j.get("title") or "") or ""
            segment, category = self._classify_job(title, source_domain or "")

            # Work type fields differ across schema versions.
            wt = ""
            for col in ("work_type", "job_type", "employment_type"):
                v = j.get(col)
                if v:
                    wt = str(v).strip()
                    break

            # Salary fields differ across schema versions.
            salary_raw = ""
            sr = j.get("salary_range")
            if isinstance(sr, dict):
                salary_raw = str(sr.get("raw") or "")
            if not salary_raw and (j.get("salary_min") is not None or j.get("salary_max") is not None):
                salary_raw = f"{j.get('salary_min') or ''}-{j.get('salary_max') or ''}".strip("-")

            skills = ", ".join(j.get("skills_required") or []) if isinstance(j.get("skills_required"), list) else ""

            jb: dict = {}
            qbd = j.get("quality_breakdown")
            if isinstance(qbd, dict):
                inner = qbd.get("job_board_export")
                if isinstance(inner, dict):
                    jb = inner

            desc_full = j.get("description") or j.get("raw_text") or ""
            fake = {
                "title": title,
                "location": j.get("location") or "",
                "description": desc_full,
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
                    j.get("company_name") or "",
                    loc_type_disp,
                    location_detail,
                    country_cell,
                    work_type_disp,
                    seniority_disp,
                    salary_disp,
                    skills_disp,
                    degree_cell,
                    self._sheet_description_cell(desc_full),
                    source_url,
                    source_domain,
                    discovered_cell,
                    posted_cell,
                    created_ist,
                    created_utc,
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

        logger.info("Exported %d jobs to '%s'", len(rows), tab_name)
        return {
            "status": "success",
            "date": date_str,
            "tab_name": tab_name,
            "jobs_exported": len(rows),
            "append": append,
            "start_row": start_row_1based,
            "source": source_value,
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

