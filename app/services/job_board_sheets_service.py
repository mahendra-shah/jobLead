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

from app.config import settings
from app.utils.timezone import ist_today_utc_window

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

    # ── Internal helpers ──────────────────────────────────────────────────────

    def _ensure_tab_with_headers(self, tab_name: str, headers: List[str]) -> None:
        """Create (or reuse) a tab and ensure header row + basic formatting."""
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
            # Basic formatting: bold header, background color, frozen first row,
            # and uniform column width for readability.
            requests = [
                {
                    "repeatCell": {
                        "range": {
                            "sheetId": sheet_id,
                            "startRowIndex": 0,
                            "endRowIndex": 1,
                        },
                        "cell": {
                            "userEnteredFormat": {
                                "backgroundColor": {
                                    "red": 0.9,
                                    "green": 0.9,
                                    "blue": 0.9,
                                },
                                "textFormat": {
                                    "bold": True,
                                },
                            }
                        },
                        "fields": "userEnteredFormat(backgroundColor,textFormat)",
                    }
                },
                {
                    "updateSheetProperties": {
                        "properties": {
                            "sheetId": sheet_id,
                            "gridProperties": {"frozenRowCount": 1},
                        },
                        "fields": "gridProperties.frozenRowCount",
                    }
                },
                {
                    "updateDimensionProperties": {
                        "range": {
                            "sheetId": sheet_id,
                            "dimension": "COLUMNS",
                            "startIndex": 0,
                            "endIndex": len(headers),
                        },
                        "properties": {
                            "pixelSize": 220,
                        },
                        "fields": "pixelSize",
                    }
                },
            ]

            self.sheets.batchUpdate(
                spreadsheetId=self.sheet_id,
                body={"requests": requests},
            ).execute()

    def _clear_data_rows(self, tab_name: str) -> None:
        """Clear all rows except the header."""
        try:
            self.sheets.values().clear(
                spreadsheetId=self.sheet_id,
                range=f"{tab_name}!A2:Z10000",
            ).execute()
        except Exception as exc:  # pragma: no cover - defensive
            logger.warning("Could not clear rows for tab '%s': %s", tab_name, exc)

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
        """Export discovery sources from JSON to a <date>_sources tab."""
        if not date_str:
            date_str = self._default_ist_date_str()
        tab_name = f"{date_str}_sources"

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
        self._ensure_tab_with_headers(tab_name, headers)
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

        logger.info("Exported %d sources to '%s'", len(rows), tab_name)
        return {
            "status": "success",
            "date": date_str,
            "tab_name": tab_name,
            "sources_exported": len(rows),
        }

    def export_jobs_from_json(
        self, json_path: Path, date_str: Optional[str] = None
    ) -> Dict:
        """Export crawled jobs from JSON to a <date>_jobs tab."""
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
            }

        headers = [
            "Segment (Tech / Non-tech)",
            "Category",
            "Title",
            "Company",
            "Location",
            "Job URL",
            "Source Domain",
            "Source Discovered Date",
            "Job Posted At (raw)",
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
                    job.get("location") or "",
                    job.get("url") or "",
                    source_domain,
                    job.get("source_discovered_date") or "",
                    job.get("job_posted_at_raw") or "",
                    job.get("crawled_at_utc") or "",
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

        logger.info("Exported %d jobs to '%s'", len(rows), tab_name)
        return {
            "status": "success",
            "date": date_str,
            "tab_name": tab_name,
            "jobs_exported": len(rows),
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

