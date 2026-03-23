"""Google Sheets service for exporting daily jobs."""

import logging
from datetime import datetime, timedelta
from app.utils.timezone import IST, ist_today_utc_window
from typing import Dict, Optional
from pathlib import Path

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from sqlalchemy import select, and_
from sqlalchemy.orm import Session

from app.models.job import Job
from app.config import settings

logger = logging.getLogger(__name__)


class GoogleSheetsService:
    """Service to export jobs to Google Sheets daily."""
    
    def __init__(self):
        """Initialize Google Sheets API client."""
        self.sheet_id = settings.SHEET_ID
        self.credentials_path = Path(__file__).parent.parent.parent / "credentials.json"
        
        # Scopes for Google Sheets API
        self.scopes = ['https://www.googleapis.com/auth/spreadsheets']
        
        # Initialize credentials
        self.credentials = service_account.Credentials.from_service_account_file(
            str(self.credentials_path),
            scopes=self.scopes
        )
        
        # Build service
        self.service = build('sheets', 'v4', credentials=self.credentials)
        self.sheets = self.service.spreadsheets()

    def _build_export_row(self, job: Job):
        company_name = job.company_name or 'Unknown'
        channel_name = job.source_channel_name or ''
        channel_url = f"https://t.me/{channel_name}" if channel_name else ''
        channel_id = job.source_telegram_channel_id or ''
        sender_id = str(job.sender_id) if job.sender_id else ''
        account_used = f"Account {job.fetched_by_account}" if job.fetched_by_account else ''

        return [
            str(job.id),
            job.source_message_id or '',
            job.created_at.strftime('%Y-%m-%d'),
            company_name,
            job.title or '',
            job.location or '',
            'Yes' if job.is_fresher else ('No' if job.is_fresher is not None else ''),
            job.experience or '',
            job.work_type or '',
            job.job_type or '',
            ', '.join(job.skills_required) if job.skills_required else '',
            channel_name,
            channel_url,
            channel_id,
            sender_id,
            account_used,
            job.source or '',
            job.source_url or '',
            (job.description or '')[:1000],
        ]
    
    def create_daily_tab_by_date_str(self, date_str: str) -> str:
        """
        Create (or reuse) a tab named '<date_str>_v2'.

        Args:
            date_str: IST calendar date string, e.g. '2026-03-03'

        Returns:
            Tab name (e.g. '2026-03-03_v2')
        """
        tab_name = f"{date_str}_v2"

        sheet_metadata = self.sheets.get(spreadsheetId=self.sheet_id).execute()
        existing_sheets = {s['properties']['title'] for s in sheet_metadata['sheets']}

        if tab_name in existing_sheets:
            logger.info(f"Tab '{tab_name}' already exists")
            self._add_header_row(tab_name)
            return tab_name

        self.sheets.batchUpdate(
            spreadsheetId=self.sheet_id,
            body={'requests': [{'addSheet': {'properties': {
                'title': tab_name,
                'gridProperties': {'rowCount': 2000, 'columnCount': 20}
            }}}]}
        ).execute()
        logger.info(f"✅ Created new tab: {tab_name}")
        self._add_header_row(tab_name)
        return tab_name

    def create_daily_tab(self, date: datetime) -> str:
        """
        Create a new tab for the given date.
        Kept for backward compatibility — internally delegates to
        create_daily_tab_by_date_str with a naive local date string.

        Args:
            date: Date for the tab name

        Returns:
            Tab name created (e.g., "2026-01-21_v2")
        """
        tab_name = f"{date.strftime('%Y-%m-%d')}_v2"
        
        try:
            # Check if tab exists
            sheet_metadata = self.sheets.get(spreadsheetId=self.sheet_id).execute()
            existing_sheets = {sheet['properties']['title'] for sheet in sheet_metadata['sheets']}
            
            if tab_name in existing_sheets:
                logger.info(f"Tab '{tab_name}' already exists")
                # Ensure headers are present even if tab exists
                self._add_header_row(tab_name)
                return tab_name
            
            # Create new tab
            request_body = {
                'requests': [{
                    'addSheet': {
                        'properties': {
                            'title': tab_name,
                            'gridProperties': {
                                'rowCount': 1000,
                                'columnCount': 15
                            }
                        }
                    }
                }]
            }
            
            self.sheets.batchUpdate(
                spreadsheetId=self.sheet_id,
                body=request_body
            ).execute()
            
            logger.info(f"✅ Created new tab: {tab_name}")
            
            # Add header row
            self._add_header_row(tab_name)
            
            return tab_name
            
        except HttpError as e:
            logger.error(f"Error creating tab: {e}")
            raise
    
    def _add_header_row(self, tab_name: str):
        """Add header row to the new tab."""
        headers = [
            'ID',
            'Message ID',
            'Date',
            # 'Time',  # Commented per user request
            'Company',
            'Job Title',
            'Location',
            # 'Experience Min',  # Commented per user request
            # 'Experience Max',  # Commented per user request
            'Is Fresher',
            'Experience Required',       # NEW — raw string, e.g. '1-2 years' or 'Fresher'
            # 'Salary Min',  # Commented per user request
            # 'Salary Max',  # Commented per user request
            'Work Type',
            'Job Type',
            'Skills',
            'Channel Name',           # NEW
            'Channel URL',            # NEW
            'Channel ID',             # Fixed - Now shows actual Telegram channel ID
            'Sender ID',              # NEW - Telegram sender user ID
            'Account Used',           # NEW
            'Source Channel',         # OLD - keeping for backward compat
            'Apply Link',
            'Full Message Text'
        ]
        
        body = {
            'values': [headers]
        }
        
        self.sheets.values().update(
            spreadsheetId=self.sheet_id,
            range=f"{tab_name}!A1:S1",  # 19 columns (A-S) - added Experience Required
            valueInputOption='RAW',
            body=body
        ).execute()
        
        # Format header row (bold, background color)
        self._format_header_row(tab_name)
    
    def _format_header_row(self, tab_name: str):
        """Format the header row with bold text and background color."""
        # Get sheet ID by name
        sheet_metadata = self.sheets.get(spreadsheetId=self.sheet_id).execute()
        sheet_id = None
        for sheet in sheet_metadata['sheets']:
            if sheet['properties']['title'] == tab_name:
                sheet_id = sheet['properties']['sheetId']
                break
        
        if not sheet_id:
            return
        
        requests = [
            {
                'repeatCell': {
                    'range': {
                        'sheetId': sheet_id,
                        'startRowIndex': 0,
                        'endRowIndex': 1
                    },
                    'cell': {
                        'userEnteredFormat': {
                            'backgroundColor': {
                                'red': 0.2,
                                'green': 0.6,
                                'blue': 0.86
                            },
                            'textFormat': {
                                'bold': True,
                                'foregroundColor': {
                                    'red': 1.0,
                                    'green': 1.0,
                                    'blue': 1.0
                                }
                            }
                        }
                    },
                    'fields': 'userEnteredFormat(backgroundColor,textFormat)'
                }
            }
        ]
        
        # Freeze the first (header) row so it stays visible when scrolling
        requests.append({
            'updateSheetProperties': {
                'properties': {
                    'sheetId': sheet_id,
                    'gridProperties': {'frozenRowCount': 1},
                },
                'fields': 'gridProperties.frozenRowCount',
            }
        })

        self.sheets.batchUpdate(
            spreadsheetId=self.sheet_id,
            body={'requests': requests}
        ).execute()

    def export_daily_jobs(self, db: Session, date: Optional[datetime] = None) -> Dict:
        """
        Export jobs processed on the given date to Google Sheets.

        Safe to call multiple times per day — clears existing data rows
        and rewrites all jobs fresh each time (idempotent, no duplicates).

        Args:
            db: Database session
            date: Date to export (defaults to today)

        Returns:
            Summary dict with export stats
        """
        # ── IST-based date boundary (via central utility) ────────────────────
        # ist_today_utc_window() accepts an optional reference datetime so callers
        # can pass a specific IST date (e.g. yesterday) for historical exports.
        start_time, end_time, ist_date_str = ist_today_utc_window(date)

        logger.info(f"📊 Exporting jobs {start_time} → {end_time} UTC (IST date: {ist_date_str})")

        query = (
            select(Job)
            .where(
                and_(
                    Job.created_at >= start_time,
                    Job.created_at < end_time,
                    Job.quality_score >= 50,
                    Job.is_active.is_(True),
                )
            )
            .order_by(Job.created_at.desc())
        )

        result = db.execute(query)
        jobs = result.scalars().all()

        if not jobs:
            logger.warning(f"No jobs found for {ist_date_str}")
            return {
                'status': 'no_jobs',
                'date': ist_date_str,
                'jobs_exported': 0,
            }

        logger.info(f"Found {len(jobs)} jobs to export")

        # Use IST date string so the tab is named after the Indian calendar day
        tab_name = self.create_daily_tab_by_date_str(ist_date_str)

        # Clear all data rows (keep header in row 1) — prevents duplicates on repeated runs
        try:
            self.sheets.values().clear(
                spreadsheetId=self.sheet_id,
                range=f"{tab_name}!A2:T10000",
            ).execute()
            logger.debug(f"Cleared data rows for tab '{tab_name}'")
        except Exception as e:
            logger.warning(f"Could not clear existing rows (non-fatal): {e}")

        rows = [self._build_export_row(job) for job in jobs]

        if rows:
            end_col = chr(ord('A') + len(rows[0]) - 1)
            range_name = f"{tab_name}!A2:{end_col}{1 + len(rows)}"
            self.sheets.values().update(
                spreadsheetId=self.sheet_id,
                range=range_name,
                valueInputOption='RAW',
                body={'values': rows},
            ).execute()
            logger.info(f"✅ Exported {len(rows)} jobs to '{tab_name}'")

        return {
            'status': 'success',
            'date': ist_date_str,
            'tab_name': tab_name,
            'jobs_exported': len(jobs),
            'sheet_url': f"https://docs.google.com/spreadsheets/d/{self.sheet_id}",
        }
    
    def export_today_jobs(self, db: Session) -> Dict:
        """
        Export jobs processed TODAY (for testing).
        
        Args:
            db: Database session
            
        Returns:
            Summary dict with export stats
        """
        today = datetime.now()
        return self.export_daily_jobs(db, today)
