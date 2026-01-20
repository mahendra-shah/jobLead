"""Google Sheets service for exporting daily jobs."""

import logging
from datetime import datetime, timedelta
from typing import Dict, Optional
from pathlib import Path

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from sqlalchemy import select, and_
from sqlalchemy.orm import Session

from app.models.job import Job
from app.models.company import Company
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
    
    def create_daily_tab(self, date: datetime) -> str:
        """
        Create a new tab for the given date.
        
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
                logger.info(f"Tab '{tab_name}' already exists, will append data")
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
            'Time',
            'Company',
            'Job Title',
            'Location',
            'Experience Min',
            'Experience Max',
            'Is Fresher',
            'Salary Min',
            'Salary Max',
            'Work Type',
            'Job Type',
            'Skills',
            'Source Channel',
            'Apply Link',
            'Full Message Text'
        ]
        
        body = {
            'values': [headers]
        }
        
        self.sheets.values().update(
            spreadsheetId=self.sheet_id,
            range=f"{tab_name}!A1:R1",
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
        
        self.sheets.batchUpdate(
            spreadsheetId=self.sheet_id,
            body={'requests': requests}
        ).execute()
    
    def export_daily_jobs(self, db: Session, date: Optional[datetime] = None) -> Dict:
        """
        Export jobs processed on the given date to Google Sheets.
        
        Args:
            db: Database session
            date: Date to export (defaults to yesterday)
            
        Returns:
            Summary dict with export stats
        """
        if date is None:
            # Default to yesterday (jobs processed yesterday)
            date = datetime.now() - timedelta(days=1)
        
        # Set time range (full day)
        start_time = date.replace(hour=0, minute=0, second=0, microsecond=0)
        end_time = start_time + timedelta(days=1)
        
        logger.info(f"📊 Exporting jobs from {start_time} to {end_time}")
        
        # Query jobs created in this time range
        query = (
            select(Job)
            .outerjoin(Company, Job.company_id == Company.id)
            .where(
                and_(
                    Job.created_at >= start_time,
                    Job.created_at < end_time
                )
            )
            .order_by(Job.created_at.desc())
        )
        
        result = db.execute(query)
        jobs = result.scalars().all()
        
        if not jobs:
            logger.warning(f"No jobs found for {date.strftime('%Y-%m-%d')}")
            return {
                'status': 'no_jobs',
                'date': date.strftime('%Y-%m-%d'),
                'jobs_exported': 0
            }
        
        logger.info(f"Found {len(jobs)} jobs to export")
        
        # Create tab
        tab_name = self.create_daily_tab(date)
        
        # Prepare data rows
        rows = []
        for job in jobs:
            # Get company name
            company_name = "Unknown"
            if job.company_id:
                company = db.execute(
                    select(Company).where(Company.id == job.company_id)
                ).scalar_one_or_none()
                if company:
                    company_name = company.name
            
            row = [
                str(job.id),  # ID
                job.source_message_id or '',  # Message ID
                job.created_at.strftime('%Y-%m-%d'),  # Date
                job.created_at.strftime('%H:%M:%S'),  # Time
                company_name,  # Company
                job.title or '',  # Job Title
                job.location or '',  # Location
                job.experience_min if job.experience_min is not None else '',  # Experience Min
                job.experience_max if job.experience_max is not None else '',  # Experience Max
                'Yes' if job.is_fresher else 'No' if job.is_fresher is not None else '',  # Is Fresher
                f"₹{job.salary_min:,.0f}" if job.salary_min else '',  # Salary Min
                f"₹{job.salary_max:,.0f}" if job.salary_max else '',  # Salary Max
                job.work_type or '',  # Work Type (remote/on-site/hybrid)
                job.job_type or '',  # Job Type
                ', '.join(job.skills_required) if job.skills_required else '',  # Skills
                job.source or '',  # Source Channel
                job.source_url or '',  # Apply Link
                (job.raw_text or '')[:1000]  # Full Message Text (truncated to 1000 chars)
            ]
            rows.append(row)
        
        # Write data to sheet
        if rows:
            # Find next empty row
            try:
                existing_data = self.sheets.values().get(
                    spreadsheetId=self.sheet_id,
                    range=f"{tab_name}!A:A"
                ).execute()
                
                next_row = len(existing_data.get('values', [])) + 1
            except Exception:
                next_row = 2  # Start after header
            
            body = {
                'values': rows
            }
            
            end_col = chr(ord('A') + len(rows[0]) - 1)  # Calculate last column letter
            range_name = f"{tab_name}!A{next_row}:{end_col}{next_row + len(rows) - 1}"
            
            self.sheets.values().update(
                spreadsheetId=self.sheet_id,
                range=range_name,
                valueInputOption='RAW',
                body=body
            ).execute()
            
            logger.info(f"✅ Exported {len(rows)} jobs to '{tab_name}'")
        
        return {
            'status': 'success',
            'date': date.strftime('%Y-%m-%d'),
            'tab_name': tab_name,
            'jobs_exported': len(jobs),
            'sheet_url': f"https://docs.google.com/spreadsheets/d/{self.sheet_id}"
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
