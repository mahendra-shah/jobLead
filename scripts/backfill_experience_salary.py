#!/usr/bin/env python3
"""
Backfill experience and salary fields for existing jobs.

This script parses the existing job descriptions to extract experience and salary
information, populating the new structured    # Pattern 7: Just "0" or "0 year"
    if re.search(r'\b0\b', text_lower):
        return {'min': 0, 'max': 0, 'is_fresher': True}
    
    # Pattern 8: "Experienced" (not fresh, but no specific number)
    if re.search(r'\bexperienced\b', text_lower):
        return {'min': None, 'max': None, 'is_fresher': False}
    
    # Pattern 9: Common phrases
    if re.search(r'not\s+(?:specified|required|applicable)', text_lower):
        return {'min': None, 'max': None, 'is_fresher': False}
    
    if re.search(r'any\s+experience', text_lower):
        return {'min': None, 'max': None, 'is_fresher': False} min_experience, max_experience, is_fresher (for experience)
- min_salary, max_salary, salary_currency (for salary)

The script extracts from the `description` field since `experience_required` and
`salary_range` are mostly NULL/empty in existing jobs.

Usage:
    python scripts/backfill_experience_salary.py [--dry-run] [--limit N]
"""

import sys
import os
import re
import argparse
from typing import Dict, Optional, Tuple

from sqlalchemy import create_engine, select, update
from sqlalchemy.orm import Session

# Add project root to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from app.models.job import Job
from app.config import Settings


def extract_experience_from_description(description: Optional[str]) -> str:
    """
    Extract experience text from job description.
    
    Looks for patterns like:
    - "Experience: 0-2 years"
    - "Experience: Fresher"
    - "Exp: 3+ years"
    
    Returns the extracted experience string or empty string.
    """
    if not description:
        return ""
    
    # Look for "Experience:" or "Exp:" followed by experience details
    patterns = [
        r'Experience[:\s]+([^\n]+)',
        r'Exp[:\s]+([^\n]+)',
        r'(?:Required\s+)?Experience[:\s]+([^\n]+)',
    ]
    
    for pattern in patterns:
        match = re.search(pattern, description, re.IGNORECASE)
        if match:
            exp_text = match.group(1).strip()
            # Clean up common trailing characters
            exp_text = re.sub(r'\s*[\|\:\.,]+\s*$', '', exp_text)
            return exp_text
    
    return ""


def extract_salary_from_description(description: Optional[str]) -> Dict[str, any]:
    """
    Extract salary information from job description.
    
    Looks for patterns like:
    - "Salary: 12-24 LPA"
    - "Salary: 3-4 LPA"
    - "CTC: 5-8 LPA"
    - "Salary: $50k - $80k"
    - "Salary: ₹6-10 lakhs"
    
    Returns dict with min, max, and currency.
    """
    if not description:
        return {'min': None, 'max': None, 'currency': 'INR'}
    
    # Pattern 1: LPA (Lakhs Per Annum)
    # "12-24 LPA" or "3-4 LPA"
    lpa_match = re.search(r'(\d+(?:\.\d+)?)\s*-\s*(\d+(?:\.\d+)?)\s*LPA', description, re.IGNORECASE)
    if lpa_match:
        min_lpa = float(lpa_match.group(1))
        max_lpa = float(lpa_match.group(2))
        # Convert LPA to actual rupees (1 LPA = 100,000)
        return {
            'min': int(min_lpa * 100000),
            'max': int(max_lpa * 100000),
            'currency': 'INR'
        }
    
    # Pattern 2: Dollar amounts
    # "$50k - $80k" or "$50,000 - $80,000"
    dollar_match = re.search(r'\$\s*(\d+(?:,\d+)?(?:\.\d+)?)\s*k?\s*-\s*\$?\s*(\d+(?:,\d+)?(?:\.\d+)?)\s*k?', description, re.IGNORECASE)
    if dollar_match:
        min_val = dollar_match.group(1).replace(',', '')
        max_val = dollar_match.group(2).replace(',', '')
        
        # Check if it's in thousands (k)
        if 'k' in dollar_match.group(0).lower():
            min_val = float(min_val) * 1000
            max_val = float(max_val) * 1000
        else:
            min_val = float(min_val)
            max_val = float(max_val)
        
        return {
            'min': int(min_val),
            'max': int(max_val),
            'currency': 'USD'
        }
    
    # Pattern 3: Lakhs (Indian currency)
    # "₹6-10 lakhs" or "6-10 lakhs"
    lakhs_match = re.search(r'₹?\s*(\d+(?:\.\d+)?)\s*-\s*(\d+(?:\.\d+)?)\s*lakhs?', description, re.IGNORECASE)
    if lakhs_match:
        min_lakhs = float(lakhs_match.group(1))
        max_lakhs = float(lakhs_match.group(2))
        # Convert lakhs to actual rupees (1 lakh = 100,000)
        return {
            'min': int(min_lakhs * 100000),
            'max': int(max_lakhs * 100000),
            'currency': 'INR'
        }
    
    return {'min': None, 'max': None, 'currency': 'INR'}


def parse_experience(text: Optional[str]) -> Dict[str, any]:
    """
    Parse experience_required text to structured fields.
    
    Examples:
        "0-2 years" → {'min': 0, 'max': 2, 'is_fresher': True}
        "2-5 yrs" → {'min': 2, 'max': 5, 'is_fresher': False}
        "Fresher" → {'min': 0, 'max': 0, 'is_fresher': True}
        "5+ years" → {'min': 5, 'max': None, 'is_fresher': False}
        "6 months" → {'min': 0.5, 'max': 0.5, 'is_fresher': True}
        NULL → {'min': None, 'max': None, 'is_fresher': False}
    
    Fresher definition: 0-6 months (0-0.5 years)
    """
    if not text or not text.strip():
        return {'min': None, 'max': None, 'is_fresher': False}
    
    text_lower = text.lower().strip()
    
    # Pattern 1: Check for "Fresher" variants (most specific first)
    if re.search(r'\b(?:fresh|fresher|freshers)\b', text_lower):
        return {'min': 0, 'max': 0, 'is_fresher': True}
    
    # Pattern 2: Check for months first
    # "6 months", "6-12 months", "6 to 12 months"
    month_range_match = re.search(r'(\d+)\s*(?:to|-)\s*(\d+)\s*months?', text_lower)
    if month_range_match:
        min_months = int(month_range_match.group(1))
        max_months = int(month_range_match.group(2))
        min_years = round(min_months / 12, 1)
        max_years = round(max_months / 12, 1)
        return {
            'min': min_years,
            'max': max_years,
            'is_fresher': max_years <= 0.5
        }
    
    # Single month value: "6 months", "3 months"
    single_month_match = re.search(r'(\d+)\s*months?', text_lower)
    if single_month_match:
        months = int(single_month_match.group(1))
        years = round(months / 12, 1)
        return {
            'min': years,
            'max': years,
            'is_fresher': years <= 0.5
        }
    
    # Pattern 3: Check for range "0-2 years", "2-5 yrs"
    range_match = re.search(r'(\d+)\s*(?:to|-)\s*(\d+)\s*(?:years?|yrs?)', text_lower)
    if range_match:
        min_exp = int(range_match.group(1))
        max_exp = int(range_match.group(2))
        return {
            'min': float(min_exp),
            'max': float(max_exp),
            'is_fresher': min_exp == 0 and max_exp <= 0.5
        }
    
    # Pattern 4: Check for "5+" pattern
    plus_match = re.search(r'(\d+)\+\s*(?:years?|yrs?)?', text_lower)
    if plus_match:
        min_exp = int(plus_match.group(1))
        return {
            'min': float(min_exp),
            'max': None,  # NULL means "no upper limit"
            'is_fresher': False
        }
    
    # Pattern 5: Check for single number "2 years", "3 yrs"
    single_match = re.search(r'(\d+)\s*(?:years?|yrs?)', text_lower)
    if single_match:
        exp = int(single_match.group(1))
        return {
            'min': float(exp),
            'max': float(exp),
            'is_fresher': exp == 0
        }
    
    # Pattern 6: Just "0" or "0 year"
    if re.search(r'\b0\b', text_lower):
        return {'min': 0, 'max': 0, 'is_fresher': True}
    
    # Pattern 7: Common phrases
    if re.search(r'not\s+(?:specified|required|applicable)', text_lower):
        return {'min': None, 'max': None, 'is_fresher': False}
    
    if re.search(r'any\s+experience', text_lower):
        return {'min': None, 'max': None, 'is_fresher': False}
    
    # Default: can't parse
    return {'min': None, 'max': None, 'is_fresher': False}


def parse_salary(salary_range: Optional[dict]) -> Tuple[Optional[int], Optional[int], str]:
    """
    Parse salary_range JSONB to structured fields.
    
    Examples:
        {"min": 50000, "max": 80000, "currency": "USD"} → (50000, 80000, "USD")
        {"min": 50000} → (50000, None, "INR")
        {} → (None, None, "INR")
        None → (None, None, "INR")
    """
    if not salary_range or not isinstance(salary_range, dict):
        return (None, None, 'INR')
    
    min_salary = salary_range.get('min')
    max_salary = salary_range.get('max')
    currency = salary_range.get('currency', 'INR')
    
    # Convert to integers if they're floats
    if min_salary is not None:
        min_salary = int(min_salary)
    if max_salary is not None:
        max_salary = int(max_salary)
    
    return (min_salary, max_salary, currency)


def backfill(dry_run: bool = False, limit: Optional[int] = None):
    """Backfill experience and salary fields for existing jobs."""
    
    # Create engine (use sync engine for scripts)
    settings = Settings()
    db_url = settings.DATABASE_URL.replace('+asyncpg', '').replace('postgresql+asyncpg', 'postgresql')
    if 'host.docker.internal' in db_url:
        db_url = db_url.replace('host.docker.internal', 'localhost')
    
    engine = create_engine(db_url)
    
    print("\n" + "="*80)
    print("🔧 BACKFILLING EXPERIENCE AND SALARY FIELDS")
    print("="*80)
    print(f"Mode: {'DRY RUN (no changes will be saved)' if dry_run else 'LIVE (will update database)'}")
    if limit:
        print(f"Limit: {limit} jobs")
    print("="*80 + "\n")
    
    with Session(engine) as session:
        # Get all jobs
        query = select(Job)
        if limit:
            query = query.limit(limit)
        
        jobs = session.execute(query).scalars().all()
        total_jobs = len(jobs)
        
        print(f"📊 Found {total_jobs} jobs to process\n")
        
        # Statistics
        updated_count = 0
        experience_parsed = 0
        experience_null = 0
        experience_failed = 0
        salary_parsed = 0
        salary_null = 0
        freshers_found = 0
        
        for i, job in enumerate(jobs, 1):
            if i % 100 == 0:
                print(f"   Processing job {i}/{total_jobs}...")
            
            try:
                # Extract experience from description
                exp_text = extract_experience_from_description(job.description)
                
                # If experience_required field exists, prefer it
                if job.experience_required:
                    exp_text = job.experience_required
                
                # Parse experience
                exp_data = parse_experience(exp_text)
                
                if exp_data['min'] is not None:
                    experience_parsed += 1
                else:
                    experience_null += 1
                
                if exp_data['is_fresher']:
                    freshers_found += 1
                
                # Extract salary from description
                salary_from_desc = extract_salary_from_description(job.description)
                
                # If salary_range JSONB has data, prefer it
                if job.salary_range and isinstance(job.salary_range, dict) and (job.salary_range.get('min') or job.salary_range.get('max')):
                    min_sal = job.salary_range.get('min')
                    max_sal = job.salary_range.get('max')
                    currency = job.salary_range.get('currency', 'INR')
                else:
                    # Use extracted from description
                    min_sal = salary_from_desc['min']
                    max_sal = salary_from_desc['max']
                    currency = salary_from_desc['currency']
                
                if min_sal is not None or max_sal is not None:
                    salary_parsed += 1
                else:
                    salary_null += 1
                
                # Execute UPDATE statement
                stmt = (
                    update(Job)
                    .where(Job.id == job.id)
                    .values(
                        min_experience=exp_data['min'],
                        max_experience=exp_data['max'],
                        is_fresher=exp_data['is_fresher'],
                        min_salary=min_sal,
                        max_salary=max_sal,
                        salary_currency=currency
                    )
                )
                session.execute(stmt)
                updated_count += 1
                
            except Exception as e:
                print(f"❌ Error processing job {job.id}: {e}")
                experience_failed += 1
        
        # Commit all changes
        if not dry_run:
            session.commit()
            print("\n✅ Committed changes to database")
        else:
            session.rollback()
            print("\n🔍 Dry run complete - no changes saved")
        
        # Print statistics
        print("\n" + "="*80)
        print("📊 BACKFILL STATISTICS")
        print("="*80)
        print(f"Total jobs processed:      {total_jobs}")
        print(f"Successfully updated:      {updated_count}")
        print(f"Errors:                    {experience_failed}")
        print()
        print("Experience Parsing:")
        print(f"  Parsed successfully:     {experience_parsed} ({experience_parsed/total_jobs*100:.1f}%)")
        print(f"  Could not parse (NULL):  {experience_null} ({experience_null/total_jobs*100:.1f}%)")
        print(f"  Fresher jobs found:      {freshers_found} ({freshers_found/total_jobs*100:.1f}%)")
        print()
        print("Salary Parsing:")
        print(f"  Parsed successfully:     {salary_parsed} ({salary_parsed/total_jobs*100:.1f}%)")
        print(f"  No salary data (NULL):   {salary_null} ({salary_null/total_jobs*100:.1f}%)")
        print("="*80)
        
        # Show some examples (re-query to get updated values)
        print("\n📝 Sample Results:")
        print("-" * 80)
        sample_query = select(Job).limit(5)
        sample_jobs = session.execute(sample_query).scalars().all()
        for job in sample_jobs:
            print(f"\nJob ID: {job.id} | Title: {job.title}")
            
            # Show experience extraction
            exp_from_desc = extract_experience_from_description(job.description)
            print(f"  Experience (from field): {job.experience_required}")
            print(f"  Experience (extracted):  {exp_from_desc}")
            print(f"  → min={job.min_experience}, max={job.max_experience}, is_fresher={job.is_fresher}")
            
            # Show salary extraction  
            sal_from_desc = extract_salary_from_description(job.description)
            print(f"  Salary (from field): {job.salary_range}")
            print(f"  Salary (extracted):  min={sal_from_desc['min']}, max={sal_from_desc['max']}, currency={sal_from_desc['currency']}")
            print(f"  → min={job.min_salary}, max={job.max_salary}, currency={job.salary_currency}")
        
        print("\n" + "="*80)
        if not dry_run:
            print("✅ BACKFILL COMPLETE!")
        else:
            print("🔍 DRY RUN COMPLETE - Run without --dry-run to save changes")
        print("="*80 + "\n")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Backfill experience and salary fields')
    parser.add_argument('--dry-run', action='store_true', help='Run without saving changes')
    parser.add_argument('--limit', type=int, help='Limit number of jobs to process')
    
    args = parser.parse_args()
    
    backfill(dry_run=args.dry_run, limit=args.limit)
