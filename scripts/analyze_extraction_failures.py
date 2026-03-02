"""
Analyze extraction failures and patterns in MongoDB and PostgreSQL data.
This script helps understand real Telegram job posting patterns and current extraction quality.
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import re
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from typing import Dict, List, Tuple

from pymongo import MongoClient
from app.db.session import SyncSessionLocal
from app.config import settings
from sqlalchemy import text


def analyze_mongodb_patterns():
    """Analyze patterns in MongoDB raw_messages."""
    print("\n" + "="*80)
    print("ANALYZING MONGODB RAW_MESSAGES")
    print("="*80)
    
    client = MongoClient(settings.MONGODB_URI)
    mongo_db = client[settings.MONGODB_DATABASE]
    raw_messages = mongo_db.raw_messages
    
    # Get sample of job and non-job messages
    job_messages = list(raw_messages.find(
        {"classification": "job", "processed": True}
    ).limit(100))
    
    non_job_messages = list(raw_messages.find(
        {"classification": "non-job", "processed": True}
    ).limit(100))
    
    print(f"\n📊 Sample Size:")
    print(f"  - Job messages: {len(job_messages)}")
    print(f"  - Non-job messages: {len(non_job_messages)}")
    
    # Analyze company name patterns
    print("\n🏢 COMPANY NAME PATTERNS:")
    company_patterns = {
        '@mention': 0,
        '"quoted"': 0,
        'Company: X': 0,
        'Join X': 0,
        'is hiring': 0,
        'first_line': 0,
        'none_detected': 0
    }
    
    company_samples = []
    
    for msg in job_messages[:50]:
        text = msg.get('text', '')
        lines = text.split('\n')
        first_line = lines[0] if lines else ''
        
        # Check patterns
        if '@' in text:
            company_patterns['@mention'] += 1
            # Extract sample
            match = re.search(r'@(\w+)', text)
            if match:
                company_samples.append(('@mention', match.group(1), first_line[:80]))
        
        if re.search(r'"([^"]+)"', text):
            company_patterns['"quoted"'] += 1
            match = re.search(r'"([^"]+)"', text)
            if match:
                company_samples.append(('"quoted"', match.group(1), first_line[:80]))
        
        if re.search(r'Company[:\s]+([A-Z][a-zA-Z\s]+)', text, re.I):
            company_patterns['Company: X'] += 1
        
        if re.search(r'Join\s+([A-Z][a-zA-Z\s]+)', text, re.I):
            company_patterns['Join X'] += 1
        
        if re.search(r'(\w+)\s+is\s+hiring', text, re.I):
            company_patterns['is hiring'] += 1
            match = re.search(r'(\w+)\s+is\s+hiring', text, re.I)
            if match:
                company_samples.append(('is hiring', match.group(1), first_line[:80]))
        
        if len(first_line) > 5 and len(first_line) < 50:
            company_patterns['first_line'] += 1
    
    for pattern, count in company_patterns.items():
        percentage = (count / 50) * 100 if count > 0 else 0
        print(f"  - {pattern}: {count}/50 ({percentage:.1f}%)")
    
    print("\n  Sample Company Extractions:")
    for pattern, company, context in company_samples[:10]:
        print(f"    [{pattern}] {company}")
        print(f"      Context: {context}")
    
    # Analyze location patterns
    print("\n📍 LOCATION PATTERNS:")
    location_patterns = {
        'emoji_📍': 0,
        'Location:': 0,
        'Based in': 0,
        'city_name': 0,
        'international': 0,
        'remote': 0,
        'onsite_only': 0
    }
    
    international_keywords = {'usa', 'uk', 'canada', 'singapore', 'dubai', 'germany', 'australia'}
    onsite_keywords = {'onsite only', 'office only', 'no remote', 'work from office'}
    
    location_samples = []
    
    for msg in job_messages[:50]:
        text = msg.get('text', '').lower()
        
        if '📍' in text:
            location_patterns['emoji_📍'] += 1
            match = re.search(r'📍\s*([^\n]+)', text)
            if match:
                location_samples.append(('📍', match.group(1).strip()[:50]))
        
        if 'location:' in text:
            location_patterns['Location:'] += 1
        
        if 'based in' in text:
            location_patterns['Based in'] += 1
        
        # Check for international
        if any(keyword in text for keyword in international_keywords):
            location_patterns['international'] += 1
            # Check if onsite only
            if any(keyword in text for keyword in onsite_keywords):
                location_patterns['onsite_only'] += 1
                location_samples.append(('International Onsite', text[:100]))
        
        if 'remote' in text or 'wfh' in text:
            location_patterns['remote'] += 1
    
    for pattern, count in location_patterns.items():
        percentage = (count / 50) * 100 if count > 0 else 0
        print(f"  - {pattern}: {count}/50 ({percentage:.1f}%)")
    
    print("\n  Sample Locations:")
    for pattern, location in location_samples[:8]:
        print(f"    [{pattern}] {location}")
    
    # Analyze salary patterns
    print("\n💰 SALARY PATTERNS:")
    salary_patterns = {
        'LPA_range': 0,
        'LPA_single': 0,
        'monthly_k': 0,
        'monthly_rupees': 0,
        'upto_X': 0,
        'none': 0
    }
    
    salary_samples = []
    
    for msg in job_messages[:50]:
        text = msg.get('text', '')
        
        if re.search(r'(\d+\.?\d*)\s*-\s*(\d+\.?\d*)\s*(?:LPA|lpa)', text):
            salary_patterns['LPA_range'] += 1
            match = re.search(r'(\d+\.?\d*)\s*-\s*(\d+\.?\d*)\s*(?:LPA|lpa)', text)
            if match:
                salary_samples.append(('LPA range', f"{match.group(1)}-{match.group(2)} LPA"))
        elif re.search(r'(\d+\.?\d*)\s*(?:LPA|lpa)', text):
            salary_patterns['LPA_single'] += 1
            match = re.search(r'(\d+\.?\d*)\s*(?:LPA|lpa)', text)
            if match:
                salary_samples.append(('LPA single', f"{match.group(1)} LPA"))
        elif re.search(r'(\d+)k', text, re.I):
            salary_patterns['monthly_k'] += 1
        elif re.search(r'₹\s*(\d+)', text):
            salary_patterns['monthly_rupees'] += 1
        elif re.search(r'upto\s+(\d+\.?\d*)\s*(?:LPA|lpa)', text, re.I):
            salary_patterns['upto_X'] += 1
        
        # Check if no salary found
        has_salary = any(re.search(pattern, text, re.I) for pattern in [
            r'\d+\.?\d*\s*(?:LPA|lpa)',
            r'\d+k',
            r'₹\s*\d+',
            r'salary'
        ])
        if not has_salary:
            salary_patterns['none'] += 1
    
    for pattern, count in salary_patterns.items():
        percentage = (count / 50) * 100 if count > 0 else 0
        print(f"  - {pattern}: {count}/50 ({percentage:.1f}%)")
    
    print("\n  Sample Salaries:")
    for pattern, salary in salary_samples[:8]:
        print(f"    [{pattern}] {salary}")
    
    # Analyze experience patterns
    print("\n🎓 EXPERIENCE PATTERNS:")
    experience_patterns = {
        'X-Y years': 0,
        'X+ years': 0,
        'fresher': 0,
        'Experience:': 0,
        'none': 0
    }
    
    for msg in job_messages[:50]:
        text = msg.get('text', '').lower()
        
        if re.search(r'(\d+)\s*-\s*(\d+)\s*(?:years|yrs)', text):
            experience_patterns['X-Y years'] += 1
        elif re.search(r'(\d+)\+\s*(?:years|yrs)', text):
            experience_patterns['X+ years'] += 1
        elif 'fresher' in text or 'freshers' in text:
            experience_patterns['fresher'] += 1
        elif 'experience:' in text:
            experience_patterns['Experience:'] += 1
        else:
            experience_patterns['none'] += 1
    
    for pattern, count in experience_patterns.items():
        percentage = (count / 50) * 100 if count > 0 else 0
        print(f"  - {pattern}: {count}/50 ({percentage:.1f}%)")
    
    # Analyze message formats
    print("\n📝 MESSAGE FORMAT ANALYSIS:")
    format_patterns = {
        'structured_emoji': 0,  # Multiple emojis, sections
        'compact_pipes': 0,     # @mention | location | salary
        'minimal': 0,           # Few lines, basic info
        'multiple_jobs': 0,     # Multiple jobs in one message
    }
    
    for msg in job_messages[:50]:
        text = msg.get('text', '')
        lines = text.split('\n')
        emoji_count = len(re.findall(r'[\U0001F300-\U0001F9FF]', text))
        pipe_count = text.count('|')
        
        if emoji_count >= 3:
            format_patterns['structured_emoji'] += 1
        
        if pipe_count >= 2 and '@' in text:
            format_patterns['compact_pipes'] += 1
        
        if len(lines) <= 5:
            format_patterns['minimal'] += 1
        
        if re.search(r'(?:1\.|2\.|3\.)', text) or text.count('hiring') > 1:
            format_patterns['multiple_jobs'] += 1
    
    for pattern, count in format_patterns.items():
        percentage = (count / 50) * 100 if count > 0 else 0
        print(f"  - {pattern}: {count}/50 ({percentage:.1f}%)")
    
    client.close()


def analyze_postgresql_jobs():
    """Analyze extraction quality in PostgreSQL jobs table."""
    print("\n" + "="*80)
    print("ANALYZING POSTGRESQL JOBS")
    print("="*80)
    
    db = SyncSessionLocal()
    
    # Get recent jobs
    result = db.execute(text("""
        SELECT 
            COUNT(*) as total_jobs,
            COUNT(company_id) as with_company,
            COUNT(salary_min) as with_salary_min,
            COUNT(salary_max) as with_salary_max,
            COUNT(experience_min) as with_experience,
            COUNT(location) as with_location,
            COUNT(skills_required) as with_skills,
            AVG(quality_score) as avg_quality
        FROM jobs
        WHERE created_at > NOW() - INTERVAL '7 days'
    """))
    
    row = result.fetchone()
    
    print(f"\n📊 Recent Jobs (Last 7 Days):")
    print(f"  - Total jobs: {row.total_jobs}")
    
    if row.total_jobs > 0:
        print(f"\n📈 Field Completeness:")
        print(f"  - Company: {row.with_company}/{row.total_jobs} ({(row.with_company/row.total_jobs)*100:.1f}%)")
        print(f"  - Salary (min): {row.with_salary_min}/{row.total_jobs} ({(row.with_salary_min/row.total_jobs)*100:.1f}%)")
        print(f"  - Salary (max): {row.with_salary_max}/{row.total_jobs} ({(row.with_salary_max/row.total_jobs)*100:.1f}%)")
        print(f"  - Experience: {row.with_experience}/{row.total_jobs} ({(row.with_experience/row.total_jobs)*100:.1f}%)")
        print(f"  - Location: {row.with_location}/{row.total_jobs} ({(row.with_location/row.total_jobs)*100:.1f}%)")
        print(f"  - Skills: {row.with_skills}/{row.total_jobs} ({(row.with_skills/row.total_jobs)*100:.1f}%)")
        print(f"  - Avg Quality Score: {row.avg_quality:.2f}/100" if row.avg_quality else "  - Avg Quality Score: N/A")
    
    # Get sample jobs without company
    result = db.execute(text("""
        SELECT id, job_title, description, source_url
        FROM jobs
        WHERE company_id IS NULL
        AND created_at > NOW() - INTERVAL '7 days'
        LIMIT 10
    """))
    
    jobs_without_company = result.fetchall()
    
    print(f"\n❌ Sample Jobs Without Company ({len(jobs_without_company)}):")
    for job in jobs_without_company[:5]:
        print(f"\n  Job ID: {job.id}")
        print(f"  Title: {job.job_title}")
        first_line = job.description.split('\n')[0] if job.description else "N/A"
        print(f"  First Line: {first_line[:100]}")
    
    # Check international jobs
    result = db.execute(text("""
        SELECT COUNT(*) as count
        FROM jobs
        WHERE created_at > NOW() - INTERVAL '7 days'
        AND (
            location ILIKE '%usa%' OR
            location ILIKE '%uk%' OR
            location ILIKE '%canada%' OR
            location ILIKE '%singapore%' OR
            location ILIKE '%dubai%' OR
            location ILIKE '%germany%' OR
            location ILIKE '%australia%'
        )
    """))
    
    international_count = result.fetchone().count
    
    print(f"\n🌍 International Jobs:")
    print(f"  - Count: {international_count}")
    
    if international_count > 0:
        result = db.execute(text("""
            SELECT id, job_title, location, work_type
            FROM jobs
            WHERE created_at > NOW() - INTERVAL '7 days'
            AND (
                location ILIKE '%usa%' OR
                location ILIKE '%uk%' OR
                location ILIKE '%canada%' OR
                location ILIKE '%singapore%' OR
                location ILIKE '%dubai%'
            )
            LIMIT 5
        """))
        
        print("\n  Sample International Jobs:")
        for job in result.fetchall():
            print(f"    - {job.job_title} | {job.location} | {job.work_type or 'N/A'}")
    
    db.close()


def generate_recommendations():
    """Generate recommendations based on analysis."""
    print("\n" + "="*80)
    print("RECOMMENDATIONS")
    print("="*80)
    
    print("\n🎯 Priority Improvements:")
    print("\n1. Company Extraction:")
    print("   - Add @mention pattern (high priority)")
    print("   - Add quoted company name pattern")
    print("   - Add 'X is hiring' pattern")
    print("   - Improve first line company detection")
    
    print("\n2. International Filtering:")
    print("   - Enforce filtering during extraction phase")
    print("   - Reject international + onsite + not remote immediately")
    print("   - Keep international + remote jobs")
    
    print("\n3. Salary Simplification:")
    print("   - Prefer single value over range")
    print("   - Convert all to monthly INR")
    print("   - Handle 'upto X LPA' as X LPA")
    
    print("\n4. Training Data:")
    print("   - Add @mention format examples (20+)")
    print("   - Add emoji-structured format examples (20+)")
    print("   - Add compact pipe-separated examples (20+)")
    print("   - Add minimal format examples (20+)")
    print("   - Add international onsite rejection examples (10+)")


if __name__ == "__main__":
    print("\n🔍 ML EXTRACTION PATTERN ANALYSIS")
    print("="*80)
    print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    try:
        # Analyze MongoDB patterns
        analyze_mongodb_patterns()
        
        # Analyze PostgreSQL jobs
        analyze_postgresql_jobs()
        
        # Generate recommendations
        generate_recommendations()
        
        print("\n" + "="*80)
        print("✅ ANALYSIS COMPLETE")
        print("="*80)
        
    except Exception as e:
        print(f"\n❌ Error during analysis: {str(e)}")
        import traceback
        traceback.print_exc()
