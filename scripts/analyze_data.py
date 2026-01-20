"""
Data Analysis Script - MongoDB & PostgreSQL
Analyzes current data to understand patterns and improvement opportunities
"""

import os
import sys
from datetime import datetime
from typing import Dict, List
from collections import Counter
import re
from pymongo import MongoClient
from sqlalchemy.orm import Session

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.config import Settings
from app.db.session import get_sync_db
# Import all models to avoid relationship errors
from app.models import job as job_module, company as company_module, channel, user, student, application
from app.models.job import Job
from app.models.company import Company

settings = Settings()


class DataAnalyzer:
    """Analyze MongoDB and PostgreSQL data"""
    
    def __init__(self):
        # MongoDB connection
        self.mongo_client = MongoClient(settings.MONGODB_URI)
        self.mongo_db = self.mongo_client[settings.MONGODB_DB_NAME]
        self.messages_collection = self.mongo_db["raw_messages"]
        
        # PostgreSQL connection
        self.db = next(get_sync_db())
    
    def analyze_mongodb_messages(self, limit: int = 100):
        """Analyze raw messages from MongoDB"""
        print("\n" + "=" * 100)
        print("📊 MONGODB RAW MESSAGES ANALYSIS")
        print("=" * 100)
        
        # Get sample messages
        job_messages = list(self.messages_collection.find(
            {"is_processed": True, "ml_classification.is_job": True}
        ).limit(limit))
        
        non_job_messages = list(self.messages_collection.find(
            {"is_processed": True, "ml_classification.is_job": False}
        ).limit(20))
        
        print(f"\n📈 Sample Size:")
        print(f"   Job messages: {len(job_messages)}")
        print(f"   Non-job messages: {len(non_job_messages)}")
        
        # Analyze job message patterns
        print(f"\n🔍 JOB MESSAGE PATTERNS:")
        print("=" * 100)
        
        company_patterns = []
        salary_found = 0
        experience_found = 0
        email_found = 0
        multiple_jobs = 0
        has_apply_link = 0
        
        for msg in job_messages[:20]:  # Detailed analysis of first 20
            text = msg.get("text", "")
            
            print(f"\n{'─' * 100}")
            print(f"Message ID: {msg.get('message_id')}")
            print(f"Channel: {msg.get('channel_username', 'Unknown')}")
            print(f"Confidence: {msg.get('ml_classification', {}).get('confidence', 'N/A')}")
            print(f"Length: {len(text)} chars")
            print(f"\n📝 TEXT:")
            print(text[:500] + ("..." if len(text) > 500 else ""))
            
            # Check for multiple job listings
            job_count = self._count_jobs_in_message(text)
            if job_count > 1:
                multiple_jobs += 1
                print(f"\n⚠️  MULTIPLE JOBS DETECTED: {job_count} jobs in one message")
            
            # Check for company names
            companies = self._extract_companies(text)
            if companies:
                company_patterns.extend(companies)
                print(f"\n🏢 Companies found: {', '.join(companies)}")
            
            # Check for salary
            salary = self._extract_salary_patterns(text)
            if salary:
                salary_found += 1
                print(f"\n💰 Salary: {salary}")
            
            # Check for experience
            exp = self._extract_experience_patterns(text)
            if exp:
                experience_found += 1
                print(f"\n📊 Experience: {exp}")
            
            # Check for email
            emails = self._extract_emails(text)
            if emails:
                email_found += 1
                print(f"\n📧 Email: {', '.join(emails)}")
            
            # Check for apply links
            if msg.get("links"):
                has_apply_link += 1
                print(f"\n🔗 Apply links: {len(msg.get('links', []))} found")
        
        # Summary statistics
        print(f"\n\n📊 STATISTICS (from {len(job_messages)} job messages):")
        print("=" * 100)
        print(f"Messages with multiple jobs: {multiple_jobs} ({multiple_jobs/len(job_messages)*100:.1f}%)")
        print(f"Messages with salary: {salary_found} ({salary_found/20*100:.1f}%)")
        print(f"Messages with experience: {experience_found} ({experience_found/20*100:.1f}%)")
        print(f"Messages with email: {email_found} ({email_found/20*100:.1f}%)")
        print(f"Messages with apply links: {has_apply_link} ({has_apply_link/20*100:.1f}%)")
        
        # Company name patterns
        if company_patterns:
            print(f"\n🏢 TOP COMPANIES (from patterns):")
            company_counter = Counter(company_patterns)
            for company, count in company_counter.most_common(15):
                print(f"   {company}: {count}")
        
        # Sample non-job messages
        print(f"\n\n❌ NON-JOB MESSAGE SAMPLES:")
        print("=" * 100)
        for msg in non_job_messages[:5]:
            text = msg.get("text", "")
            print(f"\n{'─' * 100}")
            print(f"Message ID: {msg.get('message_id')}")
            print(f"Text: {text[:300] + ('...' if len(text) > 300 else '')}")
        
        return {
            "total_job_messages": len(job_messages),
            "multiple_jobs_pct": multiple_jobs/len(job_messages)*100 if job_messages else 0,
            "salary_found_pct": salary_found/20*100,
            "experience_found_pct": experience_found/20*100,
            "email_found_pct": email_found/20*100,
            "company_patterns": company_counter.most_common(10) if company_patterns else []
        }
    
    def analyze_postgresql_jobs(self):
        """Analyze jobs in PostgreSQL"""
        print("\n\n" + "=" * 100)
        print("🗄️  POSTGRESQL JOBS ANALYSIS")
        print("=" * 100)
        
        total_jobs = self.db.query(Job).count()
        jobs_with_company = self.db.query(Job).filter(Job.company_id.isnot(None)).count()
        jobs_without_company = total_jobs - jobs_with_company
        
        # Get sample jobs
        sample_jobs = self.db.query(Job).limit(20).all()
        
        print(f"\n📊 STATISTICS:")
        print(f"   Total jobs: {total_jobs}")
        print(f"   Jobs with company_id: {jobs_with_company} ({jobs_with_company/total_jobs*100 if total_jobs else 0:.1f}%)")
        print(f"   Jobs without company_id: {jobs_without_company} ({jobs_without_company/total_jobs*100 if total_jobs else 0:.1f}%)")
        
        # Analyze fields
        missing_title = 0
        missing_location = 0
        missing_experience = 0
        missing_salary = 0
        missing_skills = 0
        has_email = 0
        
        print(f"\n\n🔍 SAMPLE JOBS:")
        print("=" * 100)
        
        for job in sample_jobs:
            print(f"\n{'─' * 100}")
            print(f"Job ID: {job.id}")
            print(f"Title: {job.title or '❌ MISSING'}")
            print(f"Company ID: {job.company_id or '❌ MISSING'}")
            print(f"Location: {job.location or '❌ MISSING'}")
            print(f"Experience: {job.experience_required or '❌ MISSING'}")
            print(f"Salary: {job.salary_range or '❌ MISSING'}")
            print(f"Skills: {job.skills_required[:3] if job.skills_required else '❌ MISSING'}")
            print(f"ML Confidence: {job.ml_confidence}")
            print(f"Source: {job.source}")
            
            # Check raw text for email
            if job.raw_text:
                emails = self._extract_emails(job.raw_text)
                if emails:
                    has_email += 1
                    print(f"📧 Email in raw text: {', '.join(emails)}")
                
                print(f"\n📝 Raw text preview:")
                print(job.raw_text[:300] + ("..." if len(job.raw_text) > 300 else ""))
            
            if not job.title or job.title == "Job Opening":
                missing_title += 1
            if not job.location:
                missing_location += 1
            if not job.experience_required:
                missing_experience += 1
            if not job.salary_range or not job.salary_range.get("raw"):
                missing_salary += 1
            if not job.skills_required:
                missing_skills += 1
        
        print(f"\n\n📉 MISSING DATA (from {len(sample_jobs)} samples):")
        print("=" * 100)
        print(f"Missing/Generic title: {missing_title} ({missing_title/len(sample_jobs)*100:.1f}%)")
        print(f"Missing location: {missing_location} ({missing_location/len(sample_jobs)*100:.1f}%)")
        print(f"Missing experience: {missing_experience} ({missing_experience/len(sample_jobs)*100:.1f}%)")
        print(f"Missing salary: {missing_salary} ({missing_salary/len(sample_jobs)*100:.1f}%)")
        print(f"Missing skills: {missing_skills} ({missing_skills/len(sample_jobs)*100:.1f}%)")
        print(f"Has email in raw text: {has_email} ({has_email/len(sample_jobs)*100:.1f}%)")
        
        return {
            "total_jobs": total_jobs,
            "missing_company_pct": jobs_without_company/total_jobs*100 if total_jobs else 0,
            "missing_title_pct": missing_title/len(sample_jobs)*100,
            "missing_location_pct": missing_location/len(sample_jobs)*100,
            "missing_experience_pct": missing_experience/len(sample_jobs)*100,
            "missing_salary_pct": missing_salary/len(sample_jobs)*100,
            "has_email_pct": has_email/len(sample_jobs)*100
        }
    
    def analyze_companies(self):
        """Analyze companies table"""
        print("\n\n" + "=" * 100)
        print("🏢 COMPANIES TABLE ANALYSIS")
        print("=" * 100)
        
        total_companies = self.db.query(Company).count()
        companies = self.db.query(Company).limit(20).all()
        
        print(f"\n📊 Total companies: {total_companies}")
        
        if companies:
            print(f"\n🏢 SAMPLE COMPANIES:")
            for company in companies:
                print(f"\n   • {company.name}")
                print(f"     Jobs: {len(company.jobs) if company.jobs else 0}")
                print(f"     Verified: {company.is_verified}")
        else:
            print("\n⚠️  No companies in database yet")
        
        return {"total_companies": total_companies}
    
    # Helper methods
    
    def _count_jobs_in_message(self, text: str) -> int:
        """Count number of job listings in a message"""
        # Look for patterns that indicate multiple jobs
        patterns = [
            r'\n\d+[\.\)]\s+',  # Numbered lists: 1. 2. 3.
            r'\n[•●◦]\s+',  # Bullet points
            r'(?:role|position)\s*\d+\s*:',  # Role 1:, Position 2:
        ]
        
        max_count = 1
        for pattern in patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            if matches:
                max_count = max(max_count, len(matches) + 1)
        
        return max_count
    
    def _extract_companies(self, text: str) -> List[str]:
        """Extract company names using patterns"""
        companies = []
        
        patterns = [
            r'(?:Company|Organization|Hiring for)\s*:\s*([A-Z][A-Za-z0-9\s&]+?)(?:\n|,|\.)',
            r'@\s*([A-Z][A-Za-z0-9\s&]+?)(?:\n|is hiring)',
            r'([A-Z][A-Za-z0-9\s&]{2,30})\s+(?:is hiring|hiring for|looking for)',
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, text)
            companies.extend([m.strip() for m in matches if len(m.strip()) > 2])
        
        return list(set(companies))[:5]  # Return unique, max 5
    
    def _extract_salary_patterns(self, text: str) -> str:
        """Extract salary with improved patterns"""
        patterns = [
            r'(?:salary|ctc|package|compensation)\s*:?\s*(\d+(?:\.\d+)?(?:\s*-\s*\d+(?:\.\d+)?)?\s*(?:lpa|lakh|lakhs?|k|l))',
            r'(₹\s*\d+(?:,\d+)*(?:\s*-\s*₹?\s*\d+(?:,\d+)*)?(?:\s*(?:lpa|lakh|lakhs?|per\s*annum))?)',
            r'(\d+(?:\.\d+)?(?:\s*-\s*\d+(?:\.\d+)?)?\s*(?:lpa|lakh|lakhs?))',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                return match.group(1).strip()
        
        return None
    
    def _extract_experience_patterns(self, text: str) -> str:
        """Extract experience with improved patterns"""
        patterns = [
            r'(?:experience|exp)\s*:?\s*(\d+(?:\+|plus)?\s*(?:to|-)\s*\d+\s*(?:years?|yrs?))',
            r'(?:experience|exp)\s*:?\s*(\d+(?:\+|plus)?\s*(?:years?|yrs?))',
            r'(\d+(?:\+|plus)?\s*(?:to|-)\s*\d+\s*(?:years?|yrs?))\s+(?:of\s+)?experience',
            r'(fresher|entry\s*level|0\s*(?:to|-)\s*\d+\s*(?:years?|yrs?))',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                return match.group(1).strip()
        
        return None
    
    def _extract_emails(self, text: str) -> List[str]:
        """Extract email addresses"""
        pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
        emails = re.findall(pattern, text)
        return list(set(emails))
    
    def generate_summary_report(self):
        """Generate comprehensive summary"""
        print("\n\n" + "=" * 100)
        print("📋 COMPREHENSIVE ANALYSIS SUMMARY")
        print("=" * 100)
        
        mongo_stats = self.analyze_mongodb_messages()
        psql_stats = self.analyze_postgresql_jobs()
        company_stats = self.analyze_companies()
        
        print("\n\n" + "=" * 100)
        print("🎯 KEY FINDINGS & IMPROVEMENT OPPORTUNITIES")
        print("=" * 100)
        
        print("\n1️⃣  MULTIPLE JOBS IN SINGLE MESSAGE:")
        print(f"   • {mongo_stats['multiple_jobs_pct']:.1f}% of messages contain multiple job listings")
        print("   • Need to: Split messages and create separate job entries")
        
        print("\n2️⃣  COMPANY EXTRACTION:")
        print(f"   • {psql_stats['missing_company_pct']:.1f}% of jobs missing company_id")
        print("   • Need to: Improve company name extraction and auto-create companies")
        
        print("\n3️⃣  SALARY EXTRACTION:")
        print(f"   • {psql_stats['missing_salary_pct']:.1f}% of jobs missing salary")
        print(f"   • But {mongo_stats['salary_found_pct']:.1f}% of raw messages have salary info")
        print("   • Need to: Improve salary pattern matching")
        
        print("\n4️⃣  EXPERIENCE EXTRACTION:")
        print(f"   • {psql_stats['missing_experience_pct']:.1f}% of jobs missing experience")
        print(f"   • But {mongo_stats['experience_found_pct']:.1f}% of raw messages have experience info")
        print("   • Need to: Better experience pattern recognition")
        
        print("\n5️⃣  EMAIL/CONTACT EXTRACTION:")
        print(f"   • {psql_stats['has_email_pct']:.1f}% of jobs have email in raw text")
        print("   • Need to: Extract and store contact emails in job table")
        
        print("\n6️⃣  JOB TITLE EXTRACTION:")
        print(f"   • {psql_stats['missing_title_pct']:.1f}% of jobs have missing/generic titles")
        print("   • Need to: Improve job title pattern matching")
        
        print("\n" + "=" * 100)
    
    def close(self):
        """Close connections"""
        self.mongo_client.close()
        self.db.close()


if __name__ == "__main__":
    print("\n🚀 Starting Data Analysis...")
    print("This will analyze your MongoDB and PostgreSQL data to identify improvement opportunities")
    
    analyzer = DataAnalyzer()
    
    try:
        analyzer.generate_summary_report()
    finally:
        analyzer.close()
    
    print("\n\n✅ Analysis complete!")
    print("\n💡 Next steps:")
    print("   1. Review the findings above")
    print("   2. Discuss improvement strategy")
    print("   3. Prioritize which features to implement first")
    print("   4. Design improved extraction logic")
    print("   5. Update ML model training data")
