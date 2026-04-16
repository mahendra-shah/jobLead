"""
Run the complete job processing pipeline.
Processes pending messages from storage -> ML classification -> Deduplication -> Job creation
"""

import sys
import asyncio
from datetime import datetime, timedelta
import traceback
import re
import hashlib

from app.db.session import AsyncSessionLocal
from sqlalchemy import select

# Import all models BEFORE Job/Company to resolve SQLAlchemy relationships
from app.models.user import User
from app.models.student import Student  
from app.models.application import Application
from app.models.channel import Channel
from app.models.company import Company
from app.models.job import Job

from app.services.storage_factory import get_storage_service
from app.ml import job_classifier


async def process_pending_jobs(limit=100):
    """
    Process pending messages from storage.
    
    Pipeline:
    1. Fetch pending messages from storage
    2. ML classification (sklearn + spaCy)
    3. Deduplication check
    4. Create job in database
    5. Update storage status
    """
    print(f"\n🚀 Starting Job Processing Pipeline (limit={limit})")
    print("=" * 70)
    
    async with AsyncSessionLocal() as db:
        storage = get_storage_service()
        
        results = {
            "status": "success",
            "messages_processed": 0,
            "jobs_created": 0,
            "duplicates_skipped": 0,
            "non_jobs_rejected": 0,
            "low_confidence_jobs": 0,
            "errors": 0,
            "error_details": [],
            "created_job_ids": []
        }
        
        try:
            # Get pending messages
            print(f"\n📥 Step 1: Fetching pending messages...")
            messages = await storage.get_pending_messages(limit=limit)
            print(f"   Found {len(messages)} pending messages")
            
            if not messages:
                print("   No pending messages to process")
                return results
            
            print(f"\n🔄 Step 2: Processing {len(messages)} messages...")
            print()
            
            for idx, msg in enumerate(messages, 1):
                try:
                    print(f"[{idx}/{len(messages)}] Message {msg.get('message_id')}...", end=" ")
                    
                    # Extract text (MongoDB uses 'content', local JSON might use 'text')
                    text = msg.get("content") or msg.get("text", "")
                    text = text.strip() if text else ""
                    
                    if not text:
                        print("⚠️ Empty")
                        await storage.mark_rejected(msg["message_id"], "Empty message text")
                        results["non_jobs_rejected"] += 1
                        results["messages_processed"] += 1
                        continue
                    
                    # ML Classification
                    classification = job_classifier.classify(text)
                    
                    if not classification.is_job:
                        print(f"❌ Non-job ({classification.confidence:.0%})")
                        await storage.mark_rejected(
                            msg["message_id"],
                            f"ML: Non-job (confidence: {classification.confidence:.2%})"
                        )
                        results["non_jobs_rejected"] += 1
                        results["messages_processed"] += 1
                        continue
                    
                    # Extract job details
                    extraction = job_classifier.extract(text)
                    
                    # Compute content hash for deduplication
                    normalized_text = ' '.join(text.lower().split())
                    content_hash = hashlib.md5(normalized_text.encode()).hexdigest()
                    
                    # Check for exact hash duplicate in last 7 days
                    cutoff_date = datetime.utcnow() - timedelta(days=7)
                    dup_result = await db.execute(
                        select(Job).where(
                            Job.content_hash == content_hash,
                            Job.created_at >= cutoff_date,
                            Job.is_active == True
                        )
                    )
                    duplicate = dup_result.scalar_one_or_none()
                    
                    if duplicate:
                        print(f"⚠️ Duplicate (Job #{duplicate.id})")
                        await storage.mark_as_duplicate(msg["message_id"], str(duplicate.id))
                        results["duplicates_skipped"] += 1
                        results["messages_processed"] += 1
                        continue
                    
                    # Extract vacancy count
                    vacancy_count = 1
                    patterns = [
                        r'(\d+)\s*(?:vacancies|openings|positions)',
                        r'hiring\s*(\d+)',
                        r'(\d+)\s*(?:spots|seats)'
                    ]
                    for pattern in patterns:
                        match = re.search(pattern, text, re.IGNORECASE)
                        if match:
                            vacancy_count = int(match.group(1))
                            break
                    
                    # Get or create company
                    company_name = extraction.company or "Unknown Company"
                    result = await db.execute(
                        select(Company).where(Company.name == company_name)
                    )
                    company = result.scalar_one_or_none()
                    
                    if not company:
                        company = Company(
                            name=company_name,
                            description="Company from job posting",
                            website="",
                            logo_url=None
                        )
                        db.add(company)
                        await db.flush()
                    
                    # Compute content hash
                    normalized_text = ' '.join(text.lower().split())
                    content_hash = hashlib.md5(normalized_text.encode()).hexdigest()
                    
                    # Create job
                    job = Job(
                        company_id=company.id,
                        title=extraction.job_title or "Position Available",
                        description=text,
                        skills_required=extraction.skills or [],
                        experience_required=extraction.experience_required or "",
                        salary_range={"text": extraction.salary} if extraction.salary else {},
                        location=extraction.location or "Not specified",
                        job_type=extraction.job_type or "remote",
                        employment_type="fulltime",
                        source="telegram",
                        raw_text=text,
                        # New ML and visibility tracking fields
                        ml_confidence=f"{classification.confidence:.2f}",
                        content_hash=content_hash,
                        source_message_id=msg.get("message_id"),
                        vacancy_count=vacancy_count,
                        max_students_to_show=vacancy_count * 10,
                        visibility_mode='all',  # Default: show to all matching students
                        students_shown_to=[],
                        # Status fields
                        is_active=True,
                        is_verified=classification.confidence >= 0.90,
                        view_count=0,
                        application_count=0
                    )
                    
                    db.add(job)
                    await db.commit()
                    
                    # Mark as processed
                    await storage.mark_processed(msg["message_id"], str(job.id))
                    
                    results["jobs_created"] += 1
                    results["messages_processed"] += 1
                    results["created_job_ids"].append(str(job.id))
                    
                    if classification.confidence < 0.80:
                        results["low_confidence_jobs"] += 1
                    
                    verified_status = "✓" if job.is_verified else ""
                    print(f"✅ Job #{job.id} created {verified_status} ({classification.confidence:.0%})")
                    
                except Exception as e:
                    error_msg = f"Message {msg.get('message_id')}: {str(e)}"
                    print(f"❌ Error: {str(e)}")
                    results["errors"] += 1
                    results["error_details"].append(error_msg)
                    await db.rollback()
                    continue
            
            return results
            
        except Exception as e:
            print(f"\n❌ Fatal error: {str(e)}")
            traceback.print_exc()
            results["status"] = "error"
            results["error"] = str(e)
            return results


async def main():
    """Main entry point."""
    print("\n" + "=" * 70)
    print("Job Processing Pipeline")
    print("=" * 70)
    
    # Run pipeline
    result = await process_pending_jobs(limit=50)
    
    # Display results
    print("\n" + "=" * 70)
    print("📊 Pipeline Results")
    print("=" * 70)
    
    if result.get("status") == "success":
        print(f"✅ Status: SUCCESS")
        print(f"   Messages processed: {result['messages_processed']}")
        print(f"   Jobs created: {result['jobs_created']}")
        print(f"   Duplicates skipped: {result['duplicates_skipped']}")
        print(f"   Non-jobs rejected: {result['non_jobs_rejected']}")
        print(f"   Low confidence (<80%): {result['low_confidence_jobs']}")
        print(f"   Errors: {result['errors']}")
        
        if result.get('error_details'):
            print("\n⚠️  Error Details:")
            for error in result['error_details'][:5]:  # Show first 5
                print(f"   - {error}")
        
        # Show created jobs
        if result['created_job_ids']:
            print(f"\n✅ Created {len(result['created_job_ids'])} jobs:")
            
            async with AsyncSessionLocal() as db:
                for job_id in result['created_job_ids'][:10]:  # Show first 10
                    result_row = await db.execute(
                        select(Job).where(Job.id == job_id)
                    )
                    job = result_row.scalar_one_or_none()
                    if job:
                        verified = "✓" if job.is_verified else ""
                        print(f"   [{job.ml_confidence}] {job.title} @ {job.location} {verified}")
                        print(f"        Vacancies: {job.vacancy_count}")
        
        success = result['errors'] == 0
        
    else:
        print(f"❌ Status: ERROR")
        print(f"   Error: {result.get('error')}")
        success = False
    
    print("\n" + "=" * 70)
    
    if success:
        print("✅ Pipeline completed successfully!")
    else:
        print("⚠️  Pipeline completed with errors")
    
    print("=" * 70 + "\n")
    
    return 0 if success else 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
