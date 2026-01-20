"""Job processing tasks with ML classification."""

import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional

from app.workers.celery_app import celery_app
from app.workers.optimized_celery import get_job_classifier, log_memory_usage
from app.utils.job_parser import parse_experience, extract_salary_from_text, parse_salary_from_jsonb

logger = logging.getLogger(__name__)


@celery_app.task(name="app.workers.job_processor.process_telegram_message")
def process_telegram_message(message_data: Dict) -> Dict:
    """
    Process a single Telegram message using ML classifier.
    
    Args:
        message_data: Dict with keys: message_id, text, channel_id, timestamp
        
    Returns:
        Dict with processing results: is_job, confidence, extracted_data
    """
    try:
        log_memory_usage()
        
        message_text = message_data.get("text", "")
        message_id = message_data.get("message_id")
        channel_id = message_data.get("channel_id")
        
        logger.info(f"Processing message {message_id} from channel {channel_id}")
        
        # Get ML classifier (lazy-loaded)
        classifier = get_job_classifier()
        
        # Classify and extract in one efficient call
        classification, extraction = classifier.classify_and_extract(message_text)
        
        result = {
            "status": "success",
            "message_id": message_id,
            "channel_id": channel_id,
            "is_job": classification.is_job,
            "confidence": classification.confidence,
            "reason": classification.reason,
            "processing_time_ms": classification.processing_time_ms,
        }
        
        # If it's a job, include extracted data
        if classification.is_job and extraction:
            result["extracted_data"] = {
                "company": extraction.company,
                "job_title": extraction.job_title,
                "location": extraction.location,
                "skills": extraction.skills,
                "job_type": extraction.job_type,
                "experience": extraction.experience,
                "salary": extraction.salary,
                "apply_link": extraction.apply_link,
            }
            
            # Flag for admin review if confidence is low
            if classification.confidence < 0.80:
                result["needs_review"] = True
                logger.info(f"Message {message_id} flagged for admin review (confidence: {classification.confidence:.2f})")
        
        log_memory_usage()
        logger.info(f"Message {message_id} processed: is_job={classification.is_job}, confidence={classification.confidence:.2f}")
        
        return result
        
    except Exception as e:
        logger.error(f"Error processing message {message_id}: {e}", exc_info=True)
        return {
            "status": "error",
            "message_id": message_id,
            "error": str(e)
        }


@celery_app.task(name="app.workers.job_processor.batch_process_messages")
def batch_process_messages(messages: List[Dict]) -> Dict:
    """
    Process multiple Telegram messages in batch.
    
    Args:
        messages: List of message dicts
        
    Returns:
        Dict with batch processing summary
    """
    try:
        log_memory_usage()
        
        logger.info(f"Processing batch of {len(messages)} messages")
        
        results = []
        jobs_found = 0
        non_jobs = 0
        needs_review = 0
        
        classifier = get_job_classifier()
        
        for message_data in messages:
            try:
                message_text = message_data.get("text", "")
                message_id = message_data.get("message_id")
                
                # Classify and extract
                classification, extraction = classifier.classify_and_extract(message_text)
                
                result = {
                    "message_id": message_id,
                    "is_job": classification.is_job,
                    "confidence": classification.confidence,
                }
                
                if classification.is_job:
                    jobs_found += 1
                    result["extracted_data"] = {
                        "company": extraction.company,
                        "job_title": extraction.job_title,
                        "location": extraction.location,
                        "skills": extraction.skills,
                    }
                    
                    if classification.confidence < 0.80:
                        result["needs_review"] = True
                        needs_review += 1
                else:
                    non_jobs += 1
                
                results.append(result)
                
            except Exception as e:
                logger.error(f"Error processing message in batch: {e}")
                results.append({
                    "message_id": message_data.get("message_id"),
                    "error": str(e)
                })
        
        log_memory_usage()
        
        summary = {
            "status": "success",
            "total_messages": len(messages),
            "jobs_found": jobs_found,
            "non_jobs": non_jobs,
            "needs_review": needs_review,
            "results": results,
        }
        
        logger.info(f"Batch processing complete: {jobs_found} jobs, {non_jobs} non-jobs, {needs_review} need review")
        
        return summary
        
    except Exception as e:
        logger.error(f"Error in batch processing: {e}", exc_info=True)
        return {
            "status": "error",
            "error": str(e)
        }


@celery_app.task(name="app.workers.job_processor.process_pending_jobs")
def process_pending_jobs(limit: int = 100):
    """
    Process pending jobs from storage (DynamoDB/Local).
    
    Complete pipeline:
    1. Fetch pending messages from storage
    2. ML classification (3-stage early exit)
    3. Deduplication check (if job detected)
    4. Save to PostgreSQL (if new job)
    5. Update storage status
    
    Args:
        limit: Maximum messages to process in one run
    
    Returns:
        Dict with processing summary
    """
    try:
        log_memory_usage()
        logger.info(f"Starting job processing (limit: {limit})...")
        
        # Import here to avoid circular dependencies
        from app.services.storage_factory import get_storage_service
        from app.services.deduplication_service import deduplication_service
        from app.db.session import get_db
        from app.models.job import Job
        from app.models.company import Company
        import asyncio
        from uuid import uuid4
        
        # Get storage service
        storage = get_storage_service()
        
        # Get pending messages
        pending_messages = asyncio.run(storage.get_pending_messages(limit=limit))
        
        if not pending_messages:
            logger.info("No pending messages to process")
            return {
                "status": "success",
                "jobs_processed": 0,
                "message": "No pending messages"
            }
        
        logger.info(f"Found {len(pending_messages)} pending messages")
        
        # Get ML classifier
        classifier = get_job_classifier()
        
        # Get database session
        db = next(get_db())
        
        # Counters
        jobs_created = 0
        jobs_duplicates = 0
        non_jobs = 0
        errors = 0
        low_confidence_count = 0
        
        try:
            for message in pending_messages:
                message_id = message.get('id')
                message_text = message.get('text', '')
                channel_id = message.get('group_username', 'unknown')
                
                try:
                    logger.info(f"Processing message {message_id}")
                    
                    # STAGE 1-3: ML Classification
                    classification, extraction = classifier.classify_and_extract(message_text)
                    
                    # If not a job, mark as rejected
                    if not classification.is_job:
                        asyncio.run(storage.mark_rejected(
                            message_id=message_id,
                            reason=classification.reason
                        ))
                        non_jobs += 1
                        logger.info(f"Message {message_id} rejected: {classification.reason}")
                        continue
                    
                    # Flag low confidence jobs
                    if classification.confidence < 0.80:
                        low_confidence_count += 1
                        logger.warning(f"Low confidence job: {message_id} ({classification.confidence:.2f})")
                    
                    # DEDUPLICATION CHECK
                    # For now, use simple text-based check against recent jobs
                    cutoff = datetime.utcnow() - timedelta(days=7)
                    recent_jobs = db.query(Job).filter(
                        Job.created_at >= cutoff,
                        Job.is_active == True
                    ).limit(100).all()
                    
                    # Check for duplicates
                    is_duplicate = False
                    duplicate_id = None
                    
                    if recent_jobs:
                        recent_texts = [j.raw_text for j in recent_jobs if j.raw_text]
                        is_duplicate, similarity = asyncio.run(
                            deduplication_service.check_duplicate_simple(
                                message_text,
                                recent_texts,
                                threshold=0.85
                            )
                        )
                        
                        if is_duplicate:
                            # Find which job it matched
                            content_hash = deduplication_service.compute_content_hash(message_text)
                            for job in recent_jobs:
                                if job.raw_text:
                                    job_hash = deduplication_service.compute_content_hash(job.raw_text)
                                    if job_hash == content_hash or \
                                       deduplication_service.compute_similarity(message_text, job.raw_text) >= 0.85:
                                        duplicate_id = str(job.id)
                                        break
                    
                    if is_duplicate:
                        asyncio.run(storage.mark_rejected(
                            message_id=message_id,
                            reason=f"Duplicate of job {duplicate_id} (similarity: {similarity:.2f})"
                        ))
                        jobs_duplicates += 1
                        logger.info(f"Message {message_id} is duplicate (similarity: {similarity:.2f})")
                        continue
                    
                    # CREATE JOB RECORD
                    # First, get or create company
                    company_name = extraction.company or "Unknown Company"
                    company = db.query(Company).filter(Company.name == company_name).first()
                    
                    if not company:
                        company = Company(
                            id=uuid4(),
                            name=company_name,
                            description=f"Company from job posting",
                            is_verified=False
                        )
                        db.add(company)
                        db.flush()
                    
                    # Extract vacancy count from text (basic extraction)
                    vacancy_count = 1  # Default
                    import re
                    vacancy_patterns = [
                        r'(\d+)\s*(?:vacancies|openings|positions)',
                        r'hiring\s*(\d+)',
                        r'(\d+)\s*(?:spots|seats)'
                    ]
                    for pattern in vacancy_patterns:
                        match = re.search(pattern, message_text.lower())
                        if match:
                            vacancy_count = int(match.group(1))
                            break
                    
                    # Create job record
                    content_hash = deduplication_service.compute_content_hash(message_text)
                    
                    # Parse structured experience data
                    exp_data = parse_experience(extraction.experience)
                    
                    # Parse structured salary data - try both methods
                    salary_data = extract_salary_from_text(message_text)
                    if not salary_data.get('min') and extraction.salary:
                        # If text parsing didn't work, try the salary field
                        salary_data = extract_salary_from_text(extraction.salary)
                    
                    job = Job(
                        id=uuid4(),
                        title=extraction.job_title or "Untitled Position",
                        company_id=company.id,
                        description=message_text,
                        skills_required=extraction.skills or [],
                        experience_required=extraction.experience,
                        # Structured experience fields (NEW)
                        min_experience=exp_data.get('min'),
                        max_experience=exp_data.get('max'),
                        is_fresher=exp_data.get('is_fresher', False),
                        salary_range={'raw': extraction.salary} if extraction.salary else {},
                        # Structured salary fields (NEW)
                        min_salary=salary_data.get('min'),
                        max_salary=salary_data.get('max'),
                        salary_currency=salary_data.get('currency', 'INR'),
                        location=extraction.location,
                        job_type=extraction.job_type,
                        employment_type='fulltime',  # Default
                        source='telegram',
                        source_url=extraction.apply_link,
                        raw_text=message_text,
                        content_hash=content_hash,
                        source_message_id=uuid4(),  # Would be actual message UUID
                        ml_confidence=f"{classification.confidence:.2f}",
                        # Visibility tracking (default: show to all)
                        students_shown_to=[],
                        max_students_to_show=vacancy_count * 10,  # 10x vacancy as default limit
                        visibility_mode='all',  # Show to all by default
                        vacancy_count=vacancy_count,
                        is_active=True,
                        is_verified=classification.confidence >= 0.90,  # Auto-verify high confidence
                        expires_at=(datetime.utcnow() + timedelta(days=30)).isoformat()
                    )
                    
                    db.add(job)
                    db.commit()
                    
                    # Mark message as processed
                    asyncio.run(storage.mark_processed(
                        message_id=message_id,
                        status='processed',
                        job_id=str(job.id)
                    ))
                    
                    jobs_created += 1
                    logger.info(f"Created job {job.id} from message {message_id}")
                    
                except Exception as e:
                    logger.error(f"Error processing message {message_id}: {e}", exc_info=True)
                    asyncio.run(storage.mark_processed(
                        message_id=message_id,
                        status='error',
                        rejection_reason=str(e)
                    ))
                    errors += 1
                    db.rollback()
            
        finally:
            db.close()
        
        log_memory_usage()
        
        result = {
            "status": "success",
            "messages_processed": len(pending_messages),
            "jobs_created": jobs_created,
            "duplicates_skipped": jobs_duplicates,
            "non_jobs_rejected": non_jobs,
            "low_confidence_jobs": low_confidence_count,
            "errors": errors
        }
        
        logger.info(
            f"Processing complete: {jobs_created} jobs created, "
            f"{jobs_duplicates} duplicates, {non_jobs} non-jobs, {errors} errors"
        )
        
        return result
        
    except Exception as e:
        logger.error(f"Error in process_pending_jobs: {e}", exc_info=True)
        return {
            "status": "error",
            "error": str(e)
        }


@celery_app.task(name="app.workers.job_processor.deduplicate_jobs")
def deduplicate_jobs(days_back: int = 7):
    """
    Find and mark duplicate jobs using TF-IDF similarity.
    
    Args:
        days_back: How many days to look back for duplicates
    
    Returns:
        Dict with deduplication summary
    """
    try:
        logger.info(f"Starting deduplication (last {days_back} days)...")
        
        from app.services.deduplication_service import deduplication_service
        from app.db.session import get_db
        from app.models.job import Job
        
        db = next(get_db())
        duplicates_found = 0
        
        try:
            # Get recent active jobs
            cutoff = datetime.utcnow() - timedelta(days=days_back)
            jobs = db.query(Job).filter(
                Job.created_at >= cutoff,
                Job.is_active == True,
                Job.duplicate_of_id == None  # Not already marked as duplicate
            ).order_by(Job.created_at.desc()).all()
            
            logger.info(f"Checking {len(jobs)} jobs for duplicates...")
            
            # Compare each job with others
            for i, job1 in enumerate(jobs):
                if job1.duplicate_of_id:  # Skip if already marked
                    continue
                
                for job2 in jobs[i+1:]:
                    if job2.duplicate_of_id:  # Skip if already marked
                        continue
                    
                    if not job1.raw_text or not job2.raw_text:
                        continue
                    
                    # Compute similarity
                    similarity = deduplication_service.compute_similarity(
                        job1.raw_text,
                        job2.raw_text
                    )
                    
                    if similarity >= 0.85:
                        # Mark newer job as duplicate of older job
                        if job1.created_at < job2.created_at:
                            job2.duplicate_of_id = job1.id
                            job2.is_active = False
                            logger.info(f"Marked job {job2.id} as duplicate of {job1.id} (similarity: {similarity:.2f})")
                        else:
                            job1.duplicate_of_id = job2.id
                            job1.is_active = False
                            logger.info(f"Marked job {job1.id} as duplicate of {job2.id} (similarity: {similarity:.2f})")
                        
                        duplicates_found += 1
                        break  # Move to next job after finding duplicate
            
            db.commit()
            
            result = {
                "status": "success",
                "jobs_checked": len(jobs),
                "duplicates_found": duplicates_found,
                "days_back": days_back
            }
            
            logger.info(f"Deduplication complete: {duplicates_found} duplicates found")
            return result
            
        finally:
            db.close()
        
    except Exception as e:
        logger.error(f"Error deduplicating jobs: {e}", exc_info=True)
        return {
            "status": "error",
            "error": str(e)
        }


@celery_app.task(name="app.workers.job_processor.cleanup_old_jobs")
def cleanup_old_jobs(retention_days: int = 30):
    """
    Cleanup jobs older than retention period.
    
    Args:
        retention_days: Number of days to keep jobs (default: 30)
    
    Returns:
        Dict with cleanup summary
    """
    try:
        cutoff_date = datetime.utcnow() - timedelta(days=retention_days)
        
        logger.info(f"Cleaning up jobs older than {cutoff_date.date()}")
        
        from app.db.session import get_db
        from app.models.job import Job
        from sqlalchemy import and_
        
        db = next(get_db())
        jobs_deleted = 0
        
        try:
            # Option 1: Soft delete (set is_active=False)
            # This preserves data for analytics
            old_jobs = db.query(Job).filter(
                and_(
                    Job.created_at < cutoff_date,
                    Job.is_active == True
                )
            ).all()
            
            for job in old_jobs:
                job.is_active = False
                jobs_deleted += 1
            
            db.commit()
            
            # Also cleanup old messages from storage
            from app.services.storage_factory import get_storage_service
            import asyncio
            
            storage = get_storage_service()
            messages_deleted = asyncio.run(storage.cleanup_old_messages(days=retention_days))
            
            result = {
                "status": "success",
                "jobs_deactivated": jobs_deleted,
                "messages_deleted": messages_deleted,
                "cutoff_date": cutoff_date.isoformat(),
                "retention_days": retention_days
            }
            
            logger.info(f"Cleanup complete: {jobs_deleted} jobs deactivated, {messages_deleted} messages deleted")
            return result
            
        finally:
            db.close()
        
    except Exception as e:
        logger.error(f"Error cleaning up old jobs: {e}", exc_info=True)
        return {
            "status": "error",
            "error": str(e)
        }
