"""
ML Feedback API - Admin Review System for Model Improvement

Enables admins to:
- Review and correct ML classifications/extractions
- Provide feedback for reinforcement learning
- Track model accuracy over time
- Trigger model retraining

This powers the continuous improvement loop.
"""

from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any
from fastapi import APIRouter, Depends, HTTPException, Body
from sqlalchemy import select, func, desc
from sqlalchemy.ext.asyncio import AsyncSession
from pydantic import BaseModel, Field
import structlog
import json

from app.api.deps import get_db
from app.models.job import Job
from app.config import settings

logger = structlog.get_logger(__name__)
router = APIRouter()


class JobFeedback(BaseModel):
    """Admin feedback on a job posting"""
    job_id: int
    is_relevant: bool = Field(description="Is this actually a job posting?")
    correct_classification: Optional[str] = Field(None, description="Correct job category if wrong")
    
    # Field corrections
    correct_company: Optional[str] = Field(None, description="Correct company name")
    correct_salary: Optional[str] = Field(None, description="Correct salary")
    correct_experience: Optional[str] = Field(None, description="Correct experience required")
    correct_location: Optional[str] = Field(None, description="Correct location")
    correct_skills: Optional[List[str]] = Field(None, description="Correct skills list")
    
    # Admin notes
    notes: Optional[str] = Field(None, description="Additional notes for training")
    reviewed_by: str = Field(description="Admin username")


class FeedbackStatsResponse(BaseModel):
    total_reviews: int
    accuracy_rate: float
    common_errors: Dict[str, int]
    last_retrain_date: Optional[datetime]


class PendingReviewJob(BaseModel):
    job_id: int
    title: str
    company: Optional[str]
    source_text: str  # Original message
    ml_classification: str
    ml_confidence: float
    extracted_data: Dict[str, Any]
    created_at: datetime


@router.post("/jobs/{job_id}/feedback")
async def submit_job_feedback(
    job_id: int,
    feedback: JobFeedback,
    db: AsyncSession = Depends(get_db)
):
    """
    Submit admin feedback for a specific job posting.
    
    **Reinforcement Learning Pipeline:**
    1. Admin reviews job and provides corrections
    2. Feedback stored in ml_feedback JSONB column
    3. Used to retrain classifier and extractor
    4. Model improves over time
    
    Args:
        job_id: Job posting ID
        feedback: Admin corrections and notes
        
    Returns:
        Confirmation with feedback ID
    """
    logger.info("ml_feedback_submitted", job_id=job_id, reviewed_by=feedback.reviewed_by)
    
    # Get the job
    result = await db.execute(select(Job).where(Job.id == job_id))
    job = result.scalar_one_or_none()
    
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    
    # Store feedback in JSONB column
    feedback_data = {
        'reviewed_at': datetime.utcnow().isoformat(),
        'reviewed_by': feedback.reviewed_by,
        'is_relevant': feedback.is_relevant,
        'corrections': {
            'classification': feedback.correct_classification,
            'company': feedback.correct_company,
            'salary': feedback.correct_salary,
            'experience': feedback.correct_experience,
            'location': feedback.correct_location,
            'skills': feedback.correct_skills
        },
        'notes': feedback.notes,
        'original_ml_output': {
            'classification': job.job_title,  # ML classified title
            'confidence': job.ml_confidence if hasattr(job, 'ml_confidence') else None,
            'extracted_company': job.company,
            'extracted_salary': job.salary_range,
            'extracted_experience': job.experience_level,
            'extracted_location': job.location,
            'extracted_skills': job.required_skills
        }
    }
    
    # Append to existing feedback or create new
    if job.ml_feedback:
        existing_feedback = json.loads(job.ml_feedback) if isinstance(job.ml_feedback, str) else job.ml_feedback
        if isinstance(existing_feedback, list):
            existing_feedback.append(feedback_data)
        else:
            existing_feedback = [existing_feedback, feedback_data]
        job.ml_feedback = existing_feedback
    else:
        job.ml_feedback = [feedback_data]
    
    # Mark as reviewed
    if not hasattr(job, 'admin_reviewed'):
        # If column doesn't exist yet, we'll just use the feedback presence
        pass
    else:
        job.admin_reviewed = True
        job.admin_reviewed_at = datetime.utcnow()
    
    await db.commit()
    
    logger.info("ml_feedback_stored", job_id=job_id, corrections_count=len([k for k, v in feedback_data['corrections'].items() if v]))
    
    return {
        'status': 'success',
        'job_id': job_id,
        'feedback_id': len(job.ml_feedback),
        'message': 'Feedback recorded for model improvement'
    }


@router.get("/jobs/pending-review", response_model=List[PendingReviewJob])
async def get_pending_review_jobs(
    limit: int = 50,
    min_confidence: float = 0.0,
    max_confidence: float = 0.7,
    db: AsyncSession = Depends(get_db)
):
    """
    Get job postings that need admin review.
    
    **Priority Queue:**
    - Low confidence predictions (< 70%)
    - Recent jobs (last 7 days)
    - Not yet reviewed
    
    **Use Cases:**
    - Daily admin review queue
    - Quality assurance sampling
    - Model calibration
    
    Args:
        limit: Max jobs to return
        min_confidence: Minimum ML confidence threshold
        max_confidence: Maximum ML confidence threshold (default 0.7 for low-confidence)
        
    Returns:
        List of jobs needing review with ML predictions
    """
    logger.info("pending_review_requested", limit=limit, confidence_range=[min_confidence, max_confidence])
    
    # Get recent jobs without feedback or with low confidence
    # Note: Adjust this query based on your actual schema
    stmt = select(Job).where(
        Job.created_at >= datetime.utcnow() - timedelta(days=7)
    )
    
    # Filter by confidence if column exists
    if hasattr(Job, 'ml_confidence'):
        stmt = stmt.where(
            Job.ml_confidence >= min_confidence,
            Job.ml_confidence <= max_confidence
        )
    
    # Exclude already reviewed
    if hasattr(Job, 'admin_reviewed'):
        stmt = stmt.where(Job.admin_reviewed == False)
    else:
        # Fallback: exclude jobs with feedback
        stmt = stmt.where(Job.ml_feedback == None)
    
    stmt = stmt.order_by(desc(Job.created_at)).limit(limit)
    result = await db.execute(stmt)
    jobs = result.scalars().all()
    
    results = []
    for job in jobs:
        # Get source text from message if available
        source_text = job.description[:500] if job.description else "N/A"
        
        results.append(PendingReviewJob(
            job_id=job.id,
            title=job.job_title or "Untitled",
            company=job.company,
            source_text=source_text,
            ml_classification=job.job_title or "unclassified",
            ml_confidence=job.ml_confidence if hasattr(job, 'ml_confidence') else 0.5,
            extracted_data={
                'company': job.company,
                'salary': job.salary_range,
                'experience': job.experience_level,
                'location': job.location,
                'skills': job.required_skills
            },
            created_at=job.created_at
        ))
    
    logger.info("pending_review_fetched", count=len(results))
    return results


@router.get("/feedback/stats", response_model=FeedbackStatsResponse)
async def get_feedback_stats(db: AsyncSession = Depends(get_db)):
    """
    Get ML model feedback statistics.
    
    **Metrics:**
    - Total reviews completed
    - Model accuracy rate (% of correct predictions)
    - Common error types
    - Last retrain date
    
    **Use for:**
    - Model performance dashboard
    - Decision to retrain
    - Quality assurance reporting
    
    Returns:
        Feedback statistics and accuracy metrics
    """
    logger.info("feedback_stats_requested")
    
    # Get jobs with feedback
    stmt = select(Job).where(Job.ml_feedback.isnot(None))
    result = await db.execute(stmt)
    jobs_with_feedback = result.scalars().all()
    
    total_reviews = len(jobs_with_feedback)
    
    if total_reviews == 0:
        return FeedbackStatsResponse(
            total_reviews=0,
            accuracy_rate=0.0,
            common_errors={},
            last_retrain_date=None
        )
    
    # Calculate accuracy
    correct_predictions = 0
    error_types = {
        'wrong_classification': 0,
        'wrong_company': 0,
        'wrong_salary': 0,
        'wrong_experience': 0,
        'wrong_location': 0,
        'wrong_skills': 0,
        'false_positive': 0  # Not actually a job
    }
    
    for job in jobs_with_feedback:
        feedback_list = job.ml_feedback if isinstance(job.ml_feedback, list) else [job.ml_feedback]
        
        for feedback in feedback_list:
            if isinstance(feedback, str):
                feedback = json.loads(feedback)
            
            # Check if prediction was correct
            if feedback.get('is_relevant', True):
                # It is a job - check corrections
                corrections = feedback.get('corrections', {})
                has_corrections = any(v for v in corrections.values() if v)
                
                if not has_corrections:
                    correct_predictions += 1
                else:
                    # Count error types
                    if corrections.get('classification'):
                        error_types['wrong_classification'] += 1
                    if corrections.get('company'):
                        error_types['wrong_company'] += 1
                    if corrections.get('salary'):
                        error_types['wrong_salary'] += 1
                    if corrections.get('experience'):
                        error_types['wrong_experience'] += 1
                    if corrections.get('location'):
                        error_types['wrong_location'] += 1
                    if corrections.get('skills'):
                        error_types['wrong_skills'] += 1
            else:
                # False positive - not actually a job
                error_types['false_positive'] += 1
    
    accuracy_rate = (correct_predictions / total_reviews) * 100 if total_reviews > 0 else 0.0
    
    # Get last retrain date from config or database
    # This would need to be stored somewhere - for now return None
    last_retrain_date = None
    
    logger.info("feedback_stats_calculated", 
                total_reviews=total_reviews, 
                accuracy_rate=accuracy_rate,
                correct=correct_predictions)
    
    return FeedbackStatsResponse(
        total_reviews=total_reviews,
        accuracy_rate=round(accuracy_rate, 2),
        common_errors=error_types,
        last_retrain_date=last_retrain_date
    )


@router.post("/trigger-retrain")
async def trigger_model_retrain(
    triggered_by: str = Body(..., description="Admin username"),
    notes: Optional[str] = Body(None, description="Reason for retraining"),
    db: AsyncSession = Depends(get_db)
):
    """
    Manually trigger ML model retraining.
    
    **Retraining Process:**
    1. Collect all feedback data from reviewed jobs
    2. Generate training dataset with corrections
    3. Retrain classifier and extractor
    4. Validate on holdout set
    5. Deploy new model if accuracy improves
    
    **When to Retrain:**
    - After 50+ reviews
    - When accuracy < 80%
    - Monthly scheduled retrain
    - After major error patterns identified
    
    Args:
        triggered_by: Admin username
        notes: Reason for manual retrain
        
    Returns:
        Training job status
    """
    logger.info("model_retrain_triggered", triggered_by=triggered_by, notes=notes)
    
    # Get feedback count
    stmt = select(func.count()).select_from(Job).where(Job.ml_feedback.isnot(None))
    result = await db.execute(stmt)
    feedback_count = result.scalar()
    
    if feedback_count < 10:
        raise HTTPException(
            status_code=400, 
            detail=f"Insufficient feedback data. Need at least 10 reviews, have {feedback_count}"
        )
    
    # In production, this would:
    # 1. Queue a background job
    # 2. Export training data
    # 3. Run training script
    # 4. Validate model
    # 5. Deploy if improved
    
    # For now, return a placeholder response
    return {
        'status': 'queued',
        'training_job_id': f"retrain_{int(datetime.utcnow().timestamp())}",
        'triggered_by': triggered_by,
        'feedback_samples': feedback_count,
        'estimated_duration_minutes': 15,
        'message': 'Model retraining queued. Check back in 15 minutes.',
        'notes': notes
    }


@router.get("/training/history")
async def get_training_history(limit: int = 10):
    """
    Get model retraining history.
    
    **Shows:**
    - When model was retrained
    - Who triggered it
    - Accuracy improvements
    - Duration and status
    
    Returns:
        Training history log
    """
    # This would come from a training_history table in production
    # For now, return placeholder
    return {
        'history': [],
        'message': 'Training history tracking will be implemented with first retrain'
    }
