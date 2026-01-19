"""
Admin ML Management API
Endpoints for admin feedback, model retraining, and ML statistics
"""

from typing import Optional, Dict, Any
from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks, status
from sqlalchemy.orm import Session
from pydantic import BaseModel, Field
import json
from datetime import datetime
from pathlib import Path

from app.db.session import get_db
from app.models.job import Job
from app.api.deps import get_current_admin_user
from app.models.user import User

router = APIRouter()

# Training data path
TRAINING_DATA_PATH = Path(__file__).parent.parent.parent.parent / "ml" / "training" / "training_data.json"


# Pydantic models
class JobFeedback(BaseModel):
    """Admin feedback on job classification"""
    is_correct: bool = Field(..., description="Is the ML classification correct?")
    correct_classification: Optional[str] = Field(None, description="Correct classification if wrong (job/non_job)")
    reason: Optional[str] = Field(None, description="Reason for the feedback")
    notes: Optional[str] = Field(None, description="Additional notes from admin")


class RetrainRequest(BaseModel):
    """Request to retrain the model"""
    notify_on_complete: bool = Field(default=False, description="Send notification when complete")


class MLStatistics(BaseModel):
    """ML system statistics"""
    total_training_examples: int
    jobs_count: int
    non_jobs_count: int
    training_data_version: str
    model_version: str
    last_trained: str
    examples_by_source: Dict[str, int]
    recent_feedback_count: int
    pending_feedback_count: int


class TrainingExample(BaseModel):
    """Training data example"""
    text: str
    is_job: bool
    metadata: Dict[str, Any]


# Endpoints
@router.post("/jobs/{job_id}/feedback", status_code=status.HTTP_200_OK)
async def submit_job_feedback(
    job_id: int,
    feedback: JobFeedback,
    db: Session = Depends(get_db),
    current_admin: User = Depends(get_current_admin_user)
):
    """
    Submit admin feedback on a job classification
    
    This feedback will be added to training data for model improvement
    """
    # Get the job
    job = db.query(Job).filter(Job.id == job_id).first()
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    
    # Load training data
    try:
        with open(TRAINING_DATA_PATH, 'r', encoding='utf-8') as f:
            training_data = json.load(f)
    except FileNotFoundError:
        raise HTTPException(status_code=500, detail="Training data file not found")
    
    # Determine correct classification
    if feedback.is_correct:
        # Classification was correct, just log it
        correct_is_job = job.ml_confidence is not None  # If has ML confidence, was classified as job
        feedback_type = "confirmation"
    else:
        # Classification was wrong
        if feedback.correct_classification == "non_job":
            correct_is_job = False
        elif feedback.correct_classification == "job":
            correct_is_job = True
        else:
            raise HTTPException(
                status_code=400, 
                detail="correct_classification must be 'job' or 'non_job' when is_correct=false"
            )
        feedback_type = "correction"
    
    # Create training example from feedback
    example = {
        "text": job.description,
        "is_job": correct_is_job,
        "metadata": {
            "source": "admin_feedback",
            "job_id": job_id,
            "original_ml_confidence": job.ml_confidence,
            "feedback_type": feedback_type,
            "feedback_reason": feedback.reason,
            "feedback_notes": feedback.notes,
            "admin_id": current_admin.id,
            "feedback_date": datetime.utcnow().isoformat(),
            "channel": job.source_channel,
            "message_id": job.source_message_id
        }
    }
    
    # Add to training data
    training_data["examples"].append(example)
    training_data["total_examples"] = len(training_data["examples"])
    
    # Update statistics
    if "statistics" not in training_data:
        training_data["statistics"] = {}
    
    # Recalculate stats
    stats = training_data["statistics"]
    stats["total"] = len(training_data["examples"])
    stats["jobs"] = sum(1 for ex in training_data["examples"] if ex["is_job"])
    stats["non_jobs"] = stats["total"] - stats["jobs"]
    stats["ratio"] = f"{stats['jobs']}:{stats['non_jobs']}"
    
    # Update sources count
    if "sources" not in stats:
        stats["sources"] = {}
    sources = {}
    for ex in training_data["examples"]:
        source = ex.get("metadata", {}).get("source", "unknown")
        sources[source] = sources.get(source, 0) + 1
    stats["sources"] = sources
    
    # Update last modified
    training_data["last_updated"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    training_data["last_feedback"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    
    # Save training data
    with open(TRAINING_DATA_PATH, 'w', encoding='utf-8') as f:
        json.dump(training_data, f, indent=2, ensure_ascii=False)
    
    # Update job record
    job.admin_feedback = {
        "is_correct": feedback.is_correct,
        "correct_classification": feedback.correct_classification,
        "reason": feedback.reason,
        "notes": feedback.notes,
        "admin_id": current_admin.id,
        "feedback_date": datetime.utcnow().isoformat()
    }
    
    # If classification was wrong, update status
    if not feedback.is_correct:
        if feedback.correct_classification == "non_job":
            job.status = "rejected"
            job.is_active = False
        else:
            job.status = "active"
            job.is_active = True
    
    db.commit()
    
    return {
        "success": True,
        "message": "Feedback submitted successfully",
        "feedback_type": feedback_type,
        "training_example_added": True,
        "total_training_examples": training_data["total_examples"],
        "job_status_updated": not feedback.is_correct
    }


@router.get("/training-stats", response_model=MLStatistics)
async def get_training_statistics(
    current_admin: User = Depends(get_current_admin_user)
):
    """
    Get ML training data statistics
    """
    try:
        with open(TRAINING_DATA_PATH, 'r', encoding='utf-8') as f:
            training_data = json.load(f)
    except FileNotFoundError:
        raise HTTPException(status_code=500, detail="Training data file not found")
    
    stats = training_data.get("statistics", {})
    
    # Count recent feedback (last 30 days)
    recent_feedback_count = 0
    for ex in training_data.get("examples", []):
        if ex.get("metadata", {}).get("source") == "admin_feedback":
            recent_feedback_count += 1
    
    # Load model metadata
    model_metadata_path = TRAINING_DATA_PATH.parent / "models" / "model_metadata.json"
    model_version = "unknown"
    last_trained = "never"
    
    try:
        with open(model_metadata_path, 'r') as f:
            model_meta = json.load(f)
            model_version = model_meta.get("version", "unknown")
            last_trained = model_meta.get("last_trained", "never")
    except FileNotFoundError:
        pass
    
    return MLStatistics(
        total_training_examples=stats.get("total", 0),
        jobs_count=stats.get("jobs", 0),
        non_jobs_count=stats.get("non_jobs", 0),
        training_data_version=training_data.get("version", "unknown"),
        model_version=model_version,
        last_trained=last_trained,
        examples_by_source=stats.get("sources", {}),
        recent_feedback_count=recent_feedback_count,
        pending_feedback_count=0  # TODO: Count jobs needing review
    )


@router.post("/retrain", status_code=status.HTTP_202_ACCEPTED)
async def trigger_model_retraining(
    request: RetrainRequest,
    background_tasks: BackgroundTasks,
    current_admin: User = Depends(get_current_admin_user)
):
    """
    Trigger ML model retraining
    
    This runs in the background and uses latest training data
    """
    # Add background task to retrain
    background_tasks.add_task(
        retrain_model_task,
        admin_id=current_admin.id,
        notify=request.notify_on_complete
    )
    
    return {
        "success": True,
        "message": "Model retraining started in background",
        "status": "processing"
    }


@router.get("/recent-feedback")
async def get_recent_feedback(
    limit: int = 20,
    current_admin: User = Depends(get_current_admin_user)
):
    """
    Get recent admin feedback submissions
    """
    try:
        with open(TRAINING_DATA_PATH, 'r', encoding='utf-8') as f:
            training_data = json.load(f)
    except FileNotFoundError:
        raise HTTPException(status_code=500, detail="Training data file not found")
    
    # Filter feedback examples
    feedback_examples = [
        ex for ex in training_data.get("examples", [])
        if ex.get("metadata", {}).get("source") == "admin_feedback"
    ]
    
    # Sort by date (most recent first)
    feedback_examples.sort(
        key=lambda x: x.get("metadata", {}).get("feedback_date", ""),
        reverse=True
    )
    
    # Limit results
    feedback_examples = feedback_examples[:limit]
    
    return {
        "total": len(feedback_examples),
        "feedback": feedback_examples
    }


@router.get("/training-data/sample")
async def get_training_data_sample(
    source: Optional[str] = None,
    is_job: Optional[bool] = None,
    limit: int = 10,
    current_admin: User = Depends(get_current_admin_user)
):
    """
    Get sample training data examples
    
    Useful for reviewing training data quality
    """
    try:
        with open(TRAINING_DATA_PATH, 'r', encoding='utf-8') as f:
            training_data = json.load(f)
    except FileNotFoundError:
        raise HTTPException(status_code=500, detail="Training data file not found")
    
    examples = training_data.get("examples", [])
    
    # Filter by source
    if source:
        examples = [ex for ex in examples if ex.get("metadata", {}).get("source") == source]
    
    # Filter by is_job
    if is_job is not None:
        examples = [ex for ex in examples if ex.get("is_job") == is_job]
    
    # Limit results
    examples = examples[:limit]
    
    return {
        "total": len(examples),
        "filters": {
            "source": source,
            "is_job": is_job,
            "limit": limit
        },
        "examples": examples
    }


# Background task
async def retrain_model_task(admin_id: int, notify: bool = False):
    """
    Background task to retrain the model
    """
    import subprocess
    import sys
    from pathlib import Path
    
    # Get project root
    project_root = Path(__file__).parent.parent.parent.parent.parent
    retrain_script = project_root / "scripts" / "retrain_model.py"
    
    try:
        # Run retraining script
        result = subprocess.run(
            [sys.executable, str(retrain_script)],
            cwd=str(project_root),
            capture_output=True,
            text=True,
            timeout=300  # 5 minutes timeout
        )
        
        if result.returncode == 0:
            print(f"✅ Model retrained successfully by admin {admin_id}")
            # TODO: Send notification if requested
        else:
            print(f"❌ Model retraining failed: {result.stderr}")
            
    except subprocess.TimeoutExpired:
        print(f"❌ Model retraining timed out after 5 minutes")
    except Exception as e:
        print(f"❌ Model retraining error: {e}")
