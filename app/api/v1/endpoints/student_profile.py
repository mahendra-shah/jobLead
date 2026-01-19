"""
Student Self-Service API
Students manage their own profile, preferences, and resume
"""

from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from app.api.deps import get_current_user, get_db
from app.models.user import User
from app.models.student import Student
from app.schemas.student import (
    StudentResponse,
    StudentProfileUpdate,
    StudentPasswordChange,
    StudentPreferencesUpdate,
    StudentPreferencesResponse,
    ProfileCompletenessResponse
)
from app.core.security import verify_password, get_password_hash
import boto3
from app.config import settings

router = APIRouter()


# ==================== Student Profile ====================

@router.get("/students/me", response_model=StudentResponse)
async def get_my_profile(
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """
    Get current student's profile
    
    **Auth**: Student (JWT required)
    """
    # Get student record
    result = await db.execute(
        select(Student).where(Student.email == current_user.email)
    )
    student = result.scalar_one_or_none()
    
    if not student:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Student profile not found"
        )
    
    return student


@router.put("/students/me", response_model=StudentResponse)
async def update_my_profile(
    profile_update: StudentProfileUpdate,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """
    Update current student's profile
    
    **Auth**: Student (JWT required)
    """
    result = await db.execute(
        select(Student).where(Student.email == current_user.email)
    )
    student = result.scalar_one_or_none()
    
    if not student:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Student profile not found"
        )
    
    # Update fields
    update_data = profile_update.dict(exclude_unset=True)
    for field, value in update_data.items():
        setattr(student, field, value)
    
    await db.commit()
    await db.refresh(student)
    
    return student


@router.post("/students/me/change-password")
async def change_password(
    password_change: StudentPasswordChange,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """
    Change student password
    
    **Auth**: Student (JWT required)
    """
    result = await db.execute(
        select(Student).where(Student.email == current_user.email)
    )
    student = result.scalar_one_or_none()
    
    if not student:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Student profile not found"
        )
    
    # Verify current password
    if not verify_password(password_change.current_password, student.password):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Current password is incorrect"
        )
    
    # Update password
    student.password = get_password_hash(password_change.new_password)
    await db.commit()
    
    return {"message": "Password changed successfully"}


# ==================== Student Preferences ====================

@router.get("/students/me/preferences", response_model=StudentPreferencesResponse)
async def get_my_preferences(
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """
    Get student's job preferences
    
    **Auth**: Student (JWT required)
    """
    result = await db.execute(
        select(Student).where(Student.email == current_user.email)
    )
    student = result.scalar_one_or_none()
    
    if not student:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Student profile not found"
        )
    
    return StudentPreferencesResponse(
        skills=student.skills or [],
        preferred_locations=student.preferred_locations or [],
        preferred_job_types=student.preferred_job_types or [],
        excluded_companies=student.excluded_companies or [],
        min_salary=student.min_salary,
        max_salary=student.max_salary
    )


@router.put("/students/me/preferences", response_model=StudentPreferencesResponse)
async def update_my_preferences(
    preferences: StudentPreferencesUpdate,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """
    Update student's job preferences
    
    **Auth**: Student (JWT required)
    
    These preferences are used for job recommendations.
    """
    result = await db.execute(
        select(Student).where(Student.email == current_user.email)
    )
    student = result.scalar_one_or_none()
    
    if not student:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Student profile not found"
        )
    
    # Update preferences
    update_data = preferences.dict(exclude_unset=True)
    for field, value in update_data.items():
        setattr(student, field, value)
    
    await db.commit()
    await db.refresh(student)
    
    return StudentPreferencesResponse(
        skills=student.skills or [],
        preferred_locations=student.preferred_locations or [],
        preferred_job_types=student.preferred_job_types or [],
        excluded_companies=student.excluded_companies or [],
        min_salary=student.min_salary,
        max_salary=student.max_salary
    )


# ==================== Resume Upload ====================

@router.post("/students/me/resume")
async def upload_resume(
    file: UploadFile = File(...),
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """
    Upload student resume to S3
    
    **Auth**: Student (JWT required)
    
    Accepts PDF files only (max 5MB)
    """
    # Validate file type
    if not file.filename.endswith('.pdf'):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Only PDF files are allowed"
        )
    
    # Read file content
    content = await file.read()
    
    # Validate file size (5MB)
    if len(content) > 5 * 1024 * 1024:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="File size must be less than 5MB"
        )
    
    # Get student
    result = await db.execute(
        select(Student).where(Student.email == current_user.email)
    )
    student = result.scalar_one_or_none()
    
    if not student:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Student profile not found"
        )
    
    # Upload to S3
    try:
        s3_client = boto3.client(
            's3',
            aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY,
            region_name=settings.AWS_REGION
        )
        
        # Generate S3 key
        s3_key = f"resumes/{student.id}/{file.filename}"
        
        # Upload
        s3_client.put_object(
            Bucket=settings.S3_BUCKET_NAME,
            Key=s3_key,
            Body=content,
            ContentType='application/pdf'
        )
        
        # Generate URL
        resume_url = f"https://{settings.S3_BUCKET_NAME}.s3.{settings.AWS_REGION}.amazonaws.com/{s3_key}"
        
        # Update student record
        student.resume_url = resume_url
        await db.commit()
        
        return {
            "message": "Resume uploaded successfully",
            "resume_url": resume_url
        }
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to upload resume: {str(e)}"
        )


@router.delete("/students/me/resume")
async def delete_resume(
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """
    Delete student resume
    
    **Auth**: Student (JWT required)
    """
    result = await db.execute(
        select(Student).where(Student.email == current_user.email)
    )
    student = result.scalar_one_or_none()
    
    if not student:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Student profile not found"
        )
    
    if not student.resume_url:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No resume found"
        )
    
    # Delete from S3
    try:
        s3_client = boto3.client(
            's3',
            aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY,
            region_name=settings.AWS_REGION
        )
        
        # Extract S3 key from URL
        s3_key = student.resume_url.split('.amazonaws.com/')[-1]
        
        # Delete
        s3_client.delete_object(
            Bucket=settings.S3_BUCKET_NAME,
            Key=s3_key
        )
        
        # Update student record
        student.resume_url = None
        await db.commit()
        
        return {"message": "Resume deleted successfully"}
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to delete resume: {str(e)}"
        )


# ==================== Profile Completeness ====================

@router.get("/students/me/profile-completeness", response_model=ProfileCompletenessResponse)
async def get_profile_completeness(
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """
    Check profile completeness
    
    **Auth**: Student (JWT required)
    
    Returns percentage and missing fields
    """
    result = await db.execute(
        select(Student).where(Student.email == current_user.email)
    )
    student = result.scalar_one_or_none()
    
    if not student:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Student profile not found"
        )
    
    # Calculate completeness
    total_fields = 15
    completed_fields = 0
    missing_fields = []
    suggestions = []
    
    # Required fields
    if student.first_name:
        completed_fields += 1
    else:
        missing_fields.append("first_name")
        suggestions.append("Add your first name")
    
    if student.last_name:
        completed_fields += 1
    else:
        missing_fields.append("last_name")
        suggestions.append("Add your last name")
    
    if student.email:
        completed_fields += 1
    
    if student.phone:
        completed_fields += 1
    else:
        missing_fields.append("phone")
        suggestions.append("Add your phone number")
    
    if student.college_id:
        completed_fields += 1
    else:
        missing_fields.append("college")
        suggestions.append("Select your college")
    
    if student.degree:
        completed_fields += 1
    else:
        missing_fields.append("degree")
        suggestions.append("Add your degree")
    
    if student.branch:
        completed_fields += 1
    else:
        missing_fields.append("branch")
        suggestions.append("Add your branch")
    
    if student.passing_year:
        completed_fields += 1
    else:
        missing_fields.append("passing_year")
        suggestions.append("Add your passing year")
    
    if student.cgpa:
        completed_fields += 1
    else:
        missing_fields.append("cgpa")
        suggestions.append("Add your CGPA")
    
    if student.resume_url:
        completed_fields += 1
    else:
        missing_fields.append("resume")
        suggestions.append("Upload your resume")
    
    if student.skills and len(student.skills) > 0:
        completed_fields += 1
    else:
        missing_fields.append("skills")
        suggestions.append("Add at least 3 skills")
    
    if student.preferred_locations and len(student.preferred_locations) > 0:
        completed_fields += 1
    else:
        missing_fields.append("preferred_locations")
        suggestions.append("Add your preferred job locations")
    
    if student.preferred_job_types and len(student.preferred_job_types) > 0:
        completed_fields += 1
    else:
        missing_fields.append("preferred_job_types")
        suggestions.append("Select your preferred job types")
    
    # Optional fields
    if student.min_salary:
        completed_fields += 1
    
    if student.max_salary:
        completed_fields += 1
    
    percentage = int((completed_fields / total_fields) * 100)
    is_complete = percentage >= 80  # 80% threshold
    
    return ProfileCompletenessResponse(
        percentage=percentage,
        missing_fields=missing_fields,
        suggestions=suggestions,
        is_complete=is_complete
    )
