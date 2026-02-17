"""
Student Profile CRUD API
GET/PUT for profile, POST/DELETE for resume
All fields match frontend requirements exactly
"""

from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, status
from fastapi.responses import FileResponse
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from typing import Optional
import traceback
import json

from app.api.deps import get_current_user, get_db
from app.models.user import User
from app.models.student import Student
from app.schemas.student_profile import (
    StudentProfileResponse,
    StudentProfileUpdate,
    InternshipDetail,
    ProjectDetail,
    LanguageProficiency
)
from app.schemas.student import ProfileCompletenessResponse
import boto3
from app.config import settings
import os
from pathlib import Path
import uuid

router = APIRouter()

# Ensure resume storage directory exists
RESUME_STORAGE_DIR = Path(settings.RESUME_STORAGE_DIR)
RESUME_STORAGE_DIR.mkdir(parents=True, exist_ok=True)


async def get_student_by_user_id(db: AsyncSession, user_id) -> Optional[Student]:
    """Helper to get student by user_id"""
    result = await db.execute(
        select(Student).where(Student.user_id == user_id)
    )
    return result.scalar_one_or_none()


def save_resume(file_content: bytes, filename: str, student_id: str) -> str:
    """Upload resume to storage (local or S3) and return URL"""
    storage_type = settings.RESUME_STORAGE_TYPE.lower()
    
    if storage_type == "s3":
        return upload_resume_to_s3(file_content, filename, student_id)
    else:
        return upload_resume_local(file_content, filename, student_id)


def upload_resume_local(file_content: bytes, filename: str, student_id: str) -> str:
    """Upload resume to local storage and return URL path"""
    try:
        # Create student-specific directory
        student_dir = RESUME_STORAGE_DIR / str(student_id)
        student_dir.mkdir(parents=True, exist_ok=True)
        
        # Generate unique filename
        unique_filename = f"{uuid.uuid4()}_{filename}"
        file_path = student_dir / unique_filename
        
        # Write file
        with open(file_path, 'wb') as f:
            f.write(file_content)
        
        # Return relative URL path that can be served by FastAPI
        resume_url = f"/api/v1/resumes/{student_id}/{unique_filename}"
        return resume_url
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to upload resume to local storage: {str(e)}"
        )


def upload_resume_to_s3(file_content: bytes, filename: str, student_id: str) -> str:
    """Upload resume to S3 and return URL"""
    try:
        s3_client = boto3.client(
            's3',
            aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY,
            region_name=settings.AWS_REGION
        )
        
        # Generate S3 key with unique filename
        unique_filename = f"{uuid.uuid4()}_{filename}"
        s3_key = f"resumes/{student_id}/{unique_filename}"
        
        # Upload
        s3_client.put_object(
            Bucket=settings.S3_BUCKET_NAME,
            Key=s3_key,
            Body=file_content,
            ContentType='application/pdf'
        )
        
        # Generate URL
        resume_url = f"https://{settings.S3_BUCKET_NAME}.s3.{settings.AWS_REGION}.amazonaws.com/{s3_key}"
        return resume_url
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to upload resume to S3: {str(e)}"
        )


def delete_resume(resume_url: str):
    """Delete resume from storage (local or S3)"""
    storage_type = settings.RESUME_STORAGE_TYPE.lower()
    
    if storage_type == "s3":
        delete_resume_from_s3(resume_url)
    else:
        delete_resume_local(resume_url)


def delete_resume_local(resume_url: str):
    """Delete resume from local storage"""
    try:
        if not resume_url:
            return
        
        # Extract file path from URL
        # URL format: /api/v1/resumes/{student_id}/{filename}
        if resume_url.startswith('/api/v1/resumes/'):
            # Remove the API prefix to get the relative path
            relative_path = resume_url.replace('/api/v1/resumes/', '')
            file_path = RESUME_STORAGE_DIR / relative_path
            
            # Delete file if it exists
            if file_path.exists():
                file_path.unlink()
                # Also try to remove parent directory if empty
                try:
                    if file_path.parent.exists() and not any(file_path.parent.iterdir()):
                        file_path.parent.rmdir()
                except OSError:
                    pass  # Directory not empty or other error, ignore
    except Exception as e:
        # Log but don't fail - resume might already be deleted
        print(f"Warning: Failed to delete resume from local storage: {str(e)}")


def delete_resume_from_s3(resume_url: str):
    """Delete resume from S3"""
    try:
        if not resume_url or '.amazonaws.com/' not in resume_url:
            return
        
        s3_client = boto3.client(
            's3',
            aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY,
            region_name=settings.AWS_REGION
        )
        
        # Extract S3 key from URL
        s3_key = resume_url.split('.amazonaws.com/')[-1]
        s3_client.delete_object(
            Bucket=settings.S3_BUCKET_NAME,
            Key=s3_key
        )
    except Exception as e:
        # Log but don't fail - resume might already be deleted
        print(f"Warning: Failed to delete resume from S3: {str(e)}")


def build_profile_response(student: Student, user: User) -> StudentProfileResponse:
    """Build comprehensive profile response with proper serialization"""
    # Convert student.id to string if it's UUID
    student_id = str(student.id) if hasattr(student, 'id') and student.id else None
    
    # Serialize nested objects properly
    internship_details = []
    if student.internship_details:
        for intern in student.internship_details:
            if isinstance(intern, dict):
                internship_details.append(intern)
            else:
                # If it's a Pydantic model, convert to dict
                internship_details.append(intern.dict() if hasattr(intern, 'dict') else intern)
    
    projects = []
    if student.projects:
        for proj in student.projects:
            if isinstance(proj, dict):
                projects.append(proj)
            else:
                projects.append(proj.dict() if hasattr(proj, 'dict') else proj)
    
    languages = []
    if student.languages:
        for lang in student.languages:
            if isinstance(lang, dict):
                languages.append(lang)
            else:
                languages.append(lang.dict() if hasattr(lang, 'dict') else lang)
    
    # Convert date_of_birth to string if it's a date object
    date_of_birth_str = None
    if student.date_of_birth:
        if isinstance(student.date_of_birth, str):
            date_of_birth_str = student.date_of_birth
        else:
            date_of_birth_str = student.date_of_birth.isoformat()
    
    return StudentProfileResponse(
        first_name=student.first_name,
        last_name=student.last_name,
        full_name=student.full_name,
        phone=student.phone,
        email=user.email,
        date_of_birth=date_of_birth_str,
        gender=student.gender,
        current_address=student.current_address,
        highest_qualification=student.highest_qualification,
        college_name=student.college_name,
        college_id=student.college_id,
        course=student.course,
        branch=student.branch,
        passing_year=student.passing_year,
        percentage=student.percentage,
        cgpa=student.cgpa,
        technical_skills=student.technical_skills or [],
        soft_skills=student.soft_skills or [],
        experience_type=student.experience_type,
        internship_details=internship_details,
        projects=projects,
        languages=languages,
        # Return flat fields like technical_skills (for consistency)
        job_type=student.job_type or [],
        work_mode=student.work_mode or [],
        preferred_job_role=student.preferred_job_role or [],
        preferred_location=student.preferred_location or [],
        expected_salary=student.expected_salary,
        # Also include nested preference for backward compatibility
        preference={
            "job_type": student.job_type or [],
            "work_mode": student.work_mode or [],
            "preferred_job_role": student.preferred_job_role or [],
            "preferred_location": student.preferred_location or [],
            "expected_salary": student.expected_salary,
        } if (student.job_type or student.work_mode or student.preferred_job_role or student.preferred_location or student.expected_salary) else None,
        github_profile=student.github_profile,
        linkedin_profile=student.linkedin_profile,
        portfolio_url=student.portfolio_url,
        coding_platforms=student.coding_platforms or {},
        resume_url=student.resume_url,
        id=student_id,
        is_active=user.is_active,
        created_at=student.created_at,
        updated_at=student.updated_at,
        profile_completeness=None
    )


# ==================== Profile CRUD Endpoints ====================

@router.get("/profile", response_model=StudentProfileResponse)
async def get_my_profile(
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """
    Get current student's comprehensive profile
    
    **Auth**: Student (JWT required)
    
    Returns all profile fields matching the frontend schema exactly.
    """
    try:
        student = await get_student_by_user_id(db, current_user.id)
        
        if not student:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Student profile not found. Please complete your profile."
            )
        
        return build_profile_response(student, current_user)
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error in get_my_profile: {str(e)}")
        print(traceback.format_exc())
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Internal server error: {str(e)}"
        )


@router.put("/profile", response_model=StudentProfileResponse)
async def update_my_profile(
    profile_update: StudentProfileUpdate,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """
    Update or create student profile - Upsert operation
    
    **Auth**: Student (JWT required)
    
    All fields are optional - only provided fields will be updated.
    Supports partial updates.
    Creates profile if it doesn't exist.
    
    Handles nested objects (internships, projects, languages) properly.
    """
    try:
        student = await get_student_by_user_id(db, current_user.id)
        
        # Get update data (only fields that are provided)
        update_data = profile_update.model_dump(exclude_unset=True, exclude_none=False)
        
        # Handle nested preference object - extract to flat fields
        if 'preference' in update_data and update_data['preference']:
            preference_data = update_data.pop('preference')
            if isinstance(preference_data, dict):
                # Map nested preference fields to flat model columns
                if 'job_type' in preference_data:
                    update_data['job_type'] = preference_data['job_type']
                if 'work_mode' in preference_data:
                    update_data['work_mode'] = preference_data['work_mode']
                if 'preferred_job_role' in preference_data:
                    update_data['preferred_job_role'] = preference_data['preferred_job_role']
                if 'preferred_location' in preference_data:
                    update_data['preferred_location'] = preference_data['preferred_location']
                if 'expected_salary' in preference_data:
                    update_data['expected_salary'] = preference_data['expected_salary']
        
        # Handle nested Pydantic models - convert to dicts
        if 'internship_details' in update_data and update_data['internship_details']:
            internship_list = []
            for intern in update_data['internship_details']:
                if isinstance(intern, dict):
                    internship_list.append(intern)
                elif hasattr(intern, 'model_dump'):
                    internship_list.append(intern.model_dump())
                elif hasattr(intern, 'dict'):
                    internship_list.append(intern.dict())
                else:
                    internship_list.append(intern)
            update_data['internship_details'] = internship_list
        
        if 'projects' in update_data and update_data['projects']:
            project_list = []
            for proj in update_data['projects']:
                if isinstance(proj, dict):
                    project_list.append(proj)
                elif hasattr(proj, 'model_dump'):
                    project_list.append(proj.model_dump())
                elif hasattr(proj, 'dict'):
                    project_list.append(proj.dict())
                else:
                    project_list.append(proj)
            update_data['projects'] = project_list
        
        if 'languages' in update_data and update_data['languages']:
            language_list = []
            for lang in update_data['languages']:
                if isinstance(lang, dict):
                    language_list.append(lang)
                elif hasattr(lang, 'model_dump'):
                    language_list.append(lang.model_dump())
                elif hasattr(lang, 'dict'):
                    language_list.append(lang.dict())
                else:
                    language_list.append(lang)
            update_data['languages'] = language_list
        
        # Process date_of_birth conversion before creating/updating
        if 'date_of_birth' in update_data and update_data['date_of_birth']:
            try:
                from datetime import datetime
                if isinstance(update_data['date_of_birth'], str):
                    update_data['date_of_birth'] = datetime.strptime(update_data['date_of_birth'], '%Y-%m-%d').date()
            except (ValueError, TypeError):
                # If date parsing fails, remove this field
                print(f"Warning: Invalid date format for date_of_birth: {update_data['date_of_birth']}")
                update_data.pop('date_of_birth', None)
        
        # If student doesn't exist, create a new one
        if not student:
            # Determine full_name - required field
            first_name = update_data.get('first_name', '')
            last_name = update_data.get('last_name', '')
            full_name = update_data.get('full_name', '')
            
            if not full_name:
                if first_name or last_name:
                    full_name = f"{first_name} {last_name}".strip()
                else:
                    # Use user's email username or email as fallback
                    full_name = current_user.email.split('@')[0] if current_user.email else 'Student'
            
            # Prepare student creation data
            student_data = {
                'user_id': current_user.id,
                'full_name': full_name
            }
            
            # Add all valid fields from update_data
            for field, value in update_data.items():
                if field != 'full_name' and hasattr(Student, field):
                    # Handle None values for optional fields
                    if value is None and field in ['date_of_birth', 'percentage', 'cgpa', 'expected_salary', 'college_id']:
                        student_data[field] = None
                    elif value is not None:
                        student_data[field] = value
            
            # Create new student record
            student = Student(**student_data)
            db.add(student)
        else:
            # Update existing student
            # Update full_name if first_name or last_name changed
            if 'first_name' in update_data or 'last_name' in update_data:
                first = update_data.get('first_name', student.first_name) or student.first_name or ''
                last = update_data.get('last_name', student.last_name) or student.last_name or ''
                if first or last:
                    update_data['full_name'] = f"{first} {last}".strip()
            
            # Apply updates to student object
            for field, value in update_data.items():
                if hasattr(student, field):
                    # Handle None values - set to None explicitly for optional fields
                    if value is None and field in ['date_of_birth', 'percentage', 'cgpa', 'expected_salary', 'college_id']:
                        setattr(student, field, None)
                    elif value is not None:
                        # Ensure arrays are properly set (for JSONB fields)
                        if field in ['job_type', 'work_mode', 'preferred_job_role', 'preferred_location', 
                                     'technical_skills', 'soft_skills']:
                            # Ensure value is a list
                            if isinstance(value, list):
                                setattr(student, field, value)
                            else:
                                # Convert to list if not already
                                setattr(student, field, [value] if value else [])
                        else:
                            setattr(student, field, value)
        
        await db.commit()
        await db.refresh(student)
        
        return build_profile_response(student, current_user)
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error in update_my_profile: {str(e)}")
        print(traceback.format_exc())
        await db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to update profile: {str(e)}"
        )


# ==================== Resume CRUD Endpoints ====================

@router.post("/resume", status_code=status.HTTP_201_CREATED)
async def upload_resume(
    file: UploadFile = File(...),
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """
    Upload student resume (PDF only, max 5MB)
    
    **Auth**: Student (JWT required)
    
    **Validation**:
    - File type: PDF only
    - File size: Maximum 5MB
    """
    try:
        # Validate file type
        if not file.filename or not file.filename.lower().endswith('.pdf'):
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
        
        # Get student, create if doesn't exist
        student = await get_student_by_user_id(db, current_user.id)
        
        if not student:
            # Create basic student profile if it doesn't exist
            full_name = current_user.email.split('@')[0] if current_user.email else 'Student'
            student = Student(
                user_id=current_user.id,
                full_name=full_name
            )
            db.add(student)
            await db.flush()  # Flush to get student.id
        
        # Delete old resume if exists
        if student.resume_url:
            delete_resume(student.resume_url)
        
        # Upload new resume
        student_id = str(student.id) if hasattr(student, 'id') and student.id else str(current_user.id)
        resume_url = save_resume(content, file.filename, student_id)
        
        # Update student record
        student.resume_url = resume_url
        await db.commit()
        await db.refresh(student)
        
        return {
            "message": "Resume uploaded successfully",
            "resume_url": resume_url
        }
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error in upload_resume: {str(e)}")
        print(traceback.format_exc())
        await db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to upload resume: {str(e)}"
        )


@router.delete("/resume")
async def delete_resume(
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """
    Delete student resume
    
    **Auth**: Student (JWT required)
    """
    try:
        student = await get_student_by_user_id(db, current_user.id)
        
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
        
        # Delete resume
        delete_resume(student.resume_url)
        
        # Update student record
        student.resume_url = None
        await db.commit()
        
        return {"message": "Resume deleted successfully"}
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error in delete_resume: {str(e)}")
        print(traceback.format_exc())
        await db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to delete resume: {str(e)}"
        )


# ==================== Profile Completeness Endpoint ====================

@router.get("/profile-completeness", response_model=ProfileCompletenessResponse)
async def get_profile_completeness(
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """
    Get profile completeness percentage and suggestions
    
    **Auth**: Student (JWT required)
    
    Returns:
    - percentage: 0-100 completeness score
    - missing_fields: List of missing required/important fields
    - suggestions: Helpful suggestions to improve profile
    - is_complete: Boolean indicating if profile is complete (>=80%)
    """
    try:
        student = await get_student_by_user_id(db, current_user.id)
        
        if not student:
            # If no profile exists, return 0% completeness
            return ProfileCompletenessResponse(
                percentage=0,
                missing_fields=[
                    "first_name", "last_name", "phone", "date_of_birth",
                    "gender", "current_address", "highest_qualification",
                    "college_name", "course", "branch", "passing_year",
                    "technical_skills", "soft_skills", "resume_url"
                ],
                suggestions=[
                    "Create your profile to get started",
                    "Fill in your personal details",
                    "Add your education information",
                    "Upload your resume"
                ],
                is_complete=False
            )
        
        # Define field categories with weights
        # Personal Details (20%)
        personal_fields = {
            "first_name": (student.first_name, "First Name"),
            "last_name": (student.last_name, "Last Name"),
            "phone": (student.phone, "Mobile Number"),
            "date_of_birth": (student.date_of_birth, "Date of Birth"),
            "gender": (student.gender, "Gender"),
            "current_address": (student.current_address, "Current Address"),
        }
        
        # Education Details (25%)
        education_fields = {
            "highest_qualification": (student.highest_qualification, "Highest Qualification"),
            "college_name": (student.college_name, "College/University Name"),
            "course": (student.course, "Course"),
            "branch": (student.branch, "Branch"),
            "passing_year": (student.passing_year, "Year of Passing"),
            "percentage": (student.percentage, "Percentage"),
            "cgpa": (student.cgpa, "CGPA"),
        }
        
        # Skills (15%)
        skills_fields = {
            "technical_skills": (student.technical_skills and len(student.technical_skills) > 0, "Technical Skills"),
            "soft_skills": (student.soft_skills and len(student.soft_skills) > 0, "Soft Skills"),
        }
        
        # Experience (10%)
        experience_fields = {
            "experience_type": (student.experience_type, "Experience Type"),
            "internship_details": (student.internship_details and len(student.internship_details) > 0, "Internship Details"),
            "projects": (student.projects and len(student.projects) > 0, "Projects"),
        }
        
        # Languages (5%)
        languages_fields = {
            "languages": (student.languages and len(student.languages) > 0, "Languages"),
        }
        
        # Job Preferences (15%)
        preferences_fields = {
            "job_type": (student.job_type and len(student.job_type) > 0, "Job Type"),
            "work_mode": (student.work_mode and len(student.work_mode) > 0, "Work Mode"),
            "preferred_job_role": (student.preferred_job_role and len(student.preferred_job_role) > 0, "Preferred Job Role"),
            "preferred_location": (student.preferred_location and len(student.preferred_location) > 0, "Preferred Location"),
        }
        
        # Technical Profile Links (5%)
        links_fields = {
            "github_profile": (student.github_profile, "GitHub Profile"),
            "linkedin_profile": (student.linkedin_profile, "LinkedIn Profile"),
            "portfolio_url": (student.portfolio_url, "Portfolio URL"),
            "coding_platforms": (student.coding_platforms and len(student.coding_platforms) > 0, "Coding Platforms"),
        }
        
        # Resume (5%)
        resume_fields = {
            "resume_url": (student.resume_url, "Resume"),
        }
        
        # Calculate completeness for each category
        def check_field(value):
            """Check if field has a value"""
            if isinstance(value, bool):
                return value
            if isinstance(value, (list, dict)):
                return len(value) > 0 if value else False
            return value is not None and str(value).strip() != ""
        
        def calculate_category_score(fields_dict):
            """Calculate score for a category"""
            filled = sum(1 for value, _ in fields_dict.values() if check_field(value))
            total = len(fields_dict)
            return (filled / total) * 100 if total > 0 else 0
        
        # Calculate weighted scores
        personal_score = calculate_category_score(personal_fields) * 0.20
        education_score = calculate_category_score(education_fields) * 0.25
        skills_score = calculate_category_score(skills_fields) * 0.15
        experience_score = calculate_category_score(experience_fields) * 0.10
        languages_score = calculate_category_score(languages_fields) * 0.05
        preferences_score = calculate_category_score(preferences_fields) * 0.15
        links_score = calculate_category_score(links_fields) * 0.05
        resume_score = calculate_category_score(resume_fields) * 0.05
        
        # Total percentage
        total_percentage = int(
            personal_score + education_score + skills_score + 
            experience_score + languages_score + preferences_score + 
            links_score + resume_score
        )
        
        # Collect missing fields
        missing_fields = []
        all_fields = {
            **personal_fields,
            **education_fields,
            **skills_fields,
            **experience_fields,
            **languages_fields,
            **preferences_fields,
            **links_fields,
            **resume_fields
        }
        
        for field_key, (value, field_name) in all_fields.items():
            if not check_field(value):
                missing_fields.append(field_name)
        
        # Generate suggestions
        suggestions = []
        if not check_field(student.first_name) or not check_field(student.last_name):
            suggestions.append("Add your full name to personalize your profile")
        if not check_field(student.phone):
            suggestions.append("Add your mobile number for better communication")
        if not check_field(student.date_of_birth):
            suggestions.append("Add your date of birth")
        if not check_field(student.college_name):
            suggestions.append("Add your college/university name")
        if not check_field(student.course) or not check_field(student.branch):
            suggestions.append("Complete your education details (course and branch)")
        if not check_field(student.technical_skills):
            suggestions.append("Add your technical skills to improve job matching")
        if not check_field(student.soft_skills):
            suggestions.append("Add your soft skills to showcase your strengths")
        if not check_field(student.resume_url):
            suggestions.append("Upload your resume to apply for jobs")
        if not check_field(student.preferred_job_role):
            suggestions.append("Add preferred job roles to get better recommendations")
        if not check_field(student.linkedin_profile):
            suggestions.append("Add your LinkedIn profile to enhance your professional presence")
        if not check_field(student.projects):
            suggestions.append("Add your projects to showcase your work")
        
        # If profile is mostly complete, add encouraging message
        if total_percentage >= 80:
            suggestions.append("Great job! Your profile is looking good. Keep it updated!")
        elif total_percentage >= 60:
            suggestions.append("You're making good progress! Complete a few more fields to improve your profile.")
        else:
            suggestions.append("Complete more fields to increase your profile visibility to employers.")
        
        return ProfileCompletenessResponse(
            percentage=total_percentage,
            missing_fields=missing_fields,
            suggestions=suggestions[:10],  # Limit to 10 suggestions
            is_complete=total_percentage >= 80
        )
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error in get_profile_completeness: {str(e)}")
        print(traceback.format_exc())
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to calculate profile completeness: {str(e)}"
        )


# ==================== Resume File Serving Endpoint ====================

@router.get("/resumes/{student_id}/{filename}")
async def get_resume_file(
    student_id: str,
    filename: str,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """
    Serve resume file from local storage
    
    **Auth**: Student (JWT required)
    **Note**: Only the file owner can access their resume
    """
    try:
        # Verify student owns this resume
        student = await get_student_by_user_id(db, current_user.id)
        if not student:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Student profile not found"
            )
        
        # Verify the student_id matches
        student_id_str = str(student.id) if hasattr(student, 'id') and student.id else str(current_user.id)
        if student_id != student_id_str:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="You don't have permission to access this resume"
            )
        
        # Build file path
        file_path = RESUME_STORAGE_DIR / student_id / filename
        
        if not file_path.exists():
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Resume file not found"
            )
        
        # Return file with appropriate content type
        return FileResponse(
            path=str(file_path),
            media_type='application/pdf',
            filename=filename
        )
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error serving resume file: {str(e)}")
        print(traceback.format_exc())
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to serve resume file: {str(e)}"
        )
