"""
Student Management API - CRUD with RBAC
- SuperAdmin/Admin: Full CRUD
- Placement: CRUD except delete
- Student: Self-service only
"""

from typing import Optional
from uuid import UUID
from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, or_, cast, Integer

from app.api.deps import (
    require_admin_role,
    require_placement_or_admin,
    get_db
)
from app.models.user import User
from app.models.student import Student
from app.schemas.student import (
    StudentCreate,
    StudentUpdate,
    StudentResponse,
    StudentListResponse,
    BulkStudentCreate,
    BulkUploadResponse
)

router = APIRouter()


def _merge_extra_detail_with_passing_year(existing: Optional[dict], passing_year: Optional[int]) -> dict:
    extra_detail = dict(existing) if isinstance(existing, dict) else {}
    if passing_year is not None:
        extra_detail["passing_year"] = passing_year
    return extra_detail


def _get_personal_details(student: Student) -> dict:
    if isinstance(getattr(student, "extra_detail", None), dict):
        return student.extra_detail
    if isinstance(getattr(student, "personal_details", None), dict):
        return student.personal_details
    return {}


def _get_passing_year(student: Student) -> Optional[int]:
    details = _get_personal_details(student)
    passing_year = details.get("passing_year")
    try:
        return int(passing_year) if passing_year is not None else None
    except (TypeError, ValueError):
        return None


def _normalize_status(is_active: Optional[bool] = None, status_value: Optional[str] = None) -> Optional[str]:
    if status_value is not None:
        cleaned = status_value.strip().lower()
        if cleaned in {"active", "inactive", "placed"}:
            return cleaned
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="status must be one of: active, inactive, placed"
        )
    if is_active is None:
        return None
    return "active" if is_active else "inactive"


# ==================== Student CRUD (Admin/Placement) ====================

@router.post("/students", response_model=StudentResponse, status_code=status.HTTP_201_CREATED)
async def create_student(
    student_in: StudentCreate,
    current_user: User = Depends(require_placement_or_admin),
    db: AsyncSession = Depends(get_db)
):
    """
    Create a new student (Admin/Placement)
    
    **RBAC**: SuperAdmin, Admin, Placement
    """
    # Check if email already exists
    result = await db.execute(
        select(Student).where(Student.email == student_in.email)
    )
    existing_student = result.scalar_one_or_none()
    
    if existing_student:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Student with email {student_in.email} already exists"
        )
    
    normalized_status = _normalize_status(status_value=student_in.status)

    # Create student
    db_student = Student(
        full_name=student_in.full_name,
        email=student_in.email,
        phone=student_in.phone,
        job_category=student_in.job_category,
        status=normalized_status or "active",
        extra_detail=_merge_extra_detail_with_passing_year(None, student_in.passing_year),
        personal_details=_merge_extra_detail_with_passing_year(None, student_in.passing_year),
    )
    
    db.add(db_student)
    await db.commit()
    await db.refresh(db_student)
    
    return db_student


@router.get("/students", response_model=StudentListResponse)
async def list_students(
    offset: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=100),
    job_category: Optional[str] = None,
    passing_year: Optional[int] = None,
    is_active: Optional[bool] = None,
    status_filter: Optional[str] = Query(None, alias="status"),
    search: Optional[str] = None,
    sort_by: str = Query("created_at", pattern="^(name|created_at|updated_at)$"),
    sort_order: str = Query("desc", pattern="^(asc|desc)$"),
    current_user: User = Depends(require_placement_or_admin),
    db: AsyncSession = Depends(get_db)
):
    """
    List students with filters and pagination (Admin/Placement)
    
    **RBAC**: SuperAdmin, Admin, Placement
    
    **Filters**:
    - `job_category`: Filter by job category
    - `passing_year`: Filter by passing year
    - `is_active`: Backward-compatible active flag (maps to status)
    - `status`: active/inactive/placed
    - `search`: Search in name or email
    - `sort_by`: Sort field (name, created_at, updated_at)
    - `sort_order`: asc or desc
    """
    normalized_status = _normalize_status(is_active=is_active, status_value=status_filter)

    # Build query
    query = select(Student)
    
    # Apply filters
    if job_category:
        query = query.where(Student.job_category == job_category)
    
    if passing_year:
        query = query.where(cast(Student.extra_detail['passing_year'].astext, Integer) == passing_year)
    
    if normalized_status is not None:
        query = query.where(Student.status == normalized_status)
    
    if search:
        search_term = f"%{search}%"
        query = query.where(
            or_(
                Student.full_name.ilike(search_term),
                Student.email.ilike(search_term)
            )
        )
    
    # Count total
    count_query = select(func.count()).select_from(Student)
    if job_category:
        count_query = count_query.where(Student.job_category == job_category)
    if passing_year:
        count_query = count_query.where(cast(Student.extra_detail['passing_year'].astext, Integer) == passing_year)
    if normalized_status is not None:
        count_query = count_query.where(Student.status == normalized_status)
    if search:
        search_term = f"%{search}%"
        count_query = count_query.where(
            or_(
                Student.full_name.ilike(search_term),
                Student.email.ilike(search_term)
            )
        )
    
    total_result = await db.execute(count_query)
    total = total_result.scalar()
    
    # Apply sorting
    if sort_by == "name":
        order_column = Student.full_name
    elif sort_by == "updated_at":
        order_column = Student.updated_at
    else:
        order_column = Student.created_at
    
    if sort_order == "desc":
        order_column = order_column.desc()
    else:
        order_column = order_column.asc()
    
    query = query.order_by(order_column)
    
    # Apply pagination
    query = query.offset(offset).limit(limit)
    
    # Execute
    result = await db.execute(query)
    students = result.scalars().all()
    
    return StudentListResponse(
        total=total,
        limit=limit,
        offset=offset,
        students=students
    )


@router.get("/students/{student_id}", response_model=StudentResponse)
async def get_student(
    student_id: UUID,
    current_user: User = Depends(require_placement_or_admin),
    db: AsyncSession = Depends(get_db)
):
    """
    Get student by ID (Admin/Placement)
    
    **RBAC**: SuperAdmin, Admin, Placement
    """
    result = await db.execute(
        select(Student).where(Student.id == student_id)
    )
    student = result.scalar_one_or_none()
    
    if not student:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Student with id {student_id} not found"
        )
    
    return student


@router.put("/students/{student_id}", response_model=StudentResponse)
async def update_student(
    student_id: UUID,
    student_in: StudentUpdate,
    current_user: User = Depends(require_placement_or_admin),
    db: AsyncSession = Depends(get_db)
):
    """
    Update student (Admin/Placement)
    
    **RBAC**: SuperAdmin, Admin, Placement
    """
    result = await db.execute(
        select(Student).where(Student.id == student_id)
    )
    student = result.scalar_one_or_none()
    
    if not student:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Student with id {student_id} not found"
        )
    
    # Update fields
    update_data = student_in.model_dump(exclude_unset=True)
    if "status" in update_data and update_data["status"] is not None:
        update_data["status"] = _normalize_status(status_value=update_data["status"])
    if "passing_year" in update_data:
        passing_year = update_data.pop("passing_year")
        student.extra_detail = _merge_extra_detail_with_passing_year(
            student.extra_detail,
            passing_year
        )
        student.personal_details = _merge_extra_detail_with_passing_year(
            student.personal_details,
            passing_year
        )
    for field, value in update_data.items():
        setattr(student, field, value)
    
    await db.commit()
    await db.refresh(student)
    
    return student


@router.delete("/students/{student_id}", response_model=StudentResponse)
async def delete_student(
    student_id: UUID,
    current_user: User = Depends(require_admin_role),  # Only SuperAdmin/Admin
    db: AsyncSession = Depends(get_db)
):
    """
    Deactivate student (SuperAdmin/Admin ONLY)
    
    **RBAC**: SuperAdmin, Admin (Placement role gets 403 Forbidden)
    
    This endpoint sets the student status to 'inactive' and deactivates the user account
    instead of permanently deleting the record. This preserves data integrity.
    
    This endpoint is restricted to SuperAdmin and Admin roles only.
    Placement role cannot deactivate students.
    """
    result = await db.execute(
        select(Student).where(Student.id == student_id)
    )
    student = result.scalar_one_or_none()
    
    if not student:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Student with id {student_id} not found"
        )
    
    # Get the associated user (if this relationship exists)
    # Commented out as user_id field has been removed from Student model
    # result = await db.execute(
    #     select(User).where(User.id == student.user_id)
    # )
    # user = result.scalar_one_or_none()
    #
    # if user:
    #     # Deactivate user account
    #     user.is_active = False
    #     db.add(user)
    
    # Set student status to inactive
    student.status = "inactive"
    db.add(student)
    
    await db.commit()
    await db.refresh(student)
    
    return student


@router.patch("/students/{student_id}/status", response_model=StudentResponse)
async def update_student_status(
    student_id: UUID,
    is_active: bool,
    current_user: User = Depends(require_placement_or_admin),
    db: AsyncSession = Depends(get_db)
):
    """
    Activate/deactivate student (Admin/Placement)
    
    **RBAC**: SuperAdmin, Admin, Placement
    """
    result = await db.execute(
        select(Student).where(Student.id == student_id)
    )
    student = result.scalar_one_or_none()
    
    if not student:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Student with id {student_id} not found"
        )
    
    student.status = "active" if is_active else "inactive"
    await db.commit()
    await db.refresh(student)
    
    return student


# ==================== Bulk Operations ====================

@router.post("/students/bulk", response_model=BulkUploadResponse)
async def bulk_create_students(
    bulk_in: BulkStudentCreate,
    current_user: User = Depends(require_admin_role),  # Only SuperAdmin/Admin
    db: AsyncSession = Depends(get_db)
):
    """
    Bulk create students from list (SuperAdmin/Admin)
    
    **RBAC**: SuperAdmin, Admin
    """
    success = 0
    failed = 0
    errors = []
    
    for idx, student_data in enumerate(bulk_in.students):
        try:
            # Check if email exists
            result = await db.execute(
                select(Student).where(Student.email == student_data.email)
            )
            if result.scalar_one_or_none():
                failed += 1
                errors.append({
                    "index": idx,
                    "email": student_data.email,
                    "error": "Email already exists"
                })
                continue
            
            normalized_status = _normalize_status(status_value=student_data.status)

            # Create student
            db_student = Student(
                full_name=student_data.full_name,
                email=student_data.email,
                phone=student_data.phone,
                job_category=student_data.job_category,
                status=normalized_status or "active",
                extra_detail=_merge_extra_detail_with_passing_year(None, student_data.passing_year),
                personal_details=_merge_extra_detail_with_passing_year(None, student_data.passing_year),
            )
            db.add(db_student)
            success += 1
            
        except Exception as e:
            failed += 1
            errors.append({
                "index": idx,
                "email": student_data.email,
                "error": str(e)
            })
    
    await db.commit()
    
    return BulkUploadResponse(
        success=success,
        failed=failed,
        total=len(bulk_in.students),
        errors=errors
    )


@router.get("/students/export")
async def export_students(
    format: str = Query("csv", pattern="^(csv|json)$"),
    current_user: User = Depends(require_placement_or_admin),
    db: AsyncSession = Depends(get_db)
):
    """
    Export all students to CSV or JSON (Admin/Placement)
    
    **RBAC**: SuperAdmin, Admin, Placement
    """
    result = await db.execute(
        select(Student)
    )
    students = result.scalars().all()
    
    if format == "json":
        return {
            "total": len(students),
            "students": [
                {
                    "id": s.id,
                    "email": s.email,
                    "full_name": s.full_name,
                    "phone": s.phone,
                    "job_category": s.job_category,
                    "passing_year": _get_passing_year(s),
                    "status": s.status
                }
                for s in students
            ]
        }
    
    # CSV export
    import csv
    from io import StringIO
    from fastapi.responses import StreamingResponse
    
    output = StringIO()
    writer = csv.DictWriter(output, fieldnames=[
        "id", "email", "full_name", "phone",
        "job_category", "passing_year", "status"
    ])
    writer.writeheader()
    
    for s in students:
        writer.writerow({
            "id": s.id,
            "email": s.email,
            "full_name": s.full_name,
            "phone": s.phone,
            "job_category": s.job_category or "",
            "passing_year": _get_passing_year(s) or "",
            "status": s.status or ""
        })
    
    output.seek(0)
    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=students.csv"}
    )
