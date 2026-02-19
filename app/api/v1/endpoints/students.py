"""
Student Management API - CRUD with RBAC
- SuperAdmin/Admin: Full CRUD
- Placement: CRUD except delete
- Student: Self-service only
"""

from typing import Optional
from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, or_
from sqlalchemy.orm import joinedload

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
    
    # Create student (password is None - using Google OAuth)
    db_student = Student(
        email=student_in.email,
        password=None,  # No password - Google OAuth only
        first_name=student_in.first_name,
        last_name=student_in.last_name,
        phone=student_in.phone,
        college_id=student_in.college_id,
        degree=student_in.degree,
        branch=student_in.branch,
        passing_year=student_in.passing_year,
        cgpa=student_in.cgpa,
        is_active=True
    )
    
    db.add(db_student)
    await db.commit()
    await db.refresh(db_student)
    
    return db_student


@router.get("/students", response_model=StudentListResponse)
async def list_students(
    offset: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=100),
    college_id: Optional[int] = None,
    branch: Optional[str] = None,
    passing_year: Optional[int] = None,
    cgpa_min: Optional[float] = None,
    cgpa_max: Optional[float] = None,
    is_active: Optional[bool] = None,
    search: Optional[str] = None,
    sort_by: str = Query("created_at", regex="^(name|cgpa|created_at|updated_at)$"),
    sort_order: str = Query("desc", regex="^(asc|desc)$"),
    current_user: User = Depends(require_placement_or_admin),
    db: AsyncSession = Depends(get_db)
):
    """
    List students with filters and pagination (Admin/Placement)
    
    **RBAC**: SuperAdmin, Admin, Placement
    
    **Filters**:
    - `college_id`: Filter by college
    - `branch`: Filter by branch
    - `passing_year`: Filter by passing year
    - `cgpa_min`, `cgpa_max`: CGPA range
    - `is_active`: Active status
    - `search`: Search in name or email
    - `sort_by`: Sort field (name, cgpa, created_at, updated_at)
    - `sort_order`: asc or desc
    """
    # Build query
    query = select(Student).options(joinedload(Student.college))
    
    # Apply filters
    if college_id:
        query = query.where(Student.college_id == college_id)
    
    if branch:
        query = query.where(Student.branch == branch)
    
    if passing_year:
        query = query.where(Student.passing_year == passing_year)
    
    if cgpa_min is not None:
        query = query.where(Student.cgpa >= cgpa_min)
    
    if cgpa_max is not None:
        query = query.where(Student.cgpa <= cgpa_max)
    
    if is_active is not None:
        query = query.where(Student.is_active == is_active)
    
    if search:
        search_term = f"%{search}%"
        query = query.where(
            or_(
                Student.first_name.ilike(search_term),
                Student.last_name.ilike(search_term),
                Student.email.ilike(search_term)
            )
        )
    
    # Count total
    count_query = select(func.count()).select_from(Student)
    if college_id:
        count_query = count_query.where(Student.college_id == college_id)
    if branch:
        count_query = count_query.where(Student.branch == branch)
    if passing_year:
        count_query = count_query.where(Student.passing_year == passing_year)
    if cgpa_min is not None:
        count_query = count_query.where(Student.cgpa >= cgpa_min)
    if cgpa_max is not None:
        count_query = count_query.where(Student.cgpa <= cgpa_max)
    if is_active is not None:
        count_query = count_query.where(Student.is_active == is_active)
    if search:
        search_term = f"%{search}%"
        count_query = count_query.where(
            or_(
                Student.first_name.ilike(search_term),
                Student.last_name.ilike(search_term),
                Student.email.ilike(search_term)
            )
        )
    
    total_result = await db.execute(count_query)
    total = total_result.scalar()
    
    # Apply sorting
    if sort_by == "name":
        order_column = Student.first_name
    elif sort_by == "cgpa":
        order_column = Student.cgpa
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
    student_id: int,
    current_user: User = Depends(require_placement_or_admin),
    db: AsyncSession = Depends(get_db)
):
    """
    Get student by ID (Admin/Placement)
    
    **RBAC**: SuperAdmin, Admin, Placement
    """
    result = await db.execute(
        select(Student)
        .options(joinedload(Student.college))
        .where(Student.id == student_id)
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
    student_id: int,
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
    update_data = student_in.dict(exclude_unset=True)
    for field, value in update_data.items():
        setattr(student, field, value)
    
    await db.commit()
    await db.refresh(student)
    
    return student


@router.delete("/students/{student_id}", response_model=StudentResponse)
async def delete_student(
    student_id: int,
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
    
    # Get the associated user
    result = await db.execute(
        select(User).where(User.id == student.user_id)
    )
    user = result.scalar_one_or_none()
    
    if user:
        # Deactivate user account
        user.is_active = False
        db.add(user)
    
    # Set student status to inactive
    student.status = "inactive"
    db.add(student)
    
    await db.commit()
    await db.refresh(student)
    
    return student


@router.patch("/students/{student_id}/status", response_model=StudentResponse)
async def update_student_status(
    student_id: int,
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
    
    student.is_active = is_active
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
            
            # Create student (password is None - using Google OAuth)
            db_student = Student(
                email=student_data.email,
                password=None,  # No password - Google OAuth only
                first_name=student_data.first_name,
                last_name=student_data.last_name,
                phone=student_data.phone,
                college_id=student_data.college_id,
                degree=student_data.degree,
                branch=student_data.branch,
                passing_year=student_data.passing_year,
                cgpa=student_data.cgpa,
                is_active=True
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
    format: str = Query("csv", regex="^(csv|json)$"),
    current_user: User = Depends(require_placement_or_admin),
    db: AsyncSession = Depends(get_db)
):
    """
    Export all students to CSV or JSON (Admin/Placement)
    
    **RBAC**: SuperAdmin, Admin, Placement
    """
    result = await db.execute(
        select(Student).options(joinedload(Student.college))
    )
    students = result.scalars().all()
    
    if format == "json":
        return {
            "total": len(students),
            "students": [
                {
                    "id": s.id,
                    "email": s.email,
                    "first_name": s.first_name,
                    "last_name": s.last_name,
                    "phone": s.phone,
                    "college": s.college.name if s.college else None,
                    "degree": s.degree,
                    "branch": s.branch,
                    "passing_year": s.passing_year,
                    "cgpa": s.cgpa,
                    "is_active": s.is_active
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
        "id", "email", "first_name", "last_name", "phone",
        "college", "degree", "branch", "passing_year", "cgpa", "is_active"
    ])
    writer.writeheader()
    
    for s in students:
        writer.writerow({
            "id": s.id,
            "email": s.email,
            "first_name": s.first_name,
            "last_name": s.last_name,
            "phone": s.phone,
            "college": s.college.name if s.college else "",
            "degree": s.degree or "",
            "branch": s.branch or "",
            "passing_year": s.passing_year or "",
            "cgpa": s.cgpa or "",
            "is_active": s.is_active
        })
    
    output.seek(0)
    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=students.csv"}
    )
