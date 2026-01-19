"""Student endpoints - placeholder."""

from fastapi import APIRouter

router = APIRouter()


@router.get("/")
async def list_students():
    """List all students (placeholder)."""
    return {"message": "Students endpoint - to be implemented"}
