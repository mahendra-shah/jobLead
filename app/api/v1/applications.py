"""Application endpoints - placeholder."""

from fastapi import APIRouter

router = APIRouter()


@router.get("/")
async def list_applications():
    """List all applications (placeholder)."""
    return {"message": "Applications endpoint - to be implemented"}
