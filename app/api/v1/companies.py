"""Company endpoints - placeholder."""

from fastapi import APIRouter

router = APIRouter()


@router.get("/")
async def list_companies():
    """List all companies (placeholder)."""
    return {"message": "Companies endpoint - to be implemented"}
