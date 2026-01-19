"""Channel endpoints - placeholder."""

from fastapi import APIRouter

router = APIRouter()


@router.get("/")
async def list_channels():
    """List all channels (placeholder)."""
    return {"message": "Channels endpoint - to be implemented"}
