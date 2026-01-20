"""
API Dependencies
Common dependencies for API endpoints (authentication, authorization, etc.)
DEPRECATED: Use app.core.deps and app.core.security instead.
This file is kept for backward compatibility with student endpoints.
"""

from typing import Optional
from fastapi import Depends, HTTPException, status

from app.models.user import User

# Import from core modules instead of defining our own
from app.core.security import get_current_user as core_get_current_user, oauth2_scheme
from app.core.deps import get_db


# Re-export for backward compatibility
async def get_current_user(
    current_user: User = Depends(core_get_current_user)
) -> User:
    """
    Get current authenticated user from JWT token.
    Wrapper for backward compatibility - delegates to app.core.security.get_current_user
    """
    return current_user


async def get_current_admin_user(
    current_user: User = Depends(get_current_user)
) -> User:
    """
    Get current user and verify they have admin privileges
    """
    if not current_user.is_admin:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Not enough privileges. Admin access required."
        )
    return current_user


async def get_current_active_user(
    current_user: User = Depends(get_current_user)
) -> User:
    """
    Get current active user (alias for get_current_user)
    """
    return current_user


# Optional authentication (for public endpoints that can work with/without auth)
async def get_current_user_optional(
    current_user: Optional[User] = Depends(core_get_current_user)
) -> Optional[User]:
    """
    Get current user if authenticated, None otherwise.
    Note: This delegates to core_get_current_user which may raise exceptions.
    For truly optional auth, endpoints should handle the exception.
    """
    return current_user


# Role-based access control functions
async def require_admin_role(
    current_user: User = Depends(get_current_user)
) -> User:
    """
    Require SuperAdmin or Admin role
    """
    if current_user.role not in ["SuperAdmin", "Admin", "admin", "superadmin"]:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Access denied. Admin or SuperAdmin role required."
        )
    return current_user


async def require_placement_or_admin(
    current_user: User = Depends(get_current_user)
) -> User:
    """
    Require Placement, Admin, or SuperAdmin role
    """
    if current_user.role not in ["SuperAdmin", "Admin", "Placement", "admin", "superadmin", "placement"]:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Access denied. Placement, Admin, or SuperAdmin role required."
        )
    return current_user

