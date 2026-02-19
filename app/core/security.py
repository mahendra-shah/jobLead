"""Security utilities: JWT, password hashing, RBAC."""

from datetime import datetime, timedelta
from enum import Enum
from typing import Optional, Union

from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from jose import JWTError, jwt
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from app.config import settings
from app.db.session import get_db
from app.models.user import User

# HTTPBearer for simple token authentication in Swagger (just paste the access token)
security = HTTPBearer()


class Role(str, Enum):
    """User roles."""

    SUPER_ADMIN = "superadmin"
    ADMIN = "admin"
    PLACEMENT = "placement"
    STUDENT = "student"
    EMPLOYER = "employer"


# Role hierarchy (higher roles inherit lower role permissions)
ROLE_HIERARCHY = {
    Role.SUPER_ADMIN: [Role.SUPER_ADMIN, Role.ADMIN, Role.PLACEMENT, Role.STUDENT],
    Role.ADMIN: [Role.ADMIN, Role.PLACEMENT, Role.STUDENT],
    Role.PLACEMENT: [Role.PLACEMENT],
    Role.STUDENT: [Role.STUDENT],
    Role.EMPLOYER: [Role.EMPLOYER],
}


# Permission definitions
ROLE_PERMISSIONS = {
    Role.SUPER_ADMIN: ["*"],  # All permissions
    Role.ADMIN: [
        "users:create",
        "users:read",
        "users:update",
        "users:delete",
        "jobs:*",
        "students:*",
        "companies:*",
        "channels:*",
        "applications:read",
        "reports:*",
        "bulk:*",
    ],
    Role.PLACEMENT: [
        "jobs:create",
        "jobs:read",
        "jobs:update",
        "students:read",
        "students:update",
        "companies:create",
        "companies:read",
        "companies:update",
        "channels:*",
        "applications:read",
    ],
    Role.STUDENT: [
        "profile:read",
        "profile:update",
        "jobs:read",
        "applications:create",
        "applications:read",
        "applications:update",
    ],
    Role.EMPLOYER: [
        "company:read",
        "company:update",
        "jobs:create",
        "jobs:read",
        "jobs:update",
        "applications:read",
    ],
}


def create_access_token(data: dict, expires_delta: Optional[timedelta] = None) -> str:
    """Create JWT access token."""
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(
            minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES
        )
    to_encode.update({"exp": expire, "type": "access"})
    encoded_jwt = jwt.encode(to_encode, settings.SECRET_KEY, algorithm=settings.ALGORITHM)
    return encoded_jwt


def create_refresh_token(data: dict) -> str:
    """Create JWT refresh token."""
    to_encode = data.copy()
    expire = datetime.utcnow() + timedelta(days=settings.REFRESH_TOKEN_EXPIRE_DAYS)
    to_encode.update({"exp": expire, "type": "refresh"})
    encoded_jwt = jwt.encode(to_encode, settings.SECRET_KEY, algorithm=settings.ALGORITHM)
    return encoded_jwt


def decode_token(token: str) -> dict:
    """Decode and verify JWT token."""
    try:
        payload = jwt.decode(token, settings.SECRET_KEY, algorithms=[settings.ALGORITHM])
        return payload
    except JWTError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Could not validate credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )


async def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: AsyncSession = Depends(get_db),
) -> User:
    """Get current authenticated user from Bearer token."""
    token = credentials.credentials
    payload = decode_token(token)

    user_id: str = payload.get("sub")
    if user_id is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Could not validate credentials",
        )

    # Fetch user from database
    result = await db.execute(select(User).where(User.id == user_id))
    user = result.scalar_one_or_none()

    if user is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="User not found",
        )

    if not user.is_active:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Inactive user",
        )

    return user


def require_role(*allowed_roles: Role):
    """Dependency to check if user has required role."""

    async def role_checker(current_user: User = Depends(get_current_user)) -> User:
        user_role = Role(current_user.role)

        # Check if user's role is in allowed roles or has higher privileges
        for allowed_role in allowed_roles:
            if user_role in ROLE_HIERARCHY.get(user_role, []):
                if allowed_role in ROLE_HIERARCHY.get(user_role, []):
                    return current_user

        # Direct role match
        if user_role in allowed_roles:
            return current_user

        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"Insufficient permissions. Required: {[r.value for r in allowed_roles]}",
        )

    return role_checker


def check_permission(user_role: Role, permission: str) -> bool:
    """Check if a role has a specific permission."""
    permissions = ROLE_PERMISSIONS.get(user_role, [])

    # Wildcard permission
    if "*" in permissions:
        return True

    # Exact match
    if permission in permissions:
        return True

    # Resource wildcard (e.g., "jobs:*" matches "jobs:create")
    resource = permission.split(":")[0]
    if f"{resource}:*" in permissions:
        return True

    return False


def require_permission(permission: str):
    """Dependency to check if user has specific permission."""

    async def permission_checker(current_user: User = Depends(get_current_user)) -> User:
        user_role = Role(current_user.role)

        if not check_permission(user_role, permission):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Permission denied: {permission}",
            )

        return current_user

    return permission_checker
