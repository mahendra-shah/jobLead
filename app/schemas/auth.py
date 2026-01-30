"""Authentication schemas."""

from __future__ import annotations  # Enable forward references

from datetime import datetime
from typing import Optional
from uuid import UUID

from pydantic import BaseModel, EmailStr, Field


class RegisterRequest(BaseModel):
    """Register request schema."""

    email: EmailStr
    password: str = Field(..., min_length=8, description="Password must be at least 8 characters")
    role: Optional[str] = "student"
    username: Optional[str] = None


class RegisterResponse(BaseModel):
    """Register response schema."""

    message: str
    access_token: str
    refresh_token: str
    token_type: str
    user: "UserResponse"


class LoginRequest(BaseModel):
    """Login request schema."""

    email: EmailStr
    password: str


class LoginResponse(BaseModel):
    """Login response schema."""

    access_token: str
    refresh_token: str
    token_type: str
    user: "UserResponse"


class UserResponse(BaseModel):
    """User response schema."""

    id: UUID
    email: str
    username: Optional[str] = None
    role: str
    is_active: bool
    created_at: datetime

    class Config:
        from_attributes = True


# Rebuild models to resolve forward references
RegisterResponse.model_rebuild()
LoginResponse.model_rebuild()
