"""Authentication endpoints."""

import logging
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from google.auth.transport import requests as google_requests
from google.oauth2 import id_token as google_id_token
from app.config import settings
from app.core.security import (
    create_access_token,
    create_refresh_token,
    get_current_user,
    get_password_hash,
    verify_password,
)
from app.db.session import get_db
from app.models.user import User
from app.schemas.auth import (
    LoginRequest,
    LoginResponse,
    RegisterRequest,
    RegisterResponse,
    UserResponse,
    GoogleLoginRequest,
)

logger = logging.getLogger(__name__)

router = APIRouter()


@router.post("/register", response_model=RegisterResponse, status_code=status.HTTP_201_CREATED)
async def register(request: RegisterRequest, db: AsyncSession = Depends(get_db)):
    """Register a new user."""
    # Check if user already exists
    result = await db.execute(select(User).where(User.email == request.email))
    existing_user = result.scalar_one_or_none()

    if existing_user:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="User with this email already exists",
        )

    # Create new user
    new_user = User(
        email=request.email,
        username=request.username,
        password_hash=get_password_hash(request.password),
        role=request.role or "student",
    )

    db.add(new_user)
    await db.commit()
    await db.refresh(new_user)

    # Generate tokens
    access_token = create_access_token({"sub": str(new_user.id)})
    refresh_token = create_refresh_token({"sub": str(new_user.id)})

    return RegisterResponse(
        message="User registered successfully",
        access_token=access_token,
        refresh_token=refresh_token,
        token_type="bearer",
        user=UserResponse(
            id=new_user.id,
            email=new_user.email,
            username=new_user.username,
            role=new_user.role,
            is_active=new_user.is_active,
            created_at=new_user.created_at,
        ),
    )


@router.post("/login", response_model=LoginResponse)
async def login(request: LoginRequest, db: AsyncSession = Depends(get_db)):
    """Login with email and password."""
    # Find user
    result = await db.execute(select(User).where(User.email == request.email))
    user = result.scalar_one_or_none()

    if not user or not verify_password(request.password, user.password_hash):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect email or password",
        )

    if not user.is_active:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="User account is inactive",
        )

    # Generate tokens
    access_token = create_access_token({"sub": str(user.id)})
    refresh_token = create_refresh_token({"sub": str(user.id)})

    return LoginResponse(
        access_token=access_token,
        refresh_token=refresh_token,
        token_type="bearer",
        user=UserResponse(
            id=user.id,
            email=user.email,
            username=user.username,
            role=user.role,
            is_active=user.is_active,
            created_at=user.created_at,
        ),
    )

@router.post("/login/google", response_model=LoginResponse)
async def login_google(request: GoogleLoginRequest, db: AsyncSession = Depends(get_db)):
    """Login with Google id_token; auto-registers users if they do not already exist."""
    logger.info("Google login attempt received")
    
    if not settings.GOOGLE_CLIENT_ID:
        logger.error("Google client ID is not configured")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Google client ID is not configured",
        )

    try:
        logger.debug(f"Verifying Google token with client ID: {settings.GOOGLE_CLIENT_ID[:20]}...")
        payload = google_id_token.verify_oauth2_token(
            request.id_token,
            google_requests.Request(),
            settings.GOOGLE_CLIENT_ID,
        )
        logger.info(f"Google token verified successfully for email: {payload.get('email')}")
    except ValueError as e:
        logger.warning(f"Invalid Google token (ValueError): {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=f"Invalid Google token: {str(e)}",
        )
    except Exception as e:
        # Catch any other exceptions from Google auth library
        logger.error(f"Google token verification failed: {type(e).__name__}: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Google token verification failed: {str(e)}",
        )

    email = payload.get("email")
    email_verified = payload.get("email_verified")
    name = payload.get("name", "")
    given_name = payload.get("given_name", "")
    family_name = payload.get("family_name", "")

    if not email:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Google token missing email",
        )

    if email_verified is False:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Google email not verified",
        )

    # Check if user exists
    result = await db.execute(select(User).where(User.email == email))
    user = result.scalar_one_or_none()

    # Auto-register user if they don't exist
    if not user:
        logger.info(f"Auto-registering new user from Google: {email}")
        
        # Generate username from email or name
        username = None
        if name:
            # Use name as username, make it URL-safe
            username = name.lower().replace(" ", "_")[:150]
        elif given_name or family_name:
            username = f"{given_name}_{family_name}".lower().replace(" ", "_")[:150]
        else:
            # Fallback to email prefix
            username = email.split("@")[0][:150]
        
        # Ensure username is unique by appending a number if needed
        base_username = username
        counter = 1
        while True:
            existing_username = await db.execute(
                select(User).where(User.username == username)
            )
            if existing_username.scalar_one_or_none() is None:
                break
            username = f"{base_username}_{counter}"[:150]
            counter += 1
        
        # Create new user
        # OAuth users don't need a password hash
        new_user = User(
            email=email,
            username=username,
            password_hash=None,  # No password for OAuth users
            role="student",  # Default role
            is_active=True,
            is_verified=True,  # Google verified email
        )
        
        db.add(new_user)
        await db.commit()
        await db.refresh(new_user)
        user = new_user
        logger.info(f"Successfully auto-registered user: {email} with username: {username}")

    if not user.is_active:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="User account is inactive",
        )

    access_token = create_access_token({"sub": str(user.id)})
    refresh_token = create_refresh_token({"sub": str(user.id)})

    return LoginResponse(
        access_token=access_token,
        refresh_token=refresh_token,
        token_type="bearer",
        user=UserResponse(
            id=user.id,
            email=user.email,
            username=user.username,
            role=user.role,
            is_active=user.is_active,
            created_at=user.created_at,
        ),
    )


@router.get("/me", response_model=UserResponse)
async def get_current_user_info(current_user: User = Depends(get_current_user)):
    """Get current user information."""
    return UserResponse(
        id=current_user.id,
        email=current_user.email,
        username=current_user.username,
        role=current_user.role,
        is_active=current_user.is_active,
        created_at=current_user.created_at,
    )


@router.post("/logout")
async def logout(current_user: User = Depends(get_current_user)):
    """Logout (client should discard tokens)."""
    return {"message": "Successfully logged out"}
