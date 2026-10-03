"""Authentication API routes."""
from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, EmailStr
from sqlalchemy.ext.asyncio import AsyncSession
from app.database.session import get_session
from app.auth.service import AuthService, TenantService, TokenData
from app.observability.logging import get_logger


logger = get_logger(__name__)
router = APIRouter()


class TenantCreate(BaseModel):
    """Create tenant request."""
    name: str
    description: str = None


class UserRegister(BaseModel):
    """User registration request."""
    tenant_name: str
    email: EmailStr
    username: str
    password: str


class UserLogin(BaseModel):
    """User login request."""
    tenant_id: str
    email: EmailStr
    password: str


class AuthToken(BaseModel):
    """Authentication token response."""
    access_token: str
    token_type: str = "bearer"


class UserInfo(BaseModel):
    """User information response."""
    id: str
    email: str
    username: str
    tenant_id: str
    is_active: bool


def verify_token(authorization: str = None) -> TokenData:
    """Verify JWT token from Authorization header."""
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing or invalid authorization header")
    
    token = authorization[7:]  # Remove "Bearer "
    success, token_data, error = AuthService.verify_token(token)
    
    if not success:
        raise HTTPException(status_code=401, detail=f"Invalid token: {error}")
    
    return token_data


@router.post("/tenants", response_model=dict)
async def create_tenant(
    req: TenantCreate,
    session: AsyncSession = Depends(get_session),
):
    """Create a new tenant."""
    success, tenant, error = await TenantService.create_tenant(
        session,
        name=req.name,
        description=req.description,
    )
    
    if not success:
        raise HTTPException(status_code=400, detail=error)
    
    return {"id": tenant.id, "name": tenant.name}


@router.post("/register", response_model=UserInfo)
async def register(
    req: UserRegister,
    session: AsyncSession = Depends(get_session),
):
    """Register a new user."""
    # Create or get tenant
    success, tenant, error = await TenantService.create_tenant(session, req.tenant_name)
    if not success and "already exists" not in error:
        raise HTTPException(status_code=400, detail=error)
    
    # Get tenant if it already exists
    if not tenant:
        from sqlalchemy import select
        from app.database.models import Tenant
        result = await session.execute(select(Tenant).where(Tenant.name == req.tenant_name))
        tenant = result.scalar_one_or_none()
        if not tenant:
            raise HTTPException(status_code=400, detail="Failed to get tenant")
    
    # Register user
    success, user, error = await AuthService.register_user(
        session,
        tenant_id=tenant.id,
        email=req.email,
        username=req.username,
        password=req.password,
    )
    
    if not success:
        raise HTTPException(status_code=400, detail=error)
    
    return UserInfo(
        id=user.id,
        email=user.email,
        username=user.username,
        tenant_id=user.tenant_id,
        is_active=user.is_active,
    )


@router.post("/login", response_model=AuthToken)
async def login(
    req: UserLogin,
    session: AsyncSession = Depends(get_session),
):
    """Authenticate user and get token."""
    success, user, error = await AuthService.authenticate_user(
        session,
        tenant_id=req.tenant_id,
        email=req.email,
        password=req.password,
    )
    
    if not success:
        raise HTTPException(status_code=401, detail=error)
    
    # Create token
    token = AuthService.create_access_token(
        user_id=user.id,
        tenant_id=user.tenant_id,
        email=user.email,
    )
    
    logger.info(f"User logged in: {user.email}")
    
    return AuthToken(access_token=token)


@router.get("/me", response_model=UserInfo)
async def get_current_user(
    authorization: str = None,
    session: AsyncSession = Depends(get_session),
):
    """Get current user info."""
    token_data = verify_token(authorization)
    
    from sqlalchemy import select
    from app.database.models import User
    result = await session.execute(select(User).where(User.id == token_data.user_id))
    user = result.scalar_one_or_none()
    
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    
    return UserInfo(
        id=user.id,
        email=user.email,
        username=user.username,
        tenant_id=user.tenant_id,
        is_active=user.is_active,
    )
