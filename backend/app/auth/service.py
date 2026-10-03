"""Authentication and authorization services."""
from datetime import datetime, timedelta, timezone
from typing import Optional, Tuple

from app.config import settings
from app.database.models import Tenant, User
from app.observability.logging import get_logger
from jwt import InvalidTokenError, decode, encode
from passlib.context import CryptContext
from pydantic import BaseModel, EmailStr
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

logger = get_logger(__name__)
pwd_context = CryptContext(schemes=["argon2"], deprecated="auto")


class TokenData(BaseModel):
    """JWT token payload."""

    user_id: str
    tenant_id: str
    email: str
    exp: datetime


class UserInfo(BaseModel):
    """User information model."""

    id: str
    tenant_id: str
    email: str
    username: str
    is_active: bool


class AuthService:
    """Authentication service."""

    @staticmethod
    def hash_password(password: str) -> str:
        """Hash a password."""
        return pwd_context.hash(password)

    @staticmethod
    def verify_password(plain_password: str, hashed_password: str) -> bool:
        """Verify a password against its hash."""
        return pwd_context.verify(plain_password, hashed_password)

    @staticmethod
    def create_access_token(
        user_id: str, tenant_id: str, email: str, expires_in_hours: Optional[int] = None
    ) -> str:
        """Create a JWT access token."""
        if expires_in_hours is None:
            expires_in_hours = settings.jwt.expiration_hours

        expire = datetime.now(timezone.utc) + timedelta(hours=expires_in_hours)

        payload = {
            "user_id": user_id,
            "tenant_id": tenant_id,
            "email": email,
            "exp": expire,
        }

        token = encode(payload, settings.jwt.secret, algorithm=settings.jwt.algorithm)
        return token

    @staticmethod
    def verify_token(token: str) -> Tuple[bool, Optional[TokenData], Optional[str]]:
        """Verify a JWT token."""
        try:
            payload = decode(
                token, settings.jwt.secret, algorithms=[settings.jwt.algorithm]
            )
            token_data = TokenData(**payload)
            return True, token_data, None
        except InvalidTokenError as e:
            logger.warning(f"Invalid token: {str(e)}")
            return False, None, str(e)
        except Exception as e:
            logger.error(f"Token verification error: {str(e)}")
            return False, None, str(e)

    @staticmethod
    async def authenticate_user(
        session: AsyncSession,
        tenant_id: str,
        email: str,
        password: str,
    ) -> Tuple[bool, Optional[User], Optional[str]]:
        """Authenticate a user."""
        try:
            # Query for user
            result = await session.execute(
                select(User).where(
                    User.tenant_id == tenant_id,
                    User.email == email,
                )
            )
            user = result.scalar_one_or_none()

            if not user:
                logger.warning(f"User not found: {email} in tenant {tenant_id}")
                return False, None, "Invalid credentials"

            if not user.is_active:
                logger.warning(f"User is inactive: {email}")
                return False, None, "User account is inactive"

            if not AuthService.verify_password(password, user.password_hash):
                logger.warning(f"Invalid password for user: {email}")
                return False, None, "Invalid credentials"

            return True, user, None
        except Exception as e:
            logger.error(f"Authentication error: {str(e)}")
            return False, None, str(e)

    @staticmethod
    async def register_user(
        session: AsyncSession,
        tenant_id: str,
        email: str,
        username: str,
        password: str,
    ) -> Tuple[bool, Optional[User], Optional[str]]:
        """Register a new user."""
        try:
            # Check if user already exists
            result = await session.execute(
                select(User).where(
                    User.tenant_id == tenant_id,
                    User.email == email,
                )
            )
            if result.scalar_one_or_none():
                return False, None, "User already exists"

            # Verify tenant exists
            tenant_result = await session.execute(
                select(Tenant).where(Tenant.id == tenant_id)
            )
            if not tenant_result.scalar_one_or_none():
                return False, None, "Tenant not found"

            # Create new user
            new_user = User(
                tenant_id=tenant_id,
                email=email,
                username=username,
                password_hash=AuthService.hash_password(password),
            )

            session.add(new_user)
            await session.commit()
            await session.refresh(new_user)

            logger.info(f"User registered: {email} in tenant {tenant_id}")
            return True, new_user, None
        except Exception as e:
            logger.error(f"Registration error: {str(e)}")
            await session.rollback()
            return False, None, str(e)


class TenantService:
    """Tenant management service."""

    @staticmethod
    async def create_tenant(
        session: AsyncSession,
        name: str,
        description: Optional[str] = None,
    ) -> Tuple[bool, Optional[Tenant], Optional[str]]:
        """Create a new tenant."""
        try:
            # Check if tenant already exists
            result = await session.execute(select(Tenant).where(Tenant.name == name))
            if result.scalar_one_or_none():
                return False, None, "Tenant already exists"

            # Create tenant
            new_tenant = Tenant(
                name=name,
                description=description,
            )

            session.add(new_tenant)
            await session.commit()
            await session.refresh(new_tenant)

            logger.info(f"Tenant created: {name}")
            return True, new_tenant, None
        except Exception as e:
            logger.error(f"Tenant creation error: {str(e)}")
            await session.rollback()
            return False, None, str(e)

    @staticmethod
    async def get_tenant(
        session: AsyncSession,
        tenant_id: str,
    ) -> Optional[Tenant]:
        """Get a tenant by ID."""
        try:
            result = await session.execute(select(Tenant).where(Tenant.id == tenant_id))
            return result.scalar_one_or_none()
        except Exception as e:
            logger.error(f"Error fetching tenant: {str(e)}")
            return None


class AuthorizationService:
    """Authorization service for tenant isolation."""

    @staticmethod
    def enforce_tenant_ownership(resource_tenant_id: str, user_tenant_id: str) -> bool:
        """Enforce that a resource belongs to the user's tenant."""
        return resource_tenant_id == user_tenant_id

    @staticmethod
    def get_user_tenant_id(token_data: TokenData) -> str:
        """Extract tenant ID from token."""
        return token_data.tenant_id
