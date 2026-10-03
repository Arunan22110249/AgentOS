"""Health check API routes."""
from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from app.database.session import get_session
from app.observability.logging import Metrics
from app.config import settings


router = APIRouter()


@router.get("/health")
async def health_check():
    """Health check endpoint."""
    return {
        "status": "ok",
        "service": "agentos-api",
        "version": settings.api_version,
    }


@router.get("/ready")
async def readiness_check(session: AsyncSession = Depends(get_session)):
    """Readiness check - verifies database connection."""
    try:
        # Try a simple database query
        await session.execute(text("SELECT 1"))
        return {
            "ready": True,
            "database": "connected",
        }
    except Exception as e:
        return {
            "ready": False,
            "database": "disconnected",
            "error": str(e),
        }


@router.get("/metrics")
async def get_metrics():
    """Get application metrics."""
    return {
        "metrics": Metrics.get_all(),
    }
