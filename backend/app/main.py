"""Main FastAPI application."""
from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import logging
from app.config import settings
from app.database.session import create_tables, drop_tables
from app.observability.logging import get_logger
from app.api import auth_routes, workflow_routes, run_routes, agent_routes, tool_routes, approval_routes, health_routes


logger = get_logger(__name__)

# Create FastAPI app
app = FastAPI(
    title=settings.api_title,
    version=settings.api_version,
    description="Distributed Agent Execution Platform",
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routes
app.include_router(auth_routes.router, prefix="/auth", tags=["Authentication"])
app.include_router(workflow_routes.router, prefix="/workflows", tags=["Workflows"])
app.include_router(run_routes.router, prefix="/runs", tags=["Workflow Runs"])
app.include_router(agent_routes.router, prefix="/agents", tags=["Agents"])
app.include_router(tool_routes.router, prefix="/tools", tags=["Tools"])
app.include_router(approval_routes.router, prefix="/approvals", tags=["Approvals"])
app.include_router(health_routes.router, tags=["Health"])


@app.on_event("startup")
async def startup_event():
    """Application startup."""
    logger.info("Starting AgentOS API")
    
    # Create tables
    await create_tables()
    logger.info("Database tables created")


@app.on_event("shutdown")
async def shutdown_event():
    """Application shutdown."""
    logger.info("Shutting down AgentOS API")


@app.get("/")
async def root():
    """Root endpoint."""
    return {
        "name": "AgentOS",
        "version": settings.api_version,
        "description": "Distributed Agent Execution Platform",
    }


if __name__ == "__main__":
    import uvicorn
    
    uvicorn.run(
        app,
        host=settings.api_host,
        port=settings.api_port,
    )
