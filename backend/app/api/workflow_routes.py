"""Workflow management API routes."""
from typing import List, Optional

from app.api.auth_routes import verify_token
from app.database.models import Tenant, Workflow
from app.database.session import get_session
from app.observability.logging import get_logger
from app.workflows.engine import DAGValidator
from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

logger = get_logger(__name__)
router = APIRouter()


class WorkflowCreate(BaseModel):
    """Create workflow request."""

    name: str
    description: Optional[str] = None
    definition: dict
    timeout_seconds: int = 3600


class WorkflowResponse(BaseModel):
    """Workflow response."""

    id: str
    name: str
    description: Optional[str]
    timeout_seconds: int
    is_active: bool
    created_at: str


@router.post("", response_model=WorkflowResponse, status_code=201)
async def create_workflow(
    req: WorkflowCreate,
    authorization: str = None,
    session: AsyncSession = Depends(get_session),
):
    """Create a new workflow."""
    token_data = verify_token(authorization)

    # Validate workflow definition
    valid, error = DAGValidator.validate_workflow(req.definition)
    if not valid:
        raise HTTPException(status_code=400, detail=f"Invalid workflow: {error}")

    # Check tenant exists
    tenant_result = await session.execute(
        select(Tenant).where(Tenant.id == token_data.tenant_id)
    )
    if not tenant_result.scalar_one_or_none():
        raise HTTPException(status_code=403, detail="Tenant not found")

    # Create workflow
    workflow = Workflow(
        tenant_id=token_data.tenant_id,
        name=req.name,
        description=req.description,
        definition=req.definition,
        timeout_seconds=req.timeout_seconds,
    )

    session.add(workflow)
    await session.commit()
    await session.refresh(workflow)

    logger.info(f"Workflow created: {workflow.id}")

    return WorkflowResponse(
        id=workflow.id,
        name=workflow.name,
        description=workflow.description,
        timeout_seconds=workflow.timeout_seconds,
        is_active=workflow.is_active,
        created_at=workflow.created_at.isoformat(),
    )


@router.get("", response_model=List[WorkflowResponse])
async def list_workflows(
    skip: int = Query(0),
    limit: int = Query(10),
    authorization: str = None,
    session: AsyncSession = Depends(get_session),
):
    """List workflows for tenant."""
    token_data = verify_token(authorization)

    result = await session.execute(
        select(Workflow)
        .where(Workflow.tenant_id == token_data.tenant_id)
        .offset(skip)
        .limit(limit)
    )
    workflows = result.scalars().all()

    return [
        WorkflowResponse(
            id=w.id,
            name=w.name,
            description=w.description,
            timeout_seconds=w.timeout_seconds,
            is_active=w.is_active,
            created_at=w.created_at.isoformat(),
        )
        for w in workflows
    ]


@router.get("/{workflow_id}", response_model=WorkflowResponse)
async def get_workflow(
    workflow_id: str,
    authorization: str = None,
    session: AsyncSession = Depends(get_session),
):
    """Get a workflow by ID."""
    token_data = verify_token(authorization)

    result = await session.execute(
        select(Workflow).where(
            Workflow.id == workflow_id,
            Workflow.tenant_id == token_data.tenant_id,
        )
    )
    workflow = result.scalar_one_or_none()

    if not workflow:
        raise HTTPException(status_code=404, detail="Workflow not found")

    return WorkflowResponse(
        id=workflow.id,
        name=workflow.name,
        description=workflow.description,
        timeout_seconds=workflow.timeout_seconds,
        is_active=workflow.is_active,
        created_at=workflow.created_at.isoformat(),
    )
