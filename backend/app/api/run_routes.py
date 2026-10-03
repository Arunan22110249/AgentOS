"""Workflow run management API routes."""
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from typing import Optional, List
from app.database.session import get_session
from app.database.models import WorkflowRun, Workflow, WorkflowState, ExecutionEvent
from app.observability.logging import get_logger
from app.api.auth_routes import verify_token


logger = get_logger(__name__)
router = APIRouter()


class WorkflowRunCreate(BaseModel):
    """Create workflow run request."""
    workflow_id: str
    input_data: dict = {}


class WorkflowRunResponse(BaseModel):
    """Workflow run response."""
    id: str
    workflow_id: str
    state: str
    input_data: dict
    output_data: Optional[dict]
    error_message: Optional[str]
    started_at: Optional[str]
    completed_at: Optional[str]
    created_at: str


class WorkflowRunEvent(BaseModel):
    """Workflow run event."""
    event_id: str
    event_type: str
    created_at: str
    payload: dict


@router.post("/{workflow_id}/runs", response_model=WorkflowRunResponse, status_code=201)
async def create_workflow_run(
    workflow_id: str,
    req: WorkflowRunCreate,
    authorization: str = None,
    session: AsyncSession = Depends(get_session),
):
    """Create a new workflow run."""
    token_data = verify_token(authorization)
    
    # Verify workflow exists and belongs to tenant
    result = await session.execute(
        select(Workflow).where(
            Workflow.id == workflow_id,
            Workflow.tenant_id == token_data.tenant_id,
        )
    )
    workflow = result.scalar_one_or_none()
    
    if not workflow:
        raise HTTPException(status_code=404, detail="Workflow not found")
    
    # Create run
    run = WorkflowRun(
        tenant_id=token_data.tenant_id,
        workflow_id=workflow_id,
        input_data=req.input_data,
        state=WorkflowState.PENDING,
    )
    
    session.add(run)
    await session.commit()
    await session.refresh(run)
    
    logger.info(f"Workflow run created: {run.id}")
    
    return WorkflowRunResponse(
        id=run.id,
        workflow_id=run.workflow_id,
        state=run.state.value,
        input_data=run.input_data,
        output_data=run.output_data,
        error_message=run.error_message,
        started_at=run.started_at.isoformat() if run.started_at else None,
        completed_at=run.completed_at.isoformat() if run.completed_at else None,
        created_at=run.created_at.isoformat(),
    )


@router.get("/{run_id}", response_model=WorkflowRunResponse)
async def get_workflow_run(
    run_id: str,
    authorization: str = None,
    session: AsyncSession = Depends(get_session),
):
    """Get a workflow run by ID."""
    token_data = verify_token(authorization)
    
    result = await session.execute(
        select(WorkflowRun).where(
            WorkflowRun.id == run_id,
            WorkflowRun.tenant_id == token_data.tenant_id,
        )
    )
    run = result.scalar_one_or_none()
    
    if not run:
        raise HTTPException(status_code=404, detail="Workflow run not found")
    
    return WorkflowRunResponse(
        id=run.id,
        workflow_id=run.workflow_id,
        state=run.state.value,
        input_data=run.input_data,
        output_data=run.output_data,
        error_message=run.error_message,
        started_at=run.started_at.isoformat() if run.started_at else None,
        completed_at=run.completed_at.isoformat() if run.completed_at else None,
        created_at=run.created_at.isoformat(),
    )


@router.post("/{run_id}/cancel", status_code=200)
async def cancel_workflow_run(
    run_id: str,
    authorization: str = None,
    session: AsyncSession = Depends(get_session),
):
    """Cancel a workflow run."""
    token_data = verify_token(authorization)
    
    result = await session.execute(
        select(WorkflowRun).where(
            WorkflowRun.id == run_id,
            WorkflowRun.tenant_id == token_data.tenant_id,
        )
    )
    run = result.scalar_one_or_none()
    
    if not run:
        raise HTTPException(status_code=404, detail="Workflow run not found")
    
    run.state = WorkflowState.CANCELLED
    await session.commit()
    
    logger.info(f"Workflow run cancelled: {run_id}")
    
    return {"status": "cancelled"}


@router.get("/{run_id}/events", response_model=List[WorkflowRunEvent])
async def get_workflow_run_events(
    run_id: str,
    skip: int = Query(0),
    limit: int = Query(100),
    authorization: str = None,
    session: AsyncSession = Depends(get_session),
):
    """Get events for a workflow run."""
    token_data = verify_token(authorization)
    
    # Verify run exists and belongs to tenant
    result = await session.execute(
        select(WorkflowRun).where(
            WorkflowRun.id == run_id,
            WorkflowRun.tenant_id == token_data.tenant_id,
        )
    )
    if not result.scalar_one_or_none():
        raise HTTPException(status_code=404, detail="Workflow run not found")
    
    # Get events
    events_result = await session.execute(
        select(ExecutionEvent)
        .where(ExecutionEvent.workflow_run_id == run_id)
        .order_by(ExecutionEvent.created_at.desc())
        .offset(skip)
        .limit(limit)
    )
    events = events_result.scalars().all()
    
    return [
        WorkflowRunEvent(
            event_id=e.id,
            event_type=e.event_type,
            created_at=e.created_at.isoformat(),
            payload=e.payload,
        )
        for e in events
    ]
