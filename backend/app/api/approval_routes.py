"""Approval request management API routes."""
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from typing import Optional, List
from datetime import datetime
from app.database.session import get_session
from app.database.models import ApprovalRequest, WorkflowRun, ApprovalStatus
from app.observability.logging import get_logger
from app.api.auth_routes import verify_token


logger = get_logger(__name__)
router = APIRouter()


class ApprovalRequestResponse(BaseModel):
    """Approval request response."""
    id: str
    workflow_run_id: str
    title: str
    description: Optional[str]
    status: str
    context_data: dict
    created_at: str


class ApprovalAction(BaseModel):
    """Approval action request."""
    status: str  # "APPROVED" or "REJECTED"
    reason: Optional[str] = None


@router.get("", response_model=List[ApprovalRequestResponse])
async def list_approvals(
    status: Optional[str] = None,
    skip: int = Query(0),
    limit: int = Query(10),
    authorization: str = None,
    session: AsyncSession = Depends(get_session),
):
    """List pending approvals for tenant."""
    token_data = verify_token(authorization)
    
    query = select(ApprovalRequest).join(WorkflowRun).where(
        WorkflowRun.tenant_id == token_data.tenant_id
    )
    
    if status:
        query = query.where(ApprovalRequest.status == status)
    
    result = await session.execute(
        query.offset(skip).limit(limit)
    )
    approvals = result.scalars().all()
    
    return [
        ApprovalRequestResponse(
            id=a.id,
            workflow_run_id=a.workflow_run_id,
            title=a.title,
            description=a.description,
            status=a.status.value,
            context_data=a.context_data,
            created_at=a.created_at.isoformat(),
        )
        for a in approvals
    ]


@router.post("/{approval_id}/approve", status_code=200)
async def approve_request(
    approval_id: str,
    authorization: str = None,
    session: AsyncSession = Depends(get_session),
):
    """Approve a request."""
    token_data = verify_token(authorization)
    
    # Get approval and verify tenant ownership
    result = await session.execute(
        select(ApprovalRequest).join(WorkflowRun).where(
            ApprovalRequest.id == approval_id,
            WorkflowRun.tenant_id == token_data.tenant_id,
        )
    )
    approval = result.scalar_one_or_none()
    
    if not approval:
        raise HTTPException(status_code=404, detail="Approval request not found")
    
    # Update approval
    approval.status = ApprovalStatus.APPROVED
    approval.approved_by = token_data.user_id
    approval.approved_at = datetime.utcnow()
    
    await session.commit()
    
    logger.info(f"Approval approved: {approval_id}")
    
    return {"status": "approved"}


@router.post("/{approval_id}/reject", status_code=200)
async def reject_request(
    approval_id: str,
    action: ApprovalAction,
    authorization: str = None,
    session: AsyncSession = Depends(get_session),
):
    """Reject a request."""
    token_data = verify_token(authorization)
    
    # Get approval and verify tenant ownership
    result = await session.execute(
        select(ApprovalRequest).join(WorkflowRun).where(
            ApprovalRequest.id == approval_id,
            WorkflowRun.tenant_id == token_data.tenant_id,
        )
    )
    approval = result.scalar_one_or_none()
    
    if not approval:
        raise HTTPException(status_code=404, detail="Approval request not found")
    
    # Update approval
    approval.status = ApprovalStatus.REJECTED
    approval.approved_by = token_data.user_id
    approval.approved_at = datetime.utcnow()
    approval.rejection_reason = action.reason
    
    await session.commit()
    
    logger.info(f"Approval rejected: {approval_id}")
    
    return {"status": "rejected"}
