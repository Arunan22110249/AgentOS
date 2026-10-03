"""Tool management API routes."""
from typing import Any, Dict, List, Optional

from app.api.auth_routes import verify_token
from app.database.models import Tenant, Tool
from app.database.session import get_session
from app.observability.logging import get_logger
from app.tools.registry import get_tool_registry
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

logger = get_logger(__name__)
router = APIRouter()


class ToolCreate(BaseModel):
    """Create tool request."""

    name: str
    description: str
    input_schema: Dict[str, Any]
    output_schema: Dict[str, Any] = {}
    execution_timeout_seconds: int = 30


class ToolResponse(BaseModel):
    """Tool response."""

    id: str
    name: str
    description: str
    input_schema: Dict[str, Any]
    output_schema: Dict[str, Any]
    execution_timeout_seconds: int
    is_active: bool
    created_at: str


@router.post("", response_model=ToolResponse, status_code=201)
async def create_tool(
    req: ToolCreate,
    authorization: str = None,
    session: AsyncSession = Depends(get_session),
):
    """Create a new tool."""
    token_data = verify_token(authorization)

    # Verify tenant exists
    tenant_result = await session.execute(
        select(Tenant).where(Tenant.id == token_data.tenant_id)
    )
    if not tenant_result.scalar_one_or_none():
        raise HTTPException(status_code=403, detail="Tenant not found")

    # Create tool
    tool = Tool(
        tenant_id=token_data.tenant_id,
        name=req.name,
        description=req.description,
        input_schema=req.input_schema,
        output_schema=req.output_schema,
        execution_timeout_seconds=req.execution_timeout_seconds,
    )

    session.add(tool)
    await session.commit()
    await session.refresh(tool)

    logger.info(f"Tool created: {tool.id}")

    return ToolResponse(
        id=tool.id,
        name=tool.name,
        description=tool.description,
        input_schema=tool.input_schema,
        output_schema=tool.output_schema,
        execution_timeout_seconds=tool.execution_timeout_seconds,
        is_active=tool.is_active,
        created_at=tool.created_at.isoformat(),
    )


@router.get("", response_model=List[ToolResponse])
async def list_tools(
    skip: int = Query(0),
    limit: int = Query(10),
    authorization: str = None,
    session: AsyncSession = Depends(get_session),
):
    """List tools."""
    token_data = verify_token(authorization)

    # Get tenant tools
    result = await session.execute(
        select(Tool)
        .where(Tool.tenant_id == token_data.tenant_id)
        .offset(skip)
        .limit(limit)
    )
    tools = result.scalars().all()

    # Add built-in tools
    registry = get_tool_registry()
    builtin_tools = registry.list_tools()

    tool_responses = [
        ToolResponse(
            id=t.id,
            name=t.name,
            description=t.description,
            input_schema=t.input_schema,
            output_schema=t.output_schema,
            execution_timeout_seconds=t.execution_timeout_seconds,
            is_active=t.is_active,
            created_at=t.created_at.isoformat(),
        )
        for t in tools
    ]

    # Add built-in tools with synthetic IDs
    for bt in builtin_tools:
        tool_responses.append(
            ToolResponse(
                id=f"builtin_{bt['name']}",
                name=bt["name"],
                description=bt["description"],
                input_schema=bt["input_schema"],
                output_schema=bt.get("output_schema", {}),
                execution_timeout_seconds=bt["timeout_seconds"],
                is_active=True,
                created_at="2024-01-01T00:00:00",
            )
        )

    return tool_responses
