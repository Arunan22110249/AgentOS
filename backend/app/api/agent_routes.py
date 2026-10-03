"""Agent management API routes."""
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from typing import Optional, List, Dict, Any
from app.database.session import get_session
from app.database.models import Agent, Tenant
from app.observability.logging import get_logger
from app.api.auth_routes import verify_token


logger = get_logger(__name__)
router = APIRouter()


class AgentCreate(BaseModel):
    """Create agent request."""
    name: str
    description: Optional[str] = None
    system_instructions: str
    model: str = "gpt-4"
    temperature: float = 0.7
    max_iterations: int = 10
    max_tokens: Optional[int] = None
    execution_timeout_seconds: int = 300
    metadata: Dict[str, Any] = {}


class AgentResponse(BaseModel):
    """Agent response."""
    id: str
    name: str
    description: Optional[str]
    model: str
    temperature: float
    max_iterations: int
    is_active: bool
    created_at: str


@router.post("", response_model=AgentResponse, status_code=201)
async def create_agent(
    req: AgentCreate,
    authorization: str = None,
    session: AsyncSession = Depends(get_session),
):
    """Create a new agent."""
    token_data = verify_token(authorization)
    
    # Verify tenant exists
    tenant_result = await session.execute(
        select(Tenant).where(Tenant.id == token_data.tenant_id)
    )
    if not tenant_result.scalar_one_or_none():
        raise HTTPException(status_code=403, detail="Tenant not found")
    
    # Create agent
    agent = Agent(
        tenant_id=token_data.tenant_id,
        name=req.name,
        description=req.description,
        system_instructions=req.system_instructions,
        model=req.model,
        temperature=req.temperature,
        max_iterations=req.max_iterations,
        max_tokens=req.max_tokens,
        execution_timeout_seconds=req.execution_timeout_seconds,
        agent_metadata=req.agent_metadata,
    )
    
    session.add(agent)
    await session.commit()
    await session.refresh(agent)
    
    logger.info(f"Agent created: {agent.id}")
    
    return AgentResponse(
        id=agent.id,
        name=agent.name,
        description=agent.description,
        model=agent.model,
        temperature=agent.temperature,
        max_iterations=agent.max_iterations,
        is_active=agent.is_active,
        created_at=agent.created_at.isoformat(),
    )


@router.get("", response_model=List[AgentResponse])
async def list_agents(
    skip: int = Query(0),
    limit: int = Query(10),
    authorization: str = None,
    session: AsyncSession = Depends(get_session),
):
    """List agents for tenant."""
    token_data = verify_token(authorization)
    
    result = await session.execute(
        select(Agent)
        .where(Agent.tenant_id == token_data.tenant_id)
        .offset(skip)
        .limit(limit)
    )
    agents = result.scalars().all()
    
    return [
        AgentResponse(
            id=a.id,
            name=a.name,
            description=a.description,
            model=a.model,
            temperature=a.temperature,
            max_iterations=a.max_iterations,
            is_active=a.is_active,
            created_at=a.created_at.isoformat(),
        )
        for a in agents
    ]


@router.get("/{agent_id}", response_model=AgentResponse)
async def get_agent(
    agent_id: str,
    authorization: str = None,
    session: AsyncSession = Depends(get_session),
):
    """Get an agent by ID."""
    token_data = verify_token(authorization)
    
    result = await session.execute(
        select(Agent).where(
            Agent.id == agent_id,
            Agent.tenant_id == token_data.tenant_id,
        )
    )
    agent = result.scalar_one_or_none()
    
    if not agent:
        raise HTTPException(status_code=404, detail="Agent not found")
    
    return AgentResponse(
        id=agent.id,
        name=agent.name,
        description=agent.description,
        model=agent.model,
        temperature=agent.temperature,
        max_iterations=agent.max_iterations,
        is_active=agent.is_active,
        created_at=agent.created_at.isoformat(),
    )
