"""Database models for AgentOS."""
from datetime import datetime
from enum import Enum
from typing import Any, Optional
from sqlalchemy import (
    String, Integer, Float, Boolean, DateTime, Text, JSON,
    ForeignKey, UniqueConstraint, Index, Enum as SQLEnum,
    func
)
from sqlalchemy.orm import declarative_base, relationship, Mapped, mapped_column
import uuid

Base = declarative_base()


class WorkflowState(str, Enum):
    """Workflow execution states."""
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    PAUSED = "PAUSED"
    WAITING_APPROVAL = "WAITING_APPROVAL"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


class NodeState(str, Enum):
    """Node execution states."""
    PENDING = "PENDING"
    READY = "READY"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    RETRYING = "RETRYING"
    SKIPPED = "SKIPPED"
    CANCELLED = "CANCELLED"


class NodeType(str, Enum):
    """Node types."""
    TASK = "TASK"
    AGENT = "AGENT"
    TOOL = "TOOL"
    APPROVAL = "APPROVAL"
    CONDITION = "CONDITION"


class ApprovalStatus(str, Enum):
    """Approval statuses."""
    PENDING = "PENDING"
    APPROVED = "APPROVED"
    REJECTED = "REJECTED"


class Tenant(Base):
    """Tenant model for multi-tenancy."""
    __tablename__ = "tenants"
    
    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    name: Mapped[str] = mapped_column(String(255), nullable=False, unique=True)
    description: Mapped[Optional[str]] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=func.now(), onupdate=func.now())
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    
    # Relationships
    users: Mapped[list["User"]] = relationship("User", back_populates="tenant", cascade="all, delete-orphan")
    agents: Mapped[list["Agent"]] = relationship("Agent", back_populates="tenant", cascade="all, delete-orphan")
    tools: Mapped[list["Tool"]] = relationship("Tool", back_populates="tenant", cascade="all, delete-orphan")
    workflows: Mapped[list["Workflow"]] = relationship("Workflow", back_populates="tenant", cascade="all, delete-orphan")
    workflow_runs: Mapped[list["WorkflowRun"]] = relationship("WorkflowRun", back_populates="tenant", cascade="all, delete-orphan")
    memory: Mapped[list["Memory"]] = relationship("Memory", back_populates="tenant", cascade="all, delete-orphan")
    audit_logs: Mapped[list["AuditLog"]] = relationship("AuditLog", back_populates="tenant", cascade="all, delete-orphan")


class User(Base):
    """User model."""
    __tablename__ = "users"
    __table_args__ = (UniqueConstraint("tenant_id", "email", name="uq_tenant_email"),)
    
    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    tenant_id: Mapped[str] = mapped_column(String(36), ForeignKey("tenants.id"), nullable=False)
    email: Mapped[str] = mapped_column(String(255), nullable=False)
    username: Mapped[str] = mapped_column(String(255), nullable=False)
    password_hash: Mapped[str] = mapped_column(String(255), nullable=False)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=func.now(), onupdate=func.now())
    
    # Relationships
    tenant: Mapped[Tenant] = relationship("Tenant", back_populates="users")
    audit_logs: Mapped[list["AuditLog"]] = relationship("AuditLog", back_populates="user")


class Agent(Base):
    """Agent model."""
    __tablename__ = "agents"
    __table_args__ = (UniqueConstraint("tenant_id", "name", name="uq_tenant_agent_name"),)
    
    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    tenant_id: Mapped[str] = mapped_column(String(36), ForeignKey("tenants.id"), nullable=False)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    description: Mapped[Optional[str]] = mapped_column(Text)
    system_instructions: Mapped[str] = mapped_column(Text, nullable=False)
    model: Mapped[str] = mapped_column(String(255), default="gpt-4")
    temperature: Mapped[float] = mapped_column(Float, default=0.7)
    max_iterations: Mapped[int] = mapped_column(Integer, default=10)
    max_tokens: Mapped[Optional[int]] = mapped_column(Integer)
    execution_timeout_seconds: Mapped[int] = mapped_column(Integer, default=300)
    agent_metadata: Mapped[dict] = mapped_column(JSON, default=dict)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=func.now(), onupdate=func.now())
    
    # Relationships
    tenant: Mapped[Tenant] = relationship("Tenant", back_populates="agents")
    tools: Mapped[list["AgentTool"]] = relationship("AgentTool", back_populates="agent", cascade="all, delete-orphan")


class Tool(Base):
    """Tool model."""
    __tablename__ = "tools"
    __table_args__ = (UniqueConstraint("tenant_id", "name", name="uq_tenant_tool_name"),)
    
    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    tenant_id: Mapped[str] = mapped_column(String(36), ForeignKey("tenants.id"), nullable=False)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    description: Mapped[str] = mapped_column(Text, nullable=False)
    input_schema: Mapped[dict] = mapped_column(JSON, nullable=False)
    output_schema: Mapped[dict] = mapped_column(JSON, nullable=False)
    execution_timeout_seconds: Mapped[int] = mapped_column(Integer, default=30)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=func.now(), onupdate=func.now())
    
    # Relationships
    tenant: Mapped[Tenant] = relationship("Tenant", back_populates="tools")
    agent_tools: Mapped[list["AgentTool"]] = relationship("AgentTool", back_populates="tool", cascade="all, delete-orphan")


class AgentTool(Base):
    """Agent-Tool relationship model."""
    __tablename__ = "agent_tools"
    __table_args__ = (UniqueConstraint("agent_id", "tool_id", name="uq_agent_tool"),)
    
    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    agent_id: Mapped[str] = mapped_column(String(36), ForeignKey("agents.id"), nullable=False)
    tool_id: Mapped[str] = mapped_column(String(36), ForeignKey("tools.id"), nullable=False)
    is_enabled: Mapped[bool] = mapped_column(Boolean, default=True)
    
    # Relationships
    agent: Mapped[Agent] = relationship("Agent", back_populates="tools")
    tool: Mapped[Tool] = relationship("Tool", back_populates="agent_tools")


class Workflow(Base):
    """Workflow model."""
    __tablename__ = "workflows"
    __table_args__ = (UniqueConstraint("tenant_id", "name", name="uq_tenant_workflow_name"),)
    
    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    tenant_id: Mapped[str] = mapped_column(String(36), ForeignKey("tenants.id"), nullable=False)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    description: Mapped[Optional[str]] = mapped_column(Text)
    definition: Mapped[dict] = mapped_column(JSON, nullable=False)
    max_concurrent_runs: Mapped[int] = mapped_column(Integer, default=5)
    timeout_seconds: Mapped[int] = mapped_column(Integer, default=3600)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=func.now(), onupdate=func.now())
    
    # Relationships
    tenant: Mapped[Tenant] = relationship("Tenant", back_populates="workflows")
    nodes: Mapped[list["WorkflowNode"]] = relationship("WorkflowNode", back_populates="workflow", cascade="all, delete-orphan")
    runs: Mapped[list["WorkflowRun"]] = relationship("WorkflowRun", back_populates="workflow", cascade="all, delete-orphan")


class WorkflowNode(Base):
    """Workflow node model."""
    __tablename__ = "workflow_nodes"
    __table_args__ = (UniqueConstraint("workflow_id", "node_id", name="uq_workflow_node_id"),)
    
    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    workflow_id: Mapped[str] = mapped_column(String(36), ForeignKey("workflows.id"), nullable=False)
    node_id: Mapped[str] = mapped_column(String(255), nullable=False)
    node_type: Mapped[NodeType] = mapped_column(SQLEnum(NodeType), nullable=False)
    config: Mapped[dict] = mapped_column(JSON, nullable=False)
    dependencies: Mapped[list[str]] = mapped_column(JSON, default=list)
    retry_policy: Mapped[dict] = mapped_column(JSON, default=dict)
    timeout_seconds: Mapped[Optional[int]] = mapped_column(Integer)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=func.now())
    
    # Relationships
    workflow: Mapped[Workflow] = relationship("Workflow", back_populates="nodes")
    executions: Mapped[list["NodeExecution"]] = relationship("NodeExecution", back_populates="node", cascade="all, delete-orphan")


class WorkflowRun(Base):
    """Workflow run model."""
    __tablename__ = "workflow_runs"
    
    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    tenant_id: Mapped[str] = mapped_column(String(36), ForeignKey("tenants.id"), nullable=False)
    workflow_id: Mapped[str] = mapped_column(String(36), ForeignKey("workflows.id"), nullable=False)
    state: Mapped[WorkflowState] = mapped_column(SQLEnum(WorkflowState), default=WorkflowState.PENDING)
    input_data: Mapped[dict] = mapped_column(JSON, default=dict)
    output_data: Mapped[Optional[dict]] = mapped_column(JSON)
    error_message: Mapped[Optional[str]] = mapped_column(Text)
    started_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    completed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=func.now(), onupdate=func.now())
    
    # Relationships
    tenant: Mapped[Tenant] = relationship("Tenant", back_populates="workflow_runs")
    workflow: Mapped[Workflow] = relationship("Workflow", back_populates="runs")
    node_executions: Mapped[list["NodeExecution"]] = relationship("NodeExecution", back_populates="workflow_run", cascade="all, delete-orphan")
    checkpoints: Mapped[list["Checkpoint"]] = relationship("Checkpoint", back_populates="workflow_run", cascade="all, delete-orphan")
    events: Mapped[list["ExecutionEvent"]] = relationship("ExecutionEvent", back_populates="workflow_run", cascade="all, delete-orphan")
    approvals: Mapped[list["ApprovalRequest"]] = relationship("ApprovalRequest", back_populates="workflow_run", cascade="all, delete-orphan")


class NodeExecution(Base):
    """Node execution model."""
    __tablename__ = "node_executions"
    __table_args__ = (Index("ix_workflow_run_node", "workflow_run_id", "node_id"),)
    
    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    workflow_run_id: Mapped[str] = mapped_column(String(36), ForeignKey("workflow_runs.id"), nullable=False)
    workflow_node_id: Mapped[str] = mapped_column(String(36), ForeignKey("workflow_nodes.id"), nullable=False)
    node_id: Mapped[str] = mapped_column(String(255), nullable=False)
    state: Mapped[NodeState] = mapped_column(SQLEnum(NodeState), default=NodeState.PENDING)
    attempt: Mapped[int] = mapped_column(Integer, default=1)
    input_data: Mapped[dict] = mapped_column(JSON, default=dict)
    output_data: Mapped[Optional[dict]] = mapped_column(JSON)
    error_message: Mapped[Optional[str]] = mapped_column(Text)
    started_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    completed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=func.now(), onupdate=func.now())
    
    # Relationships
    workflow_run: Mapped[WorkflowRun] = relationship("WorkflowRun", back_populates="node_executions")
    node: Mapped[WorkflowNode] = relationship("WorkflowNode", back_populates="executions")


class Checkpoint(Base):
    """Checkpoint model for durable execution recovery."""
    __tablename__ = "checkpoints"
    __table_args__ = (Index("ix_workflow_run_checkpoint", "workflow_run_id", "checkpoint_number"),)
    
    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    workflow_run_id: Mapped[str] = mapped_column(String(36), ForeignKey("workflow_runs.id"), nullable=False)
    checkpoint_number: Mapped[int] = mapped_column(Integer, nullable=False)
    completed_node_ids: Mapped[list[str]] = mapped_column(JSON, nullable=False)
    state_snapshot: Mapped[dict] = mapped_column(JSON, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=func.now())
    
    # Relationships
    workflow_run: Mapped[WorkflowRun] = relationship("WorkflowRun", back_populates="checkpoints")


class ExecutionEvent(Base):
    """Execution event model for audit trail and observability."""
    __tablename__ = "execution_events"
    __table_args__ = (Index("ix_workflow_run_event", "workflow_run_id", "created_at"),)
    
    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    workflow_run_id: Mapped[str] = mapped_column(String(36), ForeignKey("workflow_runs.id"), nullable=False)
    node_execution_id: Mapped[Optional[str]] = mapped_column(String(36), ForeignKey("node_executions.id"))
    event_type: Mapped[str] = mapped_column(String(255), nullable=False)
    correlation_id: Mapped[Optional[str]] = mapped_column(String(255))
    payload: Mapped[dict] = mapped_column(JSON, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=func.now())
    
    # Relationships
    workflow_run: Mapped[WorkflowRun] = relationship("WorkflowRun", back_populates="events")


class Memory(Base):
    """Persistent memory model for agent state."""
    __tablename__ = "memory"
    __table_args__ = (UniqueConstraint("tenant_id", "key", name="uq_tenant_memory_key"),)
    
    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    tenant_id: Mapped[str] = mapped_column(String(36), ForeignKey("tenants.id"), nullable=False)
    key: Mapped[str] = mapped_column(String(255), nullable=False)
    value: Mapped[dict] = mapped_column(JSON, nullable=False)
    ttl_seconds: Mapped[Optional[int]] = mapped_column(Integer)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=func.now(), onupdate=func.now())
    expires_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    
    # Relationships
    tenant: Mapped[Tenant] = relationship("Tenant", back_populates="memory")


class ApprovalRequest(Base):
    """Approval request model for human-in-the-loop."""
    __tablename__ = "approval_requests"
    
    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    workflow_run_id: Mapped[str] = mapped_column(String(36), ForeignKey("workflow_runs.id"), nullable=False)
    node_execution_id: Mapped[str] = mapped_column(String(36), ForeignKey("node_executions.id"), nullable=False)
    status: Mapped[ApprovalStatus] = mapped_column(SQLEnum(ApprovalStatus), default=ApprovalStatus.PENDING)
    title: Mapped[str] = mapped_column(String(255), nullable=False)
    description: Mapped[Optional[str]] = mapped_column(Text)
    context_data: Mapped[dict] = mapped_column(JSON, default=dict)
    approved_by: Mapped[Optional[str]] = mapped_column(String(36))
    approved_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    rejection_reason: Mapped[Optional[str]] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=func.now(), onupdate=func.now())
    
    # Relationships
    workflow_run: Mapped[WorkflowRun] = relationship("WorkflowRun", back_populates="approvals")


class AuditLog(Base):
    """Audit log model."""
    __tablename__ = "audit_logs"
    __table_args__ = (Index("ix_tenant_created", "tenant_id", "created_at"),)
    
    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    tenant_id: Mapped[str] = mapped_column(String(36), ForeignKey("tenants.id"), nullable=False)
    user_id: Mapped[Optional[str]] = mapped_column(String(36), ForeignKey("users.id"))
    action: Mapped[str] = mapped_column(String(255), nullable=False)
    resource_type: Mapped[str] = mapped_column(String(255), nullable=False)
    resource_id: Mapped[Optional[str]] = mapped_column(String(255))
    details: Mapped[dict] = mapped_column(JSON, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=func.now())
    
    # Relationships
    tenant: Mapped[Tenant] = relationship("Tenant", back_populates="audit_logs")
    user: Mapped[Optional[User]] = relationship("User", back_populates="audit_logs")
