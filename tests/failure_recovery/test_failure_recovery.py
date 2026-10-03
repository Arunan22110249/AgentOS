"""Tests for failure recovery and checkpointing."""
from datetime import datetime

import pytest
from app.auth.service import TenantService
from app.database.models import (
    Base,
    Checkpoint,
    NodeExecution,
    NodeState,
    Tenant,
    Workflow,
    WorkflowNode,
    WorkflowRun,
    WorkflowState,
)
from app.workflows.engine import CheckpointManager, ExecutionEngine
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

TEST_DB_URL = "sqlite+aiosqlite:///:memory:"


@pytest.fixture
async def test_session():
    """Create test database session."""
    engine = create_async_engine(TEST_DB_URL, echo=False)

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    async_session_factory = async_sessionmaker(
        engine,
        class_=AsyncSession,
        expire_on_commit=False,
    )

    async with async_session_factory() as session:
        yield session

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)

    await engine.dispose()


@pytest.mark.asyncio
async def test_checkpoint_creation(test_session):
    """Test checkpoint creation."""
    # Setup
    success, tenant, _ = await TenantService.create_tenant(test_session, "test-tenant")
    assert success

    workflow = Workflow(
        tenant_id=tenant.id,
        name="test-workflow",
        definition={"nodes": {"task_1": {"type": "TASK"}}},
    )
    test_session.add(workflow)
    await test_session.commit()

    run = WorkflowRun(
        tenant_id=tenant.id,
        workflow_id=workflow.id,
        state=WorkflowState.RUNNING,
    )
    test_session.add(run)
    await test_session.commit()
    await test_session.refresh(run)

    # Create checkpoint
    checkpoint_data = CheckpointManager.create_checkpoint_data(
        workflow_run_id=run.id,
        completed_node_ids=["task_1"],
        workflow_state={},
    )

    assert checkpoint_data["workflow_run_id"] == run.id
    assert checkpoint_data["completed_node_ids"] == ["task_1"]


@pytest.mark.asyncio
async def test_checkpoint_restoration(test_session):
    """Test restoration from checkpoint."""
    checkpoint_data = {
        "workflow_run_id": "run-1",
        "completed_node_ids": ["task_1", "task_2"],
        "state_snapshot": {"key": "value"},
    }

    completed, snapshot = CheckpointManager.restore_from_checkpoint(checkpoint_data)

    assert completed == ["task_1", "task_2"]
    assert snapshot["key"] == "value"


@pytest.mark.asyncio
async def test_recovery_resume_from_checkpoint(test_session):
    """Test that execution resumes from checkpoint."""
    # Setup workflow with 3 tasks
    success, tenant, _ = await TenantService.create_tenant(test_session, "test-tenant")
    assert success

    workflow = Workflow(
        tenant_id=tenant.id,
        name="test-workflow",
        definition={
            "nodes": {
                "task_1": {"type": "TASK", "dependencies": []},
                "task_2": {"type": "TASK", "dependencies": ["task_1"]},
                "task_3": {"type": "TASK", "dependencies": ["task_2"]},
            }
        },
    )
    test_session.add(workflow)
    await test_session.commit()
    await test_session.refresh(workflow)

    # Create run
    run = WorkflowRun(
        tenant_id=tenant.id,
        workflow_id=workflow.id,
        state=WorkflowState.RUNNING,
    )
    test_session.add(run)
    await test_session.commit()
    await test_session.refresh(run)

    # Simulate completion of task_1
    checkpoint = Checkpoint(
        workflow_run_id=run.id,
        checkpoint_number=1,
        completed_node_ids=["task_1"],
        state_snapshot={},
    )
    test_session.add(checkpoint)
    await test_session.commit()

    # Check that task_2 is now ready (task_1 is in checkpoint)
    ready = ExecutionEngine.get_ready_nodes(
        workflow.definition["nodes"],
        {"task_1"},  # Completed nodes
        set(),  # Failed nodes
    )

    assert "task_2" in ready
    assert "task_1" not in ready


@pytest.mark.asyncio
async def test_failed_node_not_retried(test_session):
    """Test that permanently failed nodes are not retried."""
    nodes = {
        "task_1": {"type": "TASK", "dependencies": []},
        "task_2": {"type": "TASK", "dependencies": ["task_1"]},
    }

    # task_1 failed with permission error (non-retryable)
    ready = ExecutionEngine.get_ready_nodes(nodes, set(), {"task_1"})

    # task_2 should not be ready because its dependency failed
    assert "task_2" not in ready
    assert "task_1" not in ready


@pytest.mark.asyncio
async def test_retry_with_backoff(test_session):
    """Test retry delay calculation."""
    # First attempt: 1 second
    delay = ExecutionEngine.calculate_retry_delay(1, 5, 2.0, 1)
    assert delay == 1

    # Second attempt: 2 seconds
    delay = ExecutionEngine.calculate_retry_delay(2, 5, 2.0, 1)
    assert delay == 2

    # Third attempt: 4 seconds
    delay = ExecutionEngine.calculate_retry_delay(3, 5, 2.0, 1)
    assert delay == 4

    # Fifth attempt: max retries reached
    delay = ExecutionEngine.calculate_retry_delay(5, 5, 2.0, 1)
    assert delay is None
