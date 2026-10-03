"""Integration tests for workflow execution."""
import asyncio

import pytest
from app.auth.service import AuthService, TenantService
from app.database.models import Base, Tenant, User, Workflow, WorkflowRun, WorkflowState
from app.observability.logging import get_logger
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

logger = get_logger(__name__)

# Use in-memory SQLite for testing
TEST_DB_URL = "sqlite+aiosqlite:///:memory:"


@pytest.fixture
async def test_session():
    """Create test database session."""
    engine = create_async_engine(TEST_DB_URL, echo=False)

    # Create tables
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    # Create session factory
    async_session_factory = async_sessionmaker(
        engine,
        class_=AsyncSession,
        expire_on_commit=False,
    )

    async with async_session_factory() as session:
        yield session

    # Cleanup
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)

    await engine.dispose()


@pytest.mark.asyncio
async def test_tenant_creation(test_session):
    """Test tenant creation."""
    success, tenant, error = await TenantService.create_tenant(
        test_session,
        name="test-tenant",
        description="Test tenant",
    )

    assert success
    assert tenant.name == "test-tenant"
    assert tenant.is_active


@pytest.mark.asyncio
async def test_user_registration(test_session):
    """Test user registration."""
    # Create tenant first
    success, tenant, error = await TenantService.create_tenant(
        test_session,
        name="test-tenant",
    )
    assert success

    # Register user
    success, user, error = await AuthService.register_user(
        test_session,
        tenant_id=tenant.id,
        email="user@example.com",
        username="testuser",
        password="securepass123",
    )

    assert success
    assert user.email == "user@example.com"
    assert user.username == "testuser"


@pytest.mark.asyncio
async def test_user_authentication(test_session):
    """Test user authentication."""
    # Setup
    success, tenant, _ = await TenantService.create_tenant(test_session, "test-tenant")
    assert success

    success, user, _ = await AuthService.register_user(
        test_session,
        tenant_id=tenant.id,
        email="user@example.com",
        username="testuser",
        password="securepass123",
    )
    assert success

    # Test authentication
    success, auth_user, error = await AuthService.authenticate_user(
        test_session,
        tenant_id=tenant.id,
        email="user@example.com",
        password="securepass123",
    )

    assert success
    assert auth_user.id == user.id

    # Test wrong password
    success, auth_user, error = await AuthService.authenticate_user(
        test_session,
        tenant_id=tenant.id,
        email="user@example.com",
        password="wrongpass",
    )

    assert not success


@pytest.mark.asyncio
async def test_workflow_creation(test_session):
    """Test workflow creation."""
    # Setup tenant
    success, tenant, _ = await TenantService.create_tenant(test_session, "test-tenant")
    assert success

    # Create workflow
    workflow = Workflow(
        tenant_id=tenant.id,
        name="test-workflow",
        description="Test workflow",
        definition={
            "nodes": {
                "task_1": {"type": "TASK", "dependencies": []},
                "task_2": {"type": "TASK", "dependencies": ["task_1"]},
            }
        },
    )

    test_session.add(workflow)
    await test_session.commit()
    await test_session.refresh(workflow)

    assert workflow.id is not None
    assert workflow.name == "test-workflow"


@pytest.mark.asyncio
async def test_workflow_run_creation(test_session):
    """Test workflow run creation."""
    # Setup
    success, tenant, _ = await TenantService.create_tenant(test_session, "test-tenant")
    assert success

    workflow = Workflow(
        tenant_id=tenant.id,
        name="test-workflow",
        definition={
            "nodes": {
                "task_1": {"type": "TASK", "dependencies": []},
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
        state=WorkflowState.PENDING,
        input_data={"key": "value"},
    )

    test_session.add(run)
    await test_session.commit()
    await test_session.refresh(run)

    assert run.id is not None
    assert run.state == WorkflowState.PENDING
    assert run.input_data == {"key": "value"}
