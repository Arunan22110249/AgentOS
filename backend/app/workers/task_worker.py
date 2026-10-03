"""Task worker for executing workflow nodes."""
import asyncio
import json
from datetime import datetime
from typing import Any, Dict, Optional

from app.agents.executor import AgentExecutor, get_llm_provider
from app.config import settings
from app.database.models import (
    Checkpoint,
    ExecutionEvent,
    NodeExecution,
    NodeState,
    WorkflowNode,
    WorkflowRun,
    WorkflowState,
)
from app.database.session import async_session_factory
from app.events.kafka_events import (
    EventType,
    KafkaEvent,
    NodeResultEvent,
    NodeTaskEvent,
)
from app.events.kafka_service import (
    get_idempotency_store,
    get_kafka_consumer,
    get_kafka_producer,
)
from app.observability.logging import Metrics, get_logger
from app.tools.registry import get_tool_registry
from app.workflows.engine import CheckpointManager, ExecutionEngine
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

logger = get_logger(__name__)


class TaskWorker:
    """Worker that processes node execution tasks from Kafka."""

    def __init__(self, worker_id: str = None):
        self.worker_id = worker_id or settings.worker.worker_id
        self.kafka_consumer = get_kafka_consumer("node.tasks")
        self.kafka_producer = get_kafka_producer()
        self.idempotency_store = get_idempotency_store()
        self.tool_registry = get_tool_registry()
        self.running = False

    async def start(self):
        """Start the worker."""
        logger.info(f"Starting task worker: {self.worker_id}")
        self.running = True

        try:
            while self.running:
                # Process messages
                messages_processed = await self.kafka_consumer.consume_messages(
                    handler=self._handle_task,
                    timeout_ms=1000,
                )

                if messages_processed > 0:
                    logger.info(f"Processed {messages_processed} messages")

                await asyncio.sleep(0.1)
        except KeyboardInterrupt:
            logger.info("Worker interrupted")
        except Exception as e:
            logger.error(f"Worker error: {str(e)}")
        finally:
            self.running = False
            self.kafka_consumer.close()
            self.kafka_producer.close()

    def _handle_task(self, message_data: Dict[str, Any]) -> bool:
        """Handle a single task message."""
        try:
            # Parse task
            task = NodeTaskEvent.model_validate(message_data)

            # Idempotency check
            idempotency_key = f"{task.task_id}:{task.attempt}"
            if self.idempotency_store.is_processed(idempotency_key):
                logger.info(f"Task already processed, skipping: {task.task_id}")
                return True

            # Execute in event loop
            loop = asyncio.get_event_loop()
            success = loop.run_until_complete(self._execute_task(task))

            if success:
                self.idempotency_store.mark_processed(idempotency_key)

            return success
        except Exception as e:
            logger.error(f"Error handling task message: {str(e)}")
            return False

    async def _execute_task(self, task: NodeTaskEvent) -> bool:
        """Execute a single node task."""
        logger.info(
            f"Executing task: {task.task_id}",
            workflow_run_id=task.workflow_run_id,
            node_id=task.node_id,
            attempt=task.attempt,
        )

        start_time = datetime.utcnow()

        try:
            async with async_session_factory() as session:
                # Get workflow run and node execution
                run_result = await session.execute(
                    select(WorkflowRun).where(WorkflowRun.id == task.workflow_run_id)
                )
                workflow_run = run_result.scalar_one_or_none()

                if not workflow_run:
                    logger.error(f"Workflow run not found: {task.workflow_run_id}")
                    return False

                exec_result = await session.execute(
                    select(NodeExecution).where(
                        NodeExecution.id == task.node_execution_id
                    )
                )
                node_execution = exec_result.scalar_one_or_none()

                if not node_execution:
                    logger.error(f"Node execution not found: {task.node_execution_id}")
                    return False

                # Update state to RUNNING
                node_execution.state = NodeState.RUNNING
                node_execution.started_at = start_time
                await session.commit()

                # Execute based on node type
                output_data = None
                error_message = None
                success = True

                if task.node_type == "AGENT":
                    (
                        output_data,
                        error_message,
                        success,
                    ) = await self._execute_agent_node(task)
                elif task.node_type == "TOOL":
                    output_data, error_message, success = await self._execute_tool_node(
                        task
                    )
                else:
                    # Default task node - just echo the input
                    output_data = task.input_data

                # Update node execution
                end_time = datetime.utcnow()
                if success:
                    node_execution.state = NodeState.COMPLETED
                    node_execution.output_data = output_data
                else:
                    node_execution.state = NodeState.FAILED
                    node_execution.error_message = error_message

                node_execution.completed_at = end_time
                await session.commit()

                # Create execution event
                event = ExecutionEvent(
                    workflow_run_id=task.workflow_run_id,
                    node_execution_id=node_execution.id,
                    event_type="node.completed" if success else "node.failed",
                    correlation_id=task.correlation_id,
                    payload={
                        "node_id": task.node_id,
                        "status": "completed" if success else "failed",
                        "duration_seconds": (end_time - start_time).total_seconds(),
                        "output": output_data,
                        "error": error_message,
                    },
                )
                session.add(event)
                await session.commit()

                # Create checkpoint if successful
                if success:
                    await self._create_checkpoint(
                        session, workflow_run, node_execution.node_id
                    )

                # Publish result event
                result = NodeResultEvent(
                    task_id=task.task_id,
                    workflow_run_id=task.workflow_run_id,
                    node_execution_id=node_execution.id,
                    node_id=task.node_id,
                    status="COMPLETED" if success else "FAILED",
                    output_data=output_data,
                    error_message=error_message,
                    duration_seconds=(end_time - start_time).total_seconds(),
                    correlation_id=task.correlation_id,
                )

                await self.kafka_producer.send_event(
                    "node.results",
                    KafkaEvent.create(
                        EventType.NODE_COMPLETED if success else EventType.NODE_FAILED,
                        workflow_run.tenant_id,
                        workflow_run.id,
                        result.model_dump(),
                        node_execution.id,
                        task.correlation_id,
                    ),
                    key=task.task_id,
                )

                Metrics.increment(
                    "node_executions_completed" if success else "node_executions_failed"
                )
                logger.info(
                    f"Task completed: {task.task_id}",
                    status="success" if success else "failed",
                    duration_seconds=(end_time - start_time).total_seconds(),
                )

                return True
        except Exception as e:
            logger.error(f"Task execution error: {task.task_id}: {str(e)}")
            Metrics.increment("task_execution_errors")
            return False

    async def _execute_agent_node(
        self, task: NodeTaskEvent
    ) -> tuple[Optional[Dict], Optional[str], bool]:
        """Execute an agent node."""
        try:
            config = task.config
            agent_id = config.get("agent_id")
            user_input = config.get("input", "")

            async with async_session_factory() as session:
                from app.database.models import Agent

                agent_result = await session.execute(
                    select(Agent).where(Agent.id == agent_id)
                )
                agent = agent_result.scalar_one_or_none()

                if not agent:
                    return None, f"Agent not found: {agent_id}", False

                # Create agent executor
                llm_provider = get_llm_provider()
                executor = AgentExecutor(
                    agent_id=agent.id,
                    system_instructions=agent.system_instructions,
                    model=agent.model,
                    temperature=agent.temperature,
                    max_iterations=agent.max_iterations,
                    max_tokens=agent.max_tokens,
                    llm_provider=llm_provider,
                )

                # Execute agent
                result = await executor.execute(user_input)

                if result.get("status") == "error":
                    return None, result.get("error"), False

                return (
                    {
                        "response": result.get("response"),
                        "iterations": result.get("iterations"),
                        "tokens": result.get("tokens"),
                    },
                    None,
                    True,
                )
        except Exception as e:
            logger.error(f"Agent execution error: {str(e)}")
            return None, str(e), False

    async def _execute_tool_node(
        self, task: NodeTaskEvent
    ) -> tuple[Optional[Dict], Optional[str], bool]:
        """Execute a tool node."""
        try:
            config = task.config
            tool_name = config.get("tool_name")
            tool_input = config.get("input", {})

            result = await self.tool_registry.execute_tool(tool_name, tool_input)

            if "error" in result:
                return None, result.get("error"), False

            return result, None, True
        except Exception as e:
            logger.error(f"Tool execution error: {str(e)}")
            return None, str(e), False

    async def _create_checkpoint(
        self, session: AsyncSession, workflow_run: WorkflowRun, completed_node_id: str
    ):
        """Create a checkpoint after successful node completion."""
        try:
            # Get all completed nodes for this run
            exec_result = await session.execute(
                select(NodeExecution).where(
                    NodeExecution.workflow_run_id == workflow_run.id,
                    NodeExecution.state == NodeState.COMPLETED,
                )
            )
            completed_executions = exec_result.scalars().all()
            completed_node_ids = [e.node_id for e in completed_executions]

            # Get latest checkpoint
            cp_result = await session.execute(
                select(Checkpoint)
                .where(Checkpoint.workflow_run_id == workflow_run.id)
                .order_by(Checkpoint.checkpoint_number.desc())
            )
            latest_checkpoint = cp_result.scalar_one_or_none()

            checkpoint_number = (
                (latest_checkpoint.checkpoint_number + 1) if latest_checkpoint else 1
            )

            # Create new checkpoint
            checkpoint = Checkpoint(
                workflow_run_id=workflow_run.id,
                checkpoint_number=checkpoint_number,
                completed_node_ids=completed_node_ids,
                state_snapshot={
                    "workflow_run_id": workflow_run.id,
                    "state": workflow_run.state.value,
                    "completed_nodes": completed_node_ids,
                },
            )

            session.add(checkpoint)
            await session.commit()

            logger.info(
                f"Checkpoint created: {checkpoint.id}",
                checkpoint_number=checkpoint_number,
                completed_nodes=len(completed_node_ids),
            )
        except Exception as e:
            logger.error(f"Checkpoint creation error: {str(e)}")


async def main():
    """Main entry point."""
    worker = TaskWorker()
    await worker.start()


if __name__ == "__main__":
    asyncio.run(main())
