"""Workflow scheduler for discovering and dispatching tasks."""
import asyncio
from datetime import datetime
from typing import Dict, Any, List
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from app.config import settings
from app.database.session import async_session_factory
from app.database.models import (
    WorkflowRun, NodeExecution, WorkflowNode, Checkpoint,
    WorkflowState, NodeState, ExecutionEvent
)
from app.events.kafka_service import get_kafka_producer
from app.events.kafka_events import NodeTaskEvent, KafkaTopics
from app.observability.logging import get_logger, Metrics
from app.workflows.engine import ExecutionEngine, DAGValidator


logger = get_logger(__name__)


class WorkflowScheduler:
    """Scheduler that discovers workflow nodes and dispatches them as tasks."""
    
    def __init__(self):
        self.kafka_producer = get_kafka_producer()
        self.running = False
    
    async def start(self):
        """Start the scheduler."""
        logger.info("Starting workflow scheduler")
        self.running = True
        
        try:
            while self.running:
                await self._schedule_once()
                await asyncio.sleep(settings.scheduler.interval_seconds)
        except KeyboardInterrupt:
            logger.info("Scheduler interrupted")
        except Exception as e:
            logger.error(f"Scheduler error: {str(e)}")
        finally:
            self.running = False
            self.kafka_producer.close()
    
    async def _schedule_once(self):
        """Run one scheduling cycle."""
        try:
            async with async_session_factory() as session:
                # Get all RUNNING workflow runs
                run_result = await session.execute(
                    select(WorkflowRun).where(
                        WorkflowRun.state == WorkflowState.RUNNING
                    ).limit(settings.scheduler.batch_size)
                )
                workflow_runs = run_result.scalars().all()
                
                logger.info(f"Scheduling {len(workflow_runs)} workflow runs")
                
                for workflow_run in workflow_runs:
                    await self._schedule_workflow_run(session, workflow_run)
        except Exception as e:
            logger.error(f"Scheduling error: {str(e)}")
            Metrics.increment("scheduler_errors")
    
    async def _schedule_workflow_run(self, session: AsyncSession, workflow_run: WorkflowRun):
        """Schedule tasks for a specific workflow run."""
        try:
            # Get workflow definition
            workflow = workflow_run.workflow
            definition = workflow.definition
            
            # Get completed node IDs
            exec_result = await session.execute(
                select(NodeExecution).where(
                    NodeExecution.workflow_run_id == workflow_run.id,
                    NodeExecution.state == NodeState.COMPLETED,
                )
            )
            completed_nodes = {e.node_id for e in exec_result.scalars().all()}
            
            # Get failed node IDs (don't retry permanently failed nodes)
            fail_result = await session.execute(
                select(NodeExecution).where(
                    NodeExecution.workflow_run_id == workflow_run.id,
                    NodeExecution.state == NodeState.FAILED,
                )
            )
            failed_nodes = {e.node_id for e in fail_result.scalars().all()}
            
            # Find ready nodes
            nodes_def = definition.get("nodes", {})
            ready_nodes = ExecutionEngine.get_ready_nodes(
                nodes_def,
                completed_nodes,
                failed_nodes,
            )
            
            logger.info(
                f"Ready nodes for run {workflow_run.id}: {ready_nodes}",
                completed={len(completed_nodes)},
                failed={len(failed_nodes)},
            )
            
            # Dispatch tasks for ready nodes
            for node_id in ready_nodes:
                await self._dispatch_node_task(session, workflow_run, node_id)
            
            # Check if workflow is complete
            if len(completed_nodes) + len(failed_nodes) == len(nodes_def):
                # All nodes are done
                if len(failed_nodes) > 0:
                    workflow_run.state = WorkflowState.FAILED
                    logger.info(f"Workflow failed: {workflow_run.id}")
                else:
                    workflow_run.state = WorkflowState.COMPLETED
                    logger.info(f"Workflow completed: {workflow_run.id}")
                
                workflow_run.completed_at = datetime.utcnow()
                await session.commit()
                Metrics.increment("workflow_completed" if len(failed_nodes) == 0 else "workflow_failed")
        except Exception as e:
            logger.error(f"Error scheduling workflow run {workflow_run.id}: {str(e)}")
            Metrics.increment("scheduler_workflow_errors")
    
    async def _dispatch_node_task(self, session: AsyncSession, workflow_run: WorkflowRun, node_id: str):
        """Dispatch a single node as a task."""
        try:
            # Get workflow node definition
            node_result = await session.execute(
                select(WorkflowNode).where(
                    WorkflowNode.workflow_id == workflow_run.workflow_id,
                    WorkflowNode.node_id == node_id,
                )
            )
            workflow_node = node_result.scalar_one_or_none()
            
            if not workflow_node:
                logger.error(f"Workflow node not found: {node_id}")
                return
            
            # Create node execution
            node_execution = NodeExecution(
                workflow_run_id=workflow_run.id,
                workflow_node_id=workflow_node.id,
                node_id=node_id,
                state=NodeState.READY,
                input_data=workflow_run.input_data,
            )
            
            session.add(node_execution)
            await session.commit()
            await session.refresh(node_execution)
            
            # Create task
            task = NodeTaskEvent(
                task_id=node_execution.id,
                workflow_run_id=workflow_run.id,
                node_execution_id=node_execution.id,
                node_id=node_id,
                node_type=workflow_node.node_type.value,
                config=workflow_node.config,
                input_data=workflow_run.input_data,
                attempt=1,
                max_retries=workflow_node.retry_policy.get("max_retries", settings.execution.default_max_retries),
                timeout_seconds=workflow_node.timeout_seconds or settings.execution.default_node_timeout_seconds,
                correlation_id=workflow_run.id,
            )
            
            # Send task to Kafka
            success = await self.kafka_producer.send_task(task)
            
            if success:
                node_execution.state = NodeState.RUNNING
                await session.commit()
                
                logger.info(
                    f"Task dispatched: {task.task_id}",
                    workflow_run_id=workflow_run.id,
                    node_id=node_id,
                )
                Metrics.increment("tasks_dispatched")
            else:
                logger.error(f"Failed to dispatch task for node: {node_id}")
                Metrics.increment("task_dispatch_failures")
        except Exception as e:
            logger.error(f"Error dispatching task for node {node_id}: {str(e)}")
            Metrics.increment("scheduler_dispatch_errors")


async def main():
    """Main entry point."""
    scheduler = WorkflowScheduler()
    await scheduler.start()


if __name__ == "__main__":
    asyncio.run(main())
