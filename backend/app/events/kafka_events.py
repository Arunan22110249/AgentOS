"""Kafka event architecture and schemas."""
from typing import Dict, Optional, Any
from dataclasses import dataclass, asdict
from datetime import datetime
from enum import Enum
from pydantic import BaseModel
import json
import uuid


class EventType(str, Enum):
    """Event types."""
    # Workflow events
    WORKFLOW_STARTED = "workflow.started"
    WORKFLOW_COMPLETED = "workflow.completed"
    WORKFLOW_FAILED = "workflow.failed"
    WORKFLOW_CANCELLED = "workflow.cancelled"
    
    # Node events
    NODE_READY = "node.ready"
    NODE_STARTED = "node.started"
    NODE_COMPLETED = "node.completed"
    NODE_FAILED = "node.failed"
    NODE_RETRYING = "node.retrying"
    
    # Approval events
    APPROVAL_REQUESTED = "approval.requested"
    APPROVAL_APPROVED = "approval.approved"
    APPROVAL_REJECTED = "approval.rejected"
    
    # Checkpoint events
    CHECKPOINT_CREATED = "checkpoint.created"
    
    # System events
    WORKER_HEARTBEAT = "worker.heartbeat"
    WORKER_TASK_CLAIMED = "worker.task_claimed"
    WORKER_TASK_RELEASED = "worker.task_released"


class KafkaEvent(BaseModel):
    """Base Kafka event schema."""
    event_id: str
    event_type: EventType
    tenant_id: str
    workflow_run_id: str
    node_execution_id: Optional[str] = None
    correlation_id: str
    timestamp: datetime
    payload: Dict[str, Any]
    
    @staticmethod
    def create(
        event_type: EventType,
        tenant_id: str,
        workflow_run_id: str,
        payload: Dict[str, Any],
        node_execution_id: Optional[str] = None,
        correlation_id: Optional[str] = None,
    ) -> "KafkaEvent":
        """Factory method to create events."""
        return KafkaEvent(
            event_id=str(uuid.uuid4()),
            event_type=event_type,
            tenant_id=tenant_id,
            workflow_run_id=workflow_run_id,
            node_execution_id=node_execution_id,
            correlation_id=correlation_id or str(uuid.uuid4()),
            timestamp=datetime.utcnow(),
            payload=payload,
        )
    
    def to_json(self) -> str:
        """Serialize to JSON."""
        return self.model_dump_json(default=str)
    
    @staticmethod
    def from_json(data: str) -> "KafkaEvent":
        """Deserialize from JSON."""
        return KafkaEvent.model_validate_json(data)


class NodeTaskEvent(BaseModel):
    """Node task for worker execution."""
    task_id: str
    workflow_run_id: str
    node_execution_id: str
    node_id: str
    node_type: str
    config: Dict[str, Any]
    input_data: Dict[str, Any]
    attempt: int
    max_retries: int
    timeout_seconds: int
    correlation_id: str
    
    def to_json(self) -> str:
        """Serialize to JSON."""
        return json.dumps(self.model_dump(default=str))
    
    @staticmethod
    def from_json(data: str) -> "NodeTaskEvent":
        """Deserialize from JSON."""
        return NodeTaskEvent.model_validate_json(data)


class NodeResultEvent(BaseModel):
    """Node execution result."""
    task_id: str
    workflow_run_id: str
    node_execution_id: str
    node_id: str
    status: str  # COMPLETED, FAILED, TIMED_OUT
    output_data: Optional[Dict[str, Any]] = None
    error_message: Optional[str] = None
    duration_seconds: float
    correlation_id: str
    
    def to_json(self) -> str:
        """Serialize to JSON."""
        return json.dumps(self.model_dump(default=str))
    
    @staticmethod
    def from_json(data: str) -> "NodeResultEvent":
        """Deserialize from JSON."""
        return NodeResultEvent.model_validate_json(data)


class KafkaTopics:
    """Kafka topic names."""
    WORKFLOW_COMMANDS = "workflow.commands"
    WORKFLOW_EVENTS = "workflow.events"
    NODE_TASKS = "node.tasks"
    NODE_RESULTS = "node.results"
    DEAD_LETTER = "dead-letter"
    
    @staticmethod
    def all_topics() -> list[str]:
        """Get all topic names."""
        return [
            KafkaTopics.WORKFLOW_COMMANDS,
            KafkaTopics.WORKFLOW_EVENTS,
            KafkaTopics.NODE_TASKS,
            KafkaTopics.NODE_RESULTS,
            KafkaTopics.DEAD_LETTER,
        ]


class IdempotencyKey:
    """Idempotency tracking for duplicate message handling."""
    
    @staticmethod
    def generate_key(task_id: str, attempt: int) -> str:
        """Generate idempotency key for a task."""
        return f"{task_id}:{attempt}"
    
    @staticmethod
    def parse_key(key: str) -> tuple[str, int]:
        """Parse idempotency key."""
        parts = key.split(":")
        return parts[0], int(parts[1])
