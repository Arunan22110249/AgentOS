"""Workflow engine and DAG execution logic."""
from enum import Enum
from typing import Dict, List, Optional, Set, Tuple

from app.database.models import NodeState, NodeType, WorkflowState
from app.observability.logging import get_logger
from pydantic import BaseModel, Field

logger = get_logger(__name__)


class WorkflowValidationError(Exception):
    """Workflow validation error."""

    pass


class DAGValidationError(Exception):
    """DAG validation error."""

    pass


class WorkflowNode(BaseModel):
    """Workflow node definition."""

    node_id: str
    node_type: NodeType
    config: Dict
    dependencies: List[str] = Field(default_factory=list)
    retry_policy: Dict = Field(default_factory=dict)
    timeout_seconds: Optional[int] = None


class WorkflowDefinition(BaseModel):
    """Workflow definition."""

    name: str
    description: Optional[str] = None
    nodes: Dict[str, Dict]  # node_id -> node_config
    timeout_seconds: int = 3600


class DAGValidator:
    """Validates workflow DAGs."""

    @staticmethod
    def validate_workflow(definition: Dict) -> Tuple[bool, Optional[str]]:
        """Validate a workflow definition."""
        try:
            # Check required fields
            if "nodes" not in definition:
                return False, "Workflow must have 'nodes' field"

            nodes = definition.get("nodes", {})
            if not isinstance(nodes, dict):
                return False, "'nodes' must be a dictionary"

            if not nodes:
                return False, "Workflow must have at least one node"

            # Validate each node
            for node_id, node_config in nodes.items():
                if not isinstance(node_id, str):
                    return False, f"Node ID must be string, got {type(node_id)}"

                if "type" not in node_config:
                    return False, f"Node {node_id} must have 'type' field"

                node_type = node_config.get("type")
                if node_type not in [nt.value for nt in NodeType]:
                    return False, f"Node {node_id} has invalid type: {node_type}"

            # Validate DAG structure
            success, error = DAGValidator.validate_dag(nodes)
            if not success:
                return False, error

            return True, None
        except Exception as e:
            logger.error(f"Workflow validation error: {str(e)}")
            return False, f"Workflow validation error: {str(e)}"

    @staticmethod
    def validate_dag(nodes: Dict[str, Dict]) -> Tuple[bool, Optional[str]]:
        """Validate DAG structure (no cycles, valid dependencies)."""
        # Check for cycles
        has_cycle, cycle_nodes = DAGValidator._detect_cycle(nodes)
        if has_cycle:
            return False, f"Cycle detected in DAG: {cycle_nodes}"

        # Validate dependencies
        for node_id, node_config in nodes.items():
            dependencies = node_config.get("dependencies", [])

            if not isinstance(dependencies, list):
                return False, f"Node {node_id} dependencies must be a list"

            for dep in dependencies:
                if dep not in nodes:
                    return False, f"Node {node_id} depends on non-existent node: {dep}"

        return True, None

    @staticmethod
    def _detect_cycle(nodes: Dict[str, Dict]) -> Tuple[bool, Optional[List[str]]]:
        """Detect cycles in the DAG using DFS."""
        visited = set()
        rec_stack = set()
        path = []

        def dfs(node_id: str) -> bool:
            visited.add(node_id)
            rec_stack.add(node_id)
            path.append(node_id)

            dependencies = nodes[node_id].get("dependencies", [])
            for dep in dependencies:
                if dep not in visited:
                    if dfs(dep):
                        return True
                elif dep in rec_stack:
                    # Cycle found
                    cycle_start = path.index(dep)
                    cycle = path[cycle_start:] + [dep]
                    path.pop()
                    return True

            path.pop()
            rec_stack.remove(node_id)
            return False

        for node_id in nodes:
            if node_id not in visited:
                if dfs(node_id):
                    return True, path

        return False, None

    @staticmethod
    def get_topological_order(nodes: Dict[str, Dict]) -> List[str]:
        """Get topological order of nodes."""
        visited = set()
        temp_visited = set()
        order = []

        def dfs(node_id: str):
            if node_id in temp_visited:
                raise DAGValidationError(f"Cycle detected at {node_id}")

            if node_id in visited:
                return

            temp_visited.add(node_id)

            dependencies = nodes[node_id].get("dependencies", [])
            for dep in dependencies:
                dfs(dep)

            temp_visited.remove(node_id)
            visited.add(node_id)
            order.append(node_id)

        for node_id in nodes:
            if node_id not in visited:
                dfs(node_id)

        return order


class ExecutionEngine:
    """Workflow execution engine."""

    @staticmethod
    def get_ready_nodes(
        nodes: Dict[str, Dict],
        completed_node_ids: Set[str],
        failed_node_ids: Set[str],
    ) -> List[str]:
        """Get nodes that are ready to execute."""
        ready = []

        for node_id, node_config in nodes.items():
            # Skip already completed or failed nodes
            if node_id in completed_node_ids or node_id in failed_node_ids:
                continue

            dependencies = node_config.get("dependencies", [])

            # Check if all dependencies are completed
            if all(dep in completed_node_ids for dep in dependencies):
                ready.append(node_id)

        return ready

    @staticmethod
    def get_independent_nodes(nodes: Dict[str, Dict]) -> List[str]:
        """Get nodes with no dependencies."""
        return [
            node_id
            for node_id, config in nodes.items()
            if not config.get("dependencies")
        ]

    @staticmethod
    def calculate_retry_delay(
        attempt: int,
        max_retries: int,
        backoff_multiplier: float = 2.0,
        base_delay: int = 1,
    ) -> Optional[int]:
        """Calculate retry delay in seconds."""
        if attempt >= max_retries:
            return None

        # Exponential backoff
        delay = base_delay * (backoff_multiplier ** (attempt - 1))
        return int(delay)

    @staticmethod
    def should_retry(
        node_error: Optional[str],
        attempt: int,
        max_retries: int,
        retryable_error_patterns: Optional[List[str]] = None,
    ) -> bool:
        """Determine if a node should be retried."""
        if attempt >= max_retries:
            return False

        if not node_error:
            return False

        # Default retryable patterns (non-retryable errors: validation, permission)
        non_retryable = [
            "validation",
            "permission",
            "unauthorized",
            "forbidden",
            "invalid",
        ]

        for pattern in non_retryable:
            if pattern.lower() in node_error.lower():
                return False

        return True


class CheckpointManager:
    """Manages workflow checkpoints for durable recovery."""

    @staticmethod
    def create_checkpoint_data(
        workflow_run_id: str,
        completed_node_ids: List[str],
        workflow_state: Dict,
    ) -> Dict:
        """Create checkpoint data."""
        return {
            "workflow_run_id": workflow_run_id,
            "completed_node_ids": completed_node_ids,
            "state_snapshot": workflow_state,
        }

    @staticmethod
    def restore_from_checkpoint(checkpoint_data: Dict) -> Tuple[List[str], Dict]:
        """Restore execution state from checkpoint."""
        completed_node_ids = checkpoint_data.get("completed_node_ids", [])
        state_snapshot = checkpoint_data.get("state_snapshot", {})
        return completed_node_ids, state_snapshot
