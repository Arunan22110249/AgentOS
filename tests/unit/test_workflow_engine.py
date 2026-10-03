"""Unit tests for workflow engine."""
import pytest
from app.workflows.engine import DAGValidator, ExecutionEngine


class TestDAGValidator:
    """Test DAG validation."""

    def test_valid_simple_dag(self):
        """Test validation of simple DAG."""
        workflow = {
            "nodes": {
                "task_1": {"type": "TASK", "dependencies": []},
                "task_2": {"type": "TASK", "dependencies": ["task_1"]},
            }
        }

        valid, error = DAGValidator.validate_workflow(workflow)
        assert valid
        assert error is None

    def test_cycle_detection(self):
        """Test cycle detection."""
        workflow = {
            "nodes": {
                "task_1": {"type": "TASK", "dependencies": ["task_2"]},
                "task_2": {"type": "TASK", "dependencies": ["task_1"]},
            }
        }

        valid, error = DAGValidator.validate_workflow(workflow)
        assert not valid
        assert "Cycle" in error

    def test_missing_dependency(self):
        """Test validation of missing dependency."""
        workflow = {
            "nodes": {
                "task_1": {"type": "TASK", "dependencies": ["nonexistent"]},
            }
        }

        valid, error = DAGValidator.validate_workflow(workflow)
        assert not valid
        assert "nonexistent" in error.lower() or "non-existent" in error.lower()

    def test_empty_workflow(self):
        """Test validation of empty workflow."""
        workflow = {"nodes": {}}

        valid, error = DAGValidator.validate_workflow(workflow)
        assert not valid

    def test_topological_order(self):
        """Test topological ordering."""
        nodes = {
            "task_1": {"type": "TASK", "dependencies": []},
            "task_2": {"type": "TASK", "dependencies": ["task_1"]},
            "task_3": {"type": "TASK", "dependencies": ["task_1"]},
            "task_4": {"type": "TASK", "dependencies": ["task_2", "task_3"]},
        }

        order = DAGValidator.get_topological_order(nodes)

        # Verify order
        assert order.index("task_1") < order.index("task_2")
        assert order.index("task_1") < order.index("task_3")
        assert order.index("task_2") < order.index("task_4")
        assert order.index("task_3") < order.index("task_4")


class TestExecutionEngine:
    """Test execution engine."""

    def test_get_ready_nodes_no_dependencies(self):
        """Test getting ready nodes when none have dependencies."""
        nodes = {
            "task_1": {"type": "TASK", "dependencies": []},
            "task_2": {"type": "TASK", "dependencies": []},
        }

        ready = ExecutionEngine.get_ready_nodes(nodes, set(), set())
        assert set(ready) == {"task_1", "task_2"}

    def test_get_ready_nodes_with_dependencies(self):
        """Test getting ready nodes with dependencies."""
        nodes = {
            "task_1": {"type": "TASK", "dependencies": []},
            "task_2": {"type": "TASK", "dependencies": ["task_1"]},
            "task_3": {"type": "TASK", "dependencies": ["task_1"]},
        }

        # Only task_1 is ready initially
        ready = ExecutionEngine.get_ready_nodes(nodes, set(), set())
        assert ready == ["task_1"]

        # After task_1 completes, task_2 and task_3 are ready
        ready = ExecutionEngine.get_ready_nodes(nodes, {"task_1"}, set())
        assert set(ready) == {"task_2", "task_3"}

    def test_skip_completed_nodes(self):
        """Test that completed nodes are skipped."""
        nodes = {
            "task_1": {"type": "TASK", "dependencies": []},
            "task_2": {"type": "TASK", "dependencies": ["task_1"]},
        }

        ready = ExecutionEngine.get_ready_nodes(nodes, {"task_1"}, set())
        assert "task_1" not in ready
        assert "task_2" in ready

    def test_skip_failed_nodes(self):
        """Test that failed nodes are skipped."""
        nodes = {
            "task_1": {"type": "TASK", "dependencies": []},
            "task_2": {"type": "TASK", "dependencies": ["task_1"]},
        }

        ready = ExecutionEngine.get_ready_nodes(nodes, set(), {"task_1"})
        assert "task_1" not in ready
        assert "task_2" not in ready

    def test_retry_delay_calculation(self):
        """Test retry delay calculation."""
        # First retry
        delay = ExecutionEngine.calculate_retry_delay(1, 3, 2.0, 1)
        assert delay == 1

        # Second retry
        delay = ExecutionEngine.calculate_retry_delay(2, 3, 2.0, 1)
        assert delay == 2

        # Third retry
        delay = ExecutionEngine.calculate_retry_delay(3, 3, 2.0, 1)
        assert delay is None  # Max retries exceeded

    def test_should_retry_retryable_error(self):
        """Test retry decision for transient errors."""
        should = ExecutionEngine.should_retry("Connection timeout", 1, 3)
        assert should is True

    def test_should_retry_non_retryable_error(self):
        """Test retry decision for permanent errors."""
        should = ExecutionEngine.should_retry("Validation error: invalid input", 1, 3)
        assert should is False

    def test_should_retry_max_retries(self):
        """Test retry decision when max retries exceeded."""
        should = ExecutionEngine.should_retry("Connection timeout", 3, 3)
        assert should is False
