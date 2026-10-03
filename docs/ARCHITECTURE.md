# AgentOS Architecture

## Overview

AgentOS is a distributed workflow execution platform for AI agents. It provides durable, fault-tolerant execution with checkpoint-based recovery, multi-tenant isolation, and comprehensive observability.

## System Layers

```
┌─────────────────────────────────────────────────────┐
│                   REST API Layer                     │
│  (FastAPI routes for auth, workflows, runs, etc.)   │
└──────────────────┬──────────────────────────────────┘
                   │
┌──────────────────▼──────────────────────────────────┐
│              Application Layer                       │
│  (Workflow engine, agent executor, tool registry)   │
└──────────────────┬──────────────────────────────────┘
                   │
┌──────────────────▼──────────────────────────────────┐
│              Persistence Layer                       │
│  (PostgreSQL ORM models, Kafka events)              │
└──────────────────┬──────────────────────────────────┘
                   │
┌──────────────────▼──────────────────────────────────┐
│         Infrastructure Layer                         │
│  (Docker, Kubernetes, Terraform)                     │
└─────────────────────────────────────────────────────┘
```

## Data Model

### Tenant Isolation
```
Tenant
├── Users (with credentials)
├── Workflows (DAG definitions)
├── Agents (LLM configurations)
├── Tools (custom tool definitions)
├── WorkflowRuns (execution instances)
└── Audit logs (compliance trail)
```

### Workflow Execution
```
Workflow (DAG definition)
└── WorkflowRun (execution instance)
    ├── NodeExecutions (individual node attempts)
    │   ├── state: READY→RUNNING→COMPLETED/FAILED
    │   ├── output_data, error_message
    │   └── completed_at timestamp
    ├── Checkpoints (durable recovery points)
    │   ├── checkpoint_number
    │   ├── completed_node_ids
    │   └── state_snapshot (for resumption)
    └── ApprovalRequests (human-in-the-loop)
        ├── status: PENDING→APPROVED/REJECTED
        ├── approved_by, rejection_reason
        └── created_at timestamp
```

## Workflow Execution Flow

### 1. Workflow Creation
```
User → API → DAGValidator.validate_workflow() → PostgreSQL
```

Validates:
- All nodes exist in workflow definition
- No cycles in DAG
- Valid node types (AGENT, TOOL, APPROVAL, TASK)
- All dependencies reference existing nodes

### 2. Workflow Run Initiation
```
User → API → Create WorkflowRun (PENDING) → Publish to Kafka (workflow.events)
                                            ↓
                                    Scheduler wakes up
```

### 3. Scheduler Discovery & Task Dispatch
```
Every 30 seconds:
1. Query PostgreSQL for PENDING/RUNNING workflow runs
2. For each run:
   a. Get latest checkpoint (if any)
   b. Query completed/failed node executions
   c. ExecutionEngine.get_ready_nodes(definition, completed_ids, failed_ids)
   d. For each ready node:
      - Create NodeExecution (READY state)
      - Create NodeTaskEvent
      - Publish to Kafka (node.tasks)
```

Key: Ready nodes are those where ALL dependencies are in `completed_ids`.

### 4. Worker Task Consumption
```
Worker subscribes to node.tasks topic:
1. Consume message with timeout 1 second
2. Deserialize NodeTaskEvent
3. Idempotency check: is task_id:attempt already processed?
4. If no: Execute node, else skip
5. Update NodeExecution state, output_data, error_message
6. Create Checkpoint with completed_node_ids
7. Publish NodeResultEvent to node.results
8. Commit Kafka offset (ONLY on success)
```

Critical: Offset committed only after checkpoint + event publish succeeds.

### 5. Scheduler Event Consumption
```
Scheduler subscribes to node.results:
1. On node completion: update ready_nodes calculation
2. On node failure: mark in failed_nodes, block dependent tasks
3. When all nodes complete: mark workflow COMPLETED/FAILED
4. Update WorkflowRun.state, completed_at
```

### 6. Recovery from Failure
```
On Worker Crash:
1. Kafka consumer remains subscribed
2. On restart, consumer rebalances
3. Unpacked Checkpoint message: extract completed_node_ids
4. ExecutionEngine.get_ready_nodes uses checkpoint state
5. Resume from latest checkpoint (no re-execution)
```

Idempotency prevents duplicate execution even if message redelivered.

## Event Architecture

### Kafka Topics

**workflow.commands** (API → Scheduler)
```json
{
  "event_type": "WORKFLOW_STARTED",
  "workflow_run_id": "uuid",
  "input_data": {"key": "value"},
  "correlation_id": "uuid"
}
```

**node.tasks** (Scheduler → Workers)
```json
{
  "task_id": "node_execution_id",
  "workflow_run_id": "uuid",
  "node_id": "task_1",
  "node_type": "AGENT",
  "config": {"agent_id": "agent-1"},
  "input_data": {...},
  "attempt": 1,
  "max_retries": 3,
  "timeout_seconds": 300,
  "correlation_id": "uuid"
}
```

**node.results** (Workers → Scheduler)
```json
{
  "task_id": "node_execution_id",
  "status": "COMPLETED",
  "output_data": {...},
  "duration_seconds": 5.2,
  "correlation_id": "uuid"
}
```

**dead-letter** (Failed messages)
```json
{
  "original_topic": "node.tasks",
  "error": "Max retries exceeded",
  "message": {...},
  "timestamp": "2024-01-15T10:00:00Z"
}
```

### Event Semantics

- **At-least-once delivery**: Kafka can redelivery messages
- **Idempotency**: IdempotencyStore tracks (task_id, attempt) with TTL
- **Correlation IDs**: All events in same execution share correlation_id
- **Manual commits**: Offset committed only after business logic succeeds

## Multi-Tenancy

### Tenant Isolation Strategy
```
JWT Token:
{
  "user_id": "user-1",
  "tenant_id": "tenant-1",  ← Never trust from client
  "email": "user@example.com",
  "exp": 1234567890
}
```

Every database query:
```python
select(Workflow).where(
    Workflow.tenant_id == token_data.tenant_id,  # Enforce via query
    Workflow.id == workflow_id
)
```

Benefits:
- No cross-tenant data leakage
- Query-level isolation (not app logic)
- Audit trail via ExecutionEvent

## Agent Execution

### Agent Executor Flow
```
User Input: "Calculate the square root of 16"
    ↓
AgentExecutor.execute(user_input)
    ↓
Loop (max 5 iterations):
    1. LLMProvider.complete(messages, tools)
    2. LLM response: {"finish_reason": "tool_calls", "tool_calls": [...]}
    3. For each tool_call:
       - Validate schema
       - Execute: ToolRegistry.execute_tool(name, inputs)
       - Catch errors
       - Format as assistant message
    4. If finish_reason == "stop": break
    5. Else: continue loop
    ↓
Return: {"response": "The square root is 4", "iterations": 2, "tokens": 150}
```

### Tool Execution
```
ToolRegistry.execute_tool("http_request", {
    "method": "GET",
    "url": "https://api.example.com/data"
})
    ↓
Tool timeout: asyncio.wait_for(handler(), timeout=30s)
    ↓
On success: {"status": 200, "body": {...}}
On error: {"error": "Connection timeout"}
```

## Checkpoint System

### Checkpoint Semantics
```
After each successful node:
1. Query all NodeExecution records with state=COMPLETED
2. Collect completed_node_ids = [node.node_id for node in executions]
3. Create Checkpoint:
   - checkpoint_number = max_previous + 1
   - completed_node_ids = [all completed to date]
   - state_snapshot = arbitrary dict for app state
4. Persist to PostgreSQL
```

### Recovery Strategy
```
On restart with Checkpoint:
1. Load latest checkpoint
2. completed_ids = checkpoint.completed_node_ids
3. ready_nodes = ExecutionEngine.get_ready_nodes(definition, completed_ids, failed_ids)
4. Dispatch new tasks only for ready_nodes
5. No re-execution of completed_ids
```

Cost: 1 database write per successful node (negligible).

## Performance Considerations

### Scaling Model
```
Load → Scheduler (1 instance) → Kafka (topic with N partitions)
                                    ↓
                         Workers (1 to M instances)
                         (Kafka consumer group)
```

- Scheduler: Single instance sufficient (discovers ready nodes)
- Workers: Scale horizontally by increasing Kafka partition count
- Database: Connection pooling (size_pool=20)
- Cache: Redis for locks/rate limiting (optional)

### Latency Breakdown
```
User creates run: <100ms
Scheduler discovers ready: 30 seconds (interval)
Task in Kafka: ~10ms
Worker processes: N seconds (node execution time)
Checkpoint write: ~50ms
Back to scheduler: 30 seconds (next interval)

Total for simple node: 60+ seconds (mostly waiting on scheduler interval)
```

To reduce latency: decrease `SCHEDULER_INTERVAL_SECONDS`.

## Security

### Authentication
```
JWT with:
- user_id, tenant_id, email (claims)
- Argon2id password hashing
- Token expiration (configurable)
- Refresh tokens (future enhancement)
```

### Authorization
```
Every resource query includes:
WHERE tenant_id == token_data.tenant_id

No role-based access control yet (future: admin/user roles)
```

### Tool Safety
```
Tools have configurable permissions:
- http_request: allowed domains whitelist
- calculator: safe eval (no file/network access)
- json_transform: no code execution
- text_extract: regex only (no dangerous patterns)
```

## Monitoring & Observability

### Metrics Tracked
```
- workflow_created, workflow_started, workflow_completed, workflow_failed
- node_executions_completed, node_executions_failed, node_execution_retries
- agent_iterations, agent_tokens
- tool_executions, tool_execution_errors
- task_dispatch_failures, scheduler_errors
- database_query_latency, kafka_publish_latency
```

Exposed at `/metrics` endpoint in Prometheus format.

### Logging Strategy
```
Structured JSON logs with:
- timestamp, level, service, message
- request_id, correlation_id (for tracing)
- user_id, tenant_id (for compliance)
- duration_seconds, error details (for debugging)
```

### Tracing
```
Correlation ID flows through:
WorkflowRun → NodeExecutions → ExecutionEvents → Kafka → Worker → Checkpoint

All logs and events tagged with correlation_id for end-to-end visibility.
```

## Deployment Topology

### Local Development
```
docker-compose up
├── postgres:16 (state)
├── redis:7 (locks)
├── zookeeper (Kafka dependency)
├── kafka:7.5.0 (event stream)
├── prometheus (metrics)
├── grafana (dashboards)
├── agentos-api:8000 (FastAPI)
├── agentos-worker:1-N (task workers)
└── agentos-scheduler:1 (scheduler)
```

### Kubernetes Production
```
Deployment:
├── api-pod (replicas: 3)
├── worker-pod (replicas: 5-20 for scaling)
├── scheduler-pod (replicas: 1)
├── postgres-pod (replicas: 1, with persistent volume)
├── redis-pod (replicas: 1)
├── kafka-pod (replicas: 3)
└── prometheus/grafana (monitoring)

Service:
├── api-service (ClusterIP)
├── kafka-service (ClusterIP)
└── postgres-service (ClusterIP)
```

### Cloud Provisioning (Terraform/Bicep)
```
Azure Example:
├── AKS cluster (managed K8s)
├── Azure Database for PostgreSQL
├── Azure Cache for Redis
├── Azure Event Hubs (managed Kafka)
├── Azure Container Registry (image storage)
└── Azure Application Insights (monitoring)
```

## Future Enhancements

1. **Distributed Locking**: Redis-based locking for concurrent workflows
2. **Vector Memory**: Pinecone/Weaviate integration for semantic search
3. **Multi-Provider LLM**: Anthropic, Gemini, Ollama support
4. **Workflow Versioning**: Track workflow definition history
5. **Advanced Retry Logic**: Exponential backoff with jitter, circuit breakers
6. **Cost Tracking**: Per-tenant cost allocation (API calls, storage)
7. **Webhooks**: Notify external systems on workflow completion
8. **Scheduled Workflows**: Cron-based triggering
9. **Conditional Nodes**: If/else branching in workflows
10. **Parallel Loops**: Map/reduce over collections

## References

- [Workflow Engine Design](workflows/engine.py)
- [Kafka Event Architecture](events/kafka_events.py)
- [Checkpoint Manager](workflows/engine.py#CheckpointManager)
- [Agent Executor](agents/executor.py)
- [Task Worker](workers/task_worker.py)
- [Workflow Scheduler](scheduler/workflow_scheduler.py)
