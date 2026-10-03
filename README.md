# AgentOS

**A production-grade distributed execution platform for AI agents.**

AgentOS is a comprehensive system for running AI-agent workflows as durable, fault-tolerant DAG executions with checkpoint recovery, multi-tenant isolation, and human-in-the-loop approvals.

## Why AgentOS?

Most AI applications assume synchronous execution. Real-world workflows need:

- **Durability**: Survive worker crashes and resume from checkpoints
- **Fault tolerance**: Automatic retries with exponential backoff
- **Concurrency**: Independent DAG nodes execute in parallel
- **Observability**: Full execution traces for debugging
- **Multi-tenancy**: Secure tenant isolation from day one
- **Scalability**: Distributed workers via Kafka

AgentOS solves these problems.

## Core Capabilities

### 1. Durable Workflow Execution
- DAG-based workflow definition (JSON/YAML)
- Node state transitions with persistence
- Checkpoints after each successful node
- **Recovery**: Restart from latest checkpoint without re-executing completed nodes

### 2. Distributed Task Execution
- Kafka-based task queue
- Horizontal worker scaling
- At-least-once delivery semantics
- Idempotent processing for duplicate message handling
- Dead-letter queue for failed tasks

### 3. Workflow Engine
- DAG validation (cycle detection, dependency checking)
- Topological ordering for parallel execution
- Configurable retry policies (exponential backoff)
- Timeout enforcement per node and workflow
- Node-level and workflow-level concurrency limits

### 4. Agent System
- Agent abstraction with configurable system instructions
- LLM provider abstraction (OpenAI-compatible)
- Tool calling with validated input/output schemas
- Execution memory for agent state
- Configurable iteration limits and token budgets

### 5. Tool Registry
- Built-in tools: HTTP requests, calculator, JSON transform, text extraction
- Tenant-scoped tool permissions
- Custom tool registration
- Tool timeout enforcement

### 6. Multi-Tenancy
- Tenant isolation at database level
- User registration and authentication
- JWT-based session management
- Tenant-scoped workflows, agents, tools, memory
- Audit logging for compliance

### 7. Human-in-the-Loop
- Approval nodes that pause execution
- Pending approval listing and filtering
- Approve/reject with optional reasoning
- Checkpoint preservation across approval cycles

### 8. Observability
- Structured JSON logging
- Correlation IDs for request tracing
- Prometheus metrics
- Execution event audit trail
- OpenTelemetry spans (infrastructure)

## Technology Stack

### Backend
- **Framework**: FastAPI
- **ORM**: SQLAlchemy 2.x async
- **Database**: PostgreSQL
- **Cache**: Redis
- **Messaging**: Apache Kafka
- **Auth**: JWT + Argon2
- **LLM**: OpenAI-compatible providers
- **Logging**: Structured JSON
- **Metrics**: Prometheus

### Frontend
- React + TypeScript + Vite

### Infrastructure
- Docker Compose for local development
- Kubernetes manifests for production
- Terraform for cloud provisioning

## Quick Start

### Prerequisites
- Python 3.11+
- Docker and Docker Compose
- PostgreSQL, Redis, Kafka (included in docker-compose)

### Local Development

#### 1. Setup Environment
```bash
cp .env.example .env
# Edit .env with your configuration
# Required: OPENAI_API_KEY for agent execution
```

#### 2. Start Infrastructure
```bash
docker-compose up -d
# Wait for all services to be healthy
docker-compose ps
```

#### 3. Initialize Database
```bash
# Create tables
python -m app.database.models

# Or use alembic for migrations
alembic upgrade head
```

#### 4. Start Services

Terminal 1 - API:
```bash
cd backend
uvicorn app.main:app --reload --port 8000
```

Terminal 2 - Task Worker:
```bash
cd backend
python -m app.workers.task_worker
```

Terminal 3 - Scheduler:
```bash
cd backend
python -m app.scheduler.workflow_scheduler
```

#### 5. Access Services
- API: http://localhost:8000
- API Docs: http://localhost:8000/docs
- Grafana: http://localhost:3000 (admin/admin)
- Prometheus: http://localhost:9090

### Example Workflow

#### 1. Create a Tenant
```bash
curl -X POST http://localhost:8000/auth/tenants \
  -H "Content-Type: application/json" \
  -d '{"name": "demo", "description": "Demo tenant"}'
```

#### 2. Register User
```bash
curl -X POST http://localhost:8000/auth/register \
  -H "Content-Type: application/json" \
  -d '{
    "tenant_name": "demo",
    "email": "user@example.com",
    "username": "demouser",
    "password": "securepass123"
  }'
```

#### 3. Login
```bash
curl -X POST http://localhost:8000/auth/login \
  -H "Content-Type: application/json" \
  -d '{
    "tenant_id": "YOUR_TENANT_ID",
    "email": "user@example.com",
    "password": "securepass123"
  }'
# Response: {"access_token": "JWT_TOKEN"}
```

#### 4. Create a Simple Workflow
```bash
curl -X POST http://localhost:8000/workflows \
  -H "Authorization: Bearer JWT_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "demo_workflow",
    "description": "Simple demo",
    "timeout_seconds": 3600,
    "definition": {
      "nodes": {
        "task_1": {
          "type": "TASK",
          "dependencies": [],
          "config": {"description": "First task"}
        },
        "task_2": {
          "type": "TASK",
          "dependencies": ["task_1"],
          "config": {"description": "Second task"}
        }
      }
    }
  }'
```

#### 5. Create a Workflow Run
```bash
curl -X POST http://localhost:8000/workflows/{workflow_id}/runs \
  -H "Authorization: Bearer JWT_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"input_data": {"key": "value"}}'
```

#### 6. Monitor Execution
```bash
# Get run status
curl http://localhost:8000/runs/{run_id} \
  -H "Authorization: Bearer JWT_TOKEN"

# Get events
curl http://localhost:8000/runs/{run_id}/events \
  -H "Authorization: Bearer JWT_TOKEN"
```

## Architecture

```
                        ┌─────────────────┐
                        │   Users         │
                        └────────┬────────┘
                                 │
                    ┌────────────┴────────────┐
                    │                         │
            ┌───────▼────────┐        ┌──────▼──────┐
            │   FastAPI      │        │  React UI   │
            │   (8000)       │        │  (3001)     │
            └───────┬────────┘        └─────────────┘
                    │
        ┌───────────┼───────────┐
        │           │           │
   ┌────▼─────┐ ┌──▼───┐ ┌────▼──────┐
   │ PostgreSQL│ │Redis │ │  Kafka    │
   │ (state)   │ │ Lock │ │ (tasks)   │
   └──────────┘ └──────┘ └──────┬────┘
                               │
                    ┌──────────┴──────────┐
                    │                     │
            ┌───────▼────────┐    ┌──────▼──────┐
            │  Task Workers  │    │  Scheduler  │
            │  (scaled)      │    │             │
            └────────────────┘    └─────────────┘

Data Flow:
1. User creates workflow run via API
2. Scheduler discovers ready nodes
3. Scheduler dispatches tasks to Kafka
4. Workers consume tasks, execute nodes
5. Workers persist results to PostgreSQL
6. Workers emit events to Kafka
7. Checkpoints created after successful nodes
8. On failure/restart: resume from latest checkpoint
```

## API Reference

### Authentication
- `POST /auth/tenants` - Create tenant
- `POST /auth/register` - Register user
- `POST /auth/login` - Authenticate and get JWT
- `GET /auth/me` - Get current user

### Workflows
- `POST /workflows` - Create workflow
- `GET /workflows` - List workflows
- `GET /workflows/{id}` - Get workflow
- `POST /workflows/{id}/runs` - Create run

### Workflow Runs
- `GET /runs/{id}` - Get run status
- `POST /runs/{id}/cancel` - Cancel run
- `GET /runs/{id}/events` - Get execution events

### Agents
- `POST /agents` - Create agent
- `GET /agents` - List agents
- `GET /agents/{id}` - Get agent

### Tools
- `POST /tools` - Create tool
- `GET /tools` - List tools (includes built-in)

### Approvals
- `GET /approvals` - List pending approvals
- `POST /approvals/{id}/approve` - Approve
- `POST /approvals/{id}/reject` - Reject

### Health
- `GET /health` - Health check
- `GET /ready` - Readiness check
- `GET /metrics` - Application metrics

## Testing

### Unit Tests
```bash
pytest tests/unit -v
```

### Integration Tests
```bash
# Requires Docker Compose running
pytest tests/integration -v
```

### Failure/Recovery Tests
```bash
pytest tests/failure -v -s
```

### Run All Tests
```bash
pytest tests/ -v --cov=app --cov-report=html
```

## Deployment

### Kubernetes
```bash
kubectl apply -f infrastructure/kubernetes/
```

### Terraform (Azure)
```bash
cd infrastructure/terraform
terraform plan
terraform apply
```

## Feature Status

| Feature                  | Implemented | Tested | Status |
|--------------------------|-------------|--------|--------|
| Workflow DAG              | ✓           | ✓      | Complete |
| Checkpointing             | ✓           | ✓      | Complete |
| Kafka execution           | ✓           | ✓      | Complete |
| Worker recovery           | ✓           | ✓      | Complete |
| Agent execution           | ✓           | ✓      | Complete |
| Tool registry             | ✓           | ✓      | Complete |
| Memory system             | ✓           | ✓      | Complete |
| Human approval            | ✓           | ✓      | Complete |
| Multi-tenancy             | ✓           | ✓      | Complete |
| Observability             | ✓           | ✓      | Complete |
| Docker Compose            | ✓           | ✓      | Complete |
| Kubernetes                | ✓           | ~      | Manifests ready |
| Terraform                 | ✓           | ~      | IaC ready |
| Frontend                  | ✓           | ✓      | Complete |
| CI/CD Pipeline            | ✓           | ✓      | Complete |

✓ = Verified  
~ = Structure ready, not deployed  

## Security Considerations

- ✓ Multi-tenant isolation enforced at DB level
- ✓ Password hashing (Argon2)
- ✓ JWT token verification
- ✓ Tool execution sandboxing (config-based permissions)
- ✓ Audit logging for compliance
- ✓ No hardcoded secrets (environment variables)
- ⚠ Built-in tools have network access (HTTP, regex) - use tool permissions in production
- ⚠ Agent iteration limits prevent infinite loops
- ⚠ LLM token budgets prevent excessive costs

## Known Limitations

1. **Vector Memory**: The system uses PostgreSQL for memory, not vector databases. Suitable for structured state, not semantic search across documents.

2. **Real-time Streaming**: Scheduler runs on a fixed interval. For real-time workflows, reduce `SCHEDULER_INTERVAL_SECONDS`.

3. **LLM Provider**: Built on OpenAI API. Multi-provider abstraction exists but only OpenAI is tested.

4. **Distributed Locks**: No distributed locking layer. Works well for typical workloads; may need Redis locks at higher concurrency.

5. **Async Database Operations**: Full async/await architecture may have overhead for small teams. Sync option available via direct SQLAlchemy.

## Contributing

1. Create a feature branch
2. Write tests first
3. Implement feature
4. Run: `pytest`, `black`, `flake8`, `mypy`
5. Submit PR

## License

MIT

## Support

- Documentation: [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)
- Setup Guide: [docs/SETUP.md](docs/SETUP.md)
- Validation Report: [docs/VALIDATION.md](docs/VALIDATION.md)
