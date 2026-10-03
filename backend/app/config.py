"""Backend configuration and settings."""
import os
from typing import Literal
from pydantic import Field
from pydantic_settings import BaseSettings


class DatabaseSettings(BaseSettings):
    """Database configuration."""
    
    url: str = Field(default="postgresql+asyncpg://agentos:agentos_dev_password@localhost:5432/agentos_db")
    sync_url: str = Field(default="postgresql://agentos:agentos_dev_password@localhost:5432/agentos_db")
    echo: bool = False
    pool_size: int = 10
    max_overflow: int = 20
    
    class Config:
        env_prefix = "DATABASE_"


class RedisSettings(BaseSettings):
    """Redis configuration."""
    
    url: str = Field(default="redis://localhost:6379")
    db: int = 0


class KafkaSettings(BaseSettings):
    """Kafka configuration."""
    
    bootstrap_servers: str = Field(default="localhost:9092")
    consumer_group: str = Field(default="agentos")
    max_poll_records: int = 100


class JWTSettings(BaseSettings):
    """JWT configuration."""
    
    secret: str = Field(default="dev-secret-key")
    algorithm: str = Field(default="HS256")
    expiration_hours: int = 24


class WorkerSettings(BaseSettings):
    """Worker configuration."""
    
    worker_id: str = Field(default="worker-1")
    concurrency: int = 5
    task_timeout_seconds: int = 300
    lease_duration_seconds: int = 60
    heartbeat_interval_seconds: int = 10


class SchedulerSettings(BaseSettings):
    """Scheduler configuration."""
    
    interval_seconds: int = 30
    batch_size: int = 100


class ExecutionSettings(BaseSettings):
    """Execution configuration."""
    
    default_node_timeout_seconds: int = 300
    default_workflow_timeout_seconds: int = 3600
    default_max_retries: int = 3
    default_retry_backoff_multiplier: float = 2.0


class Settings(BaseSettings):
    """Main application settings."""
    
    # API
    api_host: str = "0.0.0.0"
    api_port: int = 8000
    api_title: str = "AgentOS API"
    api_version: str = "0.1.0"
    
    # Environment
    environment: Literal["development", "staging", "production"] = "development"
    log_level: str = "INFO"
    
    # Sub-configurations
    database: DatabaseSettings = Field(default_factory=DatabaseSettings)
    redis: RedisSettings = Field(default_factory=RedisSettings)
    kafka: KafkaSettings = Field(default_factory=KafkaSettings)
    jwt: JWTSettings = Field(default_factory=JWTSettings)
    worker: WorkerSettings = Field(default_factory=WorkerSettings)
    scheduler: SchedulerSettings = Field(default_factory=SchedulerSettings)
    execution: ExecutionSettings = Field(default_factory=ExecutionSettings)
    
    # LLM
    openai_api_key: str = ""
    openai_model: str = "gpt-4"
    
    # Observability
    otel_exporter_otlp_endpoint: str = "http://localhost:4317"
    prometheus_metrics_port: int = 8001
    
    # Feature flags
    enable_agent_system: bool = True
    enable_approval_nodes: bool = True
    enable_memory_persistence: bool = True
    
    class Config:
        env_file = ".env"
        env_nested_delimiter = "__"
        case_sensitive = False
    
    @property
    def is_production(self) -> bool:
        """Check if running in production."""
        return self.environment == "production"
    
    @property
    def is_development(self) -> bool:
        """Check if running in development."""
        return self.environment == "development"


def get_settings() -> Settings:
    """Get application settings."""
    return Settings(
        database=DatabaseSettings(
            url=os.getenv("DATABASE_URL", "postgresql+asyncpg://agentos:agentos_dev_password@localhost:5432/agentos_db"),
            sync_url=os.getenv("DATABASE_URL_SYNC", "postgresql://agentos:agentos_dev_password@localhost:5432/agentos_db"),
        ),
        redis=RedisSettings(
            url=os.getenv("REDIS_URL", "redis://localhost:6379"),
        ),
        kafka=KafkaSettings(
            bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        ),
        jwt=JWTSettings(
            secret=os.getenv("JWT_SECRET", "dev-secret-key"),
            algorithm=os.getenv("JWT_ALGORITHM", "HS256"),
            expiration_hours=int(os.getenv("JWT_EXPIRATION_HOURS", "24")),
        ),
        worker=WorkerSettings(
            worker_id=os.getenv("WORKER_ID", "worker-1"),
            concurrency=int(os.getenv("WORKER_CONCURRENCY", "5")),
            task_timeout_seconds=int(os.getenv("WORKER_TASK_TIMEOUT_SECONDS", "300")),
            lease_duration_seconds=int(os.getenv("WORKER_LEASE_DURATION_SECONDS", "60")),
        ),
        scheduler=SchedulerSettings(
            interval_seconds=int(os.getenv("SCHEDULER_INTERVAL_SECONDS", "30")),
            batch_size=int(os.getenv("SCHEDULER_BATCH_SIZE", "100")),
        ),
        execution=ExecutionSettings(
            default_node_timeout_seconds=int(os.getenv("DEFAULT_NODE_TIMEOUT_SECONDS", "300")),
            default_workflow_timeout_seconds=int(os.getenv("DEFAULT_WORKFLOW_TIMEOUT_SECONDS", "3600")),
            default_max_retries=int(os.getenv("DEFAULT_MAX_RETRIES", "3")),
            default_retry_backoff_multiplier=float(os.getenv("DEFAULT_RETRY_BACKOFF_MULTIPLIER", "2.0")),
        ),
        openai_api_key=os.getenv("OPENAI_API_KEY", ""),
        openai_model=os.getenv("OPENAI_MODEL", "gpt-4"),
        environment=os.getenv("ENVIRONMENT", "development"),
        log_level=os.getenv("LOG_LEVEL", "INFO"),
    )


settings = get_settings()
