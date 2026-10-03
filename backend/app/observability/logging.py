"""Observability and structured logging."""
import json
import logging
import time
from datetime import datetime
from functools import wraps
from typing import Optional

from app.config import settings


class StructuredLogger:
    """Structured JSON logger for production observability."""

    def __init__(self, name: str):
        self.logger = logging.getLogger(name)
        self._setup_handler()

    def _setup_handler(self):
        """Setup JSON logging handler."""
        handler = logging.StreamHandler()
        handler.setFormatter(JSONFormatter())
        self.logger.handlers.clear()
        self.logger.addHandler(handler)
        self.logger.setLevel(settings.log_level)

    def _log(self, level: str, message: str, **extra):
        """Internal logging method."""
        log_record = {
            "timestamp": datetime.utcnow().isoformat(),
            "level": level,
            "message": message,
            "service": "agentos",
        }
        log_record.update(extra)

        if level == "DEBUG":
            self.logger.debug(json.dumps(log_record))
        elif level == "INFO":
            self.logger.info(json.dumps(log_record))
        elif level == "WARNING":
            self.logger.warning(json.dumps(log_record))
        elif level == "ERROR":
            self.logger.error(json.dumps(log_record))
        elif level == "CRITICAL":
            self.logger.critical(json.dumps(log_record))

    def debug(self, message: str, **extra):
        """Log debug message."""
        self._log("DEBUG", message, **extra)

    def info(self, message: str, **extra):
        """Log info message."""
        self._log("INFO", message, **extra)

    def warning(self, message: str, **extra):
        """Log warning message."""
        self._log("WARNING", message, **extra)

    def error(self, message: str, **extra):
        """Log error message."""
        self._log("ERROR", message, **extra)

    def critical(self, message: str, **extra):
        """Log critical message."""
        self._log("CRITICAL", message, **extra)


class JSONFormatter(logging.Formatter):
    """JSON log formatter."""

    def format(self, record: logging.LogRecord) -> str:
        return record.getMessage()


def get_logger(name: str) -> StructuredLogger:
    """Get a structured logger."""
    return StructuredLogger(name)


class Metrics:
    """Simple metrics collection."""

    _metrics = {}

    @classmethod
    def increment(cls, name: str, value: int = 1, tags: Optional[dict] = None):
        """Increment a metric counter."""
        key = name
        if tags:
            key = f"{name}:{','.join(f'{k}={v}' for k, v in tags.items())}"

        if key not in cls._metrics:
            cls._metrics[key] = 0
        cls._metrics[key] += value

    @classmethod
    def record(cls, name: str, value: float, tags: Optional[dict] = None):
        """Record a metric value."""
        key = name
        if tags:
            key = f"{name}:{','.join(f'{k}={v}' for k, v in tags.items())}"

        cls._metrics[key] = value

    @classmethod
    def get_all(cls) -> dict:
        """Get all recorded metrics."""
        return cls._metrics.copy()

    @classmethod
    def reset(cls):
        """Reset all metrics."""
        cls._metrics.clear()


def log_execution(logger: StructuredLogger):
    """Decorator to log function execution with timing."""

    def decorator(func):
        @wraps(func)
        async def async_wrapper(*args, **kwargs):
            start_time = time.time()
            try:
                result = await func(*args, **kwargs)
                duration = time.time() - start_time
                logger.info(
                    f"Function {func.__name__} completed successfully",
                    function=func.__name__,
                    duration_seconds=duration,
                    status="success",
                )
                Metrics.record(f"{func.__name__}_duration_seconds", duration)
                return result
            except Exception as e:
                duration = time.time() - start_time
                logger.error(
                    f"Function {func.__name__} failed with exception: {str(e)}",
                    function=func.__name__,
                    duration_seconds=duration,
                    error=str(e),
                    error_type=type(e).__name__,
                    status="error",
                )
                Metrics.increment(f"{func.__name__}_errors")
                raise

        @wraps(func)
        def sync_wrapper(*args, **kwargs):
            start_time = time.time()
            try:
                result = func(*args, **kwargs)
                duration = time.time() - start_time
                logger.info(
                    f"Function {func.__name__} completed successfully",
                    function=func.__name__,
                    duration_seconds=duration,
                    status="success",
                )
                Metrics.record(f"{func.__name__}_duration_seconds", duration)
                return result
            except Exception as e:
                duration = time.time() - start_time
                logger.error(
                    f"Function {func.__name__} failed with exception: {str(e)}",
                    function=func.__name__,
                    duration_seconds=duration,
                    error=str(e),
                    error_type=type(e).__name__,
                    status="error",
                )
                Metrics.increment(f"{func.__name__}_errors")
                raise

        if hasattr(func, "__await__"):
            return async_wrapper
        return sync_wrapper

    return decorator
