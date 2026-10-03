"""Kafka producer and consumer abstractions."""
import asyncio
import json
from abc import ABC, abstractmethod
from typing import Any, Callable, Dict, Optional

from app.config import settings
from app.events.kafka_events import (
    KafkaEvent,
    KafkaTopics,
    NodeResultEvent,
    NodeTaskEvent,
)
from app.observability.logging import Metrics, get_logger
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError

logger = get_logger(__name__)


class KafkaProducerService:
    """Kafka producer for publishing events."""

    def __init__(self):
        self.producer = KafkaProducer(
            bootstrap_servers=settings.kafka.bootstrap_servers.split(","),
            value_serializer=lambda v: v.encode("utf-8") if isinstance(v, str) else v,
        )

    async def send_event(
        self, topic: str, event: KafkaEvent, key: Optional[str] = None
    ) -> bool:
        """Send an event to Kafka."""
        try:
            message_key = key.encode("utf-8") if key else None
            message_value = event.to_json()

            # Send with callback
            future = self.producer.send(topic, value=message_value, key=message_key)
            record_metadata = await asyncio.to_thread(future.get, timeout=10)

            logger.info(
                f"Event sent to Kafka",
                topic=topic,
                event_type=event.event_type,
                partition=record_metadata.partition,
                offset=record_metadata.offset,
            )
            Metrics.increment("kafka_events_sent", tags={"topic": topic})
            return True
        except Exception as e:
            logger.error(f"Failed to send event to Kafka: {str(e)}", topic=topic)
            Metrics.increment("kafka_send_errors", tags={"topic": topic})
            return False

    async def send_task(self, task: NodeTaskEvent) -> bool:
        """Send a node task."""
        try:
            message_key = task.workflow_run_id.encode("utf-8")
            message_value = task.to_json()

            future = self.producer.send(
                KafkaTopics.NODE_TASKS, value=message_value, key=message_key
            )
            await asyncio.to_thread(future.get, timeout=10)

            logger.info(
                f"Task sent to Kafka",
                workflow_run_id=task.workflow_run_id,
                node_id=task.node_id,
            )
            Metrics.increment("kafka_tasks_sent")
            return True
        except Exception as e:
            logger.error(f"Failed to send task: {str(e)}")
            Metrics.increment("kafka_task_send_errors")
            return False

    def close(self):
        """Close the producer."""
        self.producer.close()


class KafkaConsumerService:
    """Kafka consumer for processing events."""

    def __init__(self, topic: str, group_id: str = settings.kafka.consumer_group):
        self.topic = topic
        self.group_id = group_id
        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=settings.kafka.bootstrap_servers.split(","),
            group_id=group_id,
            enable_auto_commit=False,  # Manual commits for durability
            auto_offset_reset="earliest",
            max_poll_records=settings.kafka.max_poll_records,
            value_deserializer=lambda m: m.decode("utf-8"),
        )
        self.processed_keys: set[str] = set()  # Idempotency tracking

    async def consume_messages(
        self,
        handler: Callable[[Dict[str, Any]], bool],
        timeout_ms: int = 1000,
    ) -> int:
        """Consume messages and invoke handler."""
        messages_processed = 0

        try:
            for message in self.consumer.poll(
                timeout_ms=timeout_ms, max_records=settings.kafka.max_poll_records
            ).values():
                for msg in message:
                    try:
                        # Idempotency check
                        if msg.key and msg.key.decode() in self.processed_keys:
                            logger.info(f"Skipping duplicate message: {msg.key}")
                            self.consumer.commit()
                            continue

                        # Parse message
                        data = json.loads(msg.value)

                        # Invoke handler
                        success = handler(data)

                        if success:
                            # Commit offset only after successful processing
                            self.consumer.commit()

                            # Track processed key
                            if msg.key:
                                self.processed_keys.add(msg.key.decode())

                            messages_processed += 1
                            Metrics.increment(
                                "kafka_messages_processed", tags={"topic": self.topic}
                            )
                        else:
                            logger.warning(
                                f"Handler returned False for message: {msg.key}"
                            )
                            Metrics.increment(
                                "kafka_message_handler_failures",
                                tags={"topic": self.topic},
                            )
                            # Don't commit - message will be retried
                    except Exception as e:
                        logger.error(f"Error processing message: {str(e)}")
                        Metrics.increment(
                            "kafka_message_errors", tags={"topic": self.topic}
                        )
                        # Send to DLQ would go here
        except Exception as e:
            logger.error(f"Consumer error: {str(e)}")
            Metrics.increment("kafka_consumer_errors", tags={"topic": self.topic})

        return messages_processed

    def close(self):
        """Close the consumer."""
        self.consumer.close()


class IdempotencyStore:
    """Store for tracking processed message IDs to ensure idempotency."""

    def __init__(self, ttl_seconds: int = 3600):
        self.ttl_seconds = ttl_seconds
        self.store: Dict[str, float] = {}

    def mark_processed(self, key: str) -> None:
        """Mark a message as processed."""
        import time

        self.store[key] = time.time()

    def is_processed(self, key: str) -> bool:
        """Check if a message was already processed."""
        import time

        if key not in self.store:
            return False

        # Clean up expired entries
        current_time = time.time()
        if current_time - self.store[key] > self.ttl_seconds:
            del self.store[key]
            return False

        return True

    def cleanup_expired(self) -> int:
        """Clean up expired entries."""
        import time

        current_time = time.time()
        expired_keys = [
            k for k, v in self.store.items() if current_time - v > self.ttl_seconds
        ]

        for key in expired_keys:
            del self.store[key]

        return len(expired_keys)


# Singleton instances
_producer = None
_consumers: Dict[str, KafkaConsumerService] = {}
_idempotency_store = IdempotencyStore()


def get_kafka_producer() -> KafkaProducerService:
    """Get Kafka producer singleton."""
    global _producer
    if _producer is None:
        _producer = KafkaProducerService()
    return _producer


def get_kafka_consumer(topic: str) -> KafkaConsumerService:
    """Get Kafka consumer singleton for topic."""
    if topic not in _consumers:
        _consumers[topic] = KafkaConsumerService(topic)
    return _consumers[topic]


def get_idempotency_store() -> IdempotencyStore:
    """Get idempotency store singleton."""
    return _idempotency_store
