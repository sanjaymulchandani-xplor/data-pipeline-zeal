import json
import logging
from abc import ABC, abstractmethod

from confluent_kafka import Producer
from prometheus_client import Counter, Histogram

from utils.domain.events import UserActivityEvent


logger = logging.getLogger(__name__)


EVENTS_PRODUCED = Counter(
    "producer_events_total",
    "Total number of events produced",
    ["event_type"],
)

PUBLISH_LATENCY = Histogram(
    "producer_publish_latency_seconds",
    "Time spent publishing events to Kafka",
    buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0],
)

PUBLISH_ERRORS = Counter(
    "producer_publish_errors_total",
    "Total number of publish errors",
)


class EventPublisher(ABC):
    """Abstract base class defining the interface for event publishing."""

    @abstractmethod
    def publish(self, event: UserActivityEvent) -> None:
        # Publish an event to the message broker
        pass

    @abstractmethod
    def flush(self) -> None:
        # Flush pending messages
        pass


class KafkaEventPublisher(EventPublisher):
    """Kafka implementation of the event publisher."""

    def __init__(self, bootstrap_servers: str, topic: str):
        self._topic = topic
        self._producer = Producer({
            "bootstrap.servers": bootstrap_servers,
            "acks": "all",
            "retries": 3,
            "retry.backoff.ms": 100,
            "linger.ms": 5,
            "batch.size": 16384,
        })

    def _delivery_callback(self, err, msg):
        if err is not None:
            logger.error("Failed to deliver message: %s", err)
            PUBLISH_ERRORS.inc()
        else:
            logger.debug("Message delivered to %s [%d]", msg.topic(), msg.partition())

    def publish(self, event: UserActivityEvent) -> None:
        with PUBLISH_LATENCY.time():
            payload = json.dumps(event.to_dict()).encode("utf-8")
            self._producer.produce(
                self._topic,
                key=event.user_id.encode("utf-8"),
                value=payload,
                callback=self._delivery_callback,
            )
            self._producer.poll(0)
        
        EVENTS_PRODUCED.labels(event_type=event.event_type).inc()

    def flush(self) -> None:
        self._producer.flush(timeout=10)

