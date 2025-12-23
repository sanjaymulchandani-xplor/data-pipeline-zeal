import json
import logging
from typing import Iterator, Optional

from confluent_kafka import Consumer, KafkaError, KafkaException
from prometheus_client import Counter, Gauge, Histogram

from application.aggregation_service import EventConsumer
from utils.domain.events import UserActivityEvent


logger = logging.getLogger(__name__)


EVENTS_CONSUMED = Counter(
    "processor_events_consumed_total",
    "Total number of events consumed from Kafka",
    ["event_type"],
)

CONSUME_ERRORS = Counter(
    "processor_consume_errors_total",
    "Total number of consume errors",
    ["error_type"],
)

CONSUMER_LAG = Gauge(
    "processor_consumer_lag",
    "Current consumer lag per partition",
    ["partition"],
)

BATCH_SIZE = Histogram(
    "processor_batch_size",
    "Size of consumed batches",
    buckets=[1, 5, 10, 25, 50, 100, 250, 500],
)

DESERIALIZE_LATENCY = Histogram(
    "processor_deserialize_latency_seconds",
    "Time spent deserializing messages",
    buckets=[0.0001, 0.0005, 0.001, 0.005, 0.01],
)


class KafkaEventConsumer(EventConsumer):
    """Kafka consumer that reads user activity events from a topic."""

    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        group_id: str,
        auto_offset_reset: str = "earliest",
    ):
        # Initialize the Kafka consumer with connection settings
        self._topic = topic
        self._consumer = Consumer({
            "bootstrap.servers": bootstrap_servers,
            "group.id": group_id,
            "auto.offset.reset": auto_offset_reset,
            "enable.auto.commit": False,
            "max.poll.interval.ms": 300000,
            "session.timeout.ms": 45000,
            "fetch.min.bytes": 1,
            "fetch.wait.max.ms": 500,
        })
        self._consumer.subscribe([topic])
        logger.info("Subscribed to topic: %s", topic)

    def consume_batch(
        self,
        max_messages: int = 100,
        timeout_seconds: float = 1.0,
    ) -> Iterator[UserActivityEvent]:
        # Consume a batch of messages from Kafka and yield deserialized events
        messages = self._consumer.consume(
            num_messages=max_messages,
            timeout=timeout_seconds,
        )
        
        if messages:
            BATCH_SIZE.observe(len(messages))
        
        for msg in messages:
            if msg is None:
                continue
            
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                CONSUME_ERRORS.labels(error_type=msg.error().name()).inc()
                logger.error("Kafka error: %s", msg.error())
                continue
            
            try:
                with DESERIALIZE_LATENCY.time():
                    data = json.loads(msg.value().decode("utf-8"))
                    event = UserActivityEvent.from_dict(data)
                
                EVENTS_CONSUMED.labels(event_type=event.event_type).inc()
                yield event
                
            except (json.JSONDecodeError, KeyError, ValueError) as e:
                CONSUME_ERRORS.labels(error_type="deserialization").inc()
                logger.error("Failed to deserialize message: %s", e)
                continue

    def commit(self) -> None:
        # Commit current consumer offsets synchronously
        try:
            self._consumer.commit(asynchronous=False)
        except KafkaException as e:
            logger.error("Failed to commit offsets: %s", e)
            raise

    def update_lag_metrics(self) -> None:
        # Calculate and update consumer lag metrics for each partition
        try:
            assigned_partitions = self._consumer.assignment()
            if not assigned_partitions:
                return
            
            committed_offsets = self._consumer.committed(assigned_partitions)
            for topic_partition in assigned_partitions:
                low_watermark, high_watermark = self._consumer.get_watermark_offsets(topic_partition)
                committed_offset = next(
                    (
                        partition_offset.offset
                        for partition_offset in committed_offsets
                        if partition_offset.partition == topic_partition.partition
                    ),
                    low_watermark,
                )
                if committed_offset >= 0:
                    lag = high_watermark - committed_offset
                    CONSUMER_LAG.labels(partition=topic_partition.partition).set(lag)
        except KafkaException as e:
            logger.warning("Failed to update lag metrics: %s", e)

    def close(self) -> None:
        # Close the Kafka consumer connection gracefully
        logger.info("Closing Kafka consumer")
        self._consumer.close()

