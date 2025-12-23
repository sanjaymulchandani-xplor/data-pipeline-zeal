import os


class KafkaConfig:
    """Shared Kafka configuration."""

    BOOTSTRAP_SERVERS: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    TOPIC: str = os.getenv("KAFKA_TOPIC", "user-activity-events")

