import os

from utils.config.kafka import KafkaConfig
from utils.config.database import PostgresConfig


class Config:
    """Processor-specific configuration extending shared configs."""

    KAFKA_BOOTSTRAP_SERVERS: str = KafkaConfig.BOOTSTRAP_SERVERS
    KAFKA_TOPIC: str = KafkaConfig.TOPIC
    KAFKA_GROUP_ID: str = os.getenv("KAFKA_GROUP_ID", "activity-processor")
    KAFKA_AUTO_OFFSET_RESET: str = os.getenv("KAFKA_AUTO_OFFSET_RESET", "earliest")
    
    BATCH_SIZE: int = int(os.getenv("BATCH_SIZE", "100"))
    CONSUME_TIMEOUT_SECONDS: float = float(os.getenv("CONSUME_TIMEOUT_SECONDS", "1.0"))
    FLUSH_INTERVAL_SECONDS: int = int(os.getenv("FLUSH_INTERVAL_SECONDS", "60"))
    GRACE_PERIOD_SECONDS: int = int(os.getenv("GRACE_PERIOD_SECONDS", "60"))
    LAG_UPDATE_INTERVAL_SECONDS: int = int(os.getenv("LAG_UPDATE_INTERVAL_SECONDS", "10"))
    
    METRICS_PORT: int = int(os.getenv("METRICS_PORT", "8001"))
    ADMIN_PORT: int = int(os.getenv("ADMIN_PORT", "8002"))
    
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")

    @classmethod
    def postgres_connection_string(cls) -> str:
        # Delegate to shared PostgresConfig
        return PostgresConfig.connection_string()
