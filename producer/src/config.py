import os

from shared.config.kafka import KafkaConfig


class Config:
    """Producer-specific configuration extending shared configs."""

    KAFKA_BOOTSTRAP_SERVERS: str = KafkaConfig.BOOTSTRAP_SERVERS
    KAFKA_TOPIC: str = KafkaConfig.TOPIC
    
    MIN_INTERVAL_MS: int = int(os.getenv("MIN_INTERVAL_MS", "50"))
    MAX_INTERVAL_MS: int = int(os.getenv("MAX_INTERVAL_MS", "2000"))
    
    USER_POOL_SIZE: int = int(os.getenv("USER_POOL_SIZE", "100"))
    
    METRICS_PORT: int = int(os.getenv("METRICS_PORT", "8000"))
    
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")
