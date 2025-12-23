import os

from utils.config.database import PostgresConfig


class Config:
    """API-specific configuration extending shared configs."""

    PROMETHEUS_URL: str = os.getenv("PROMETHEUS_URL", "http://prometheus:9090")
    PRODUCER_METRICS_URL: str = os.getenv("PRODUCER_METRICS_URL", "http://producer:8000/metrics")
    PROCESSOR_METRICS_URL: str = os.getenv("PROCESSOR_METRICS_URL", "http://processor:8001/metrics")
    
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")

    @classmethod
    def postgres_connection_string(cls) -> str:
        # Delegate to shared PostgresConfig
        return PostgresConfig.connection_string()
