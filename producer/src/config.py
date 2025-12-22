import os


class Config:
    KAFKA_BOOTSTRAP_SERVERS: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    KAFKA_TOPIC: str = os.getenv("KAFKA_TOPIC", "user-activity-events")
    
    MIN_INTERVAL_MS: int = int(os.getenv("MIN_INTERVAL_MS", "50"))
    MAX_INTERVAL_MS: int = int(os.getenv("MAX_INTERVAL_MS", "2000"))
    
    USER_POOL_SIZE: int = int(os.getenv("USER_POOL_SIZE", "100"))
    
    METRICS_PORT: int = int(os.getenv("METRICS_PORT", "8000"))
    
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")

