import logging
import random
import signal
import sys
import time
from threading import Event

from prometheus_client import start_http_server

from config import Config
from domain.event_generator import EventGenerator
from infrastructure.kafka_publisher import KafkaEventPublisher


logging.basicConfig(
    level=getattr(logging, Config.LOG_LEVEL),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


shutdown_event = Event()


def signal_handler(signum, frame):
    logger.info("Shutdown signal received")
    shutdown_event.set()


def main():
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    logger.info("Starting metrics server on port %d", Config.METRICS_PORT)
    start_http_server(Config.METRICS_PORT)

    publisher = KafkaEventPublisher(
        bootstrap_servers=Config.KAFKA_BOOTSTRAP_SERVERS,
        topic=Config.KAFKA_TOPIC,
    )
    
    generator = EventGenerator(user_pool_size=Config.USER_POOL_SIZE)
    
    logger.info(
        "Starting event producer - Kafka: %s, Topic: %s",
        Config.KAFKA_BOOTSTRAP_SERVERS,
        Config.KAFKA_TOPIC,
    )

    events_produced = 0
    
    try:
        while not shutdown_event.is_set():
            event = generator.generate()
            publisher.publish(event)
            events_produced += 1
            
            if events_produced % 100 == 0:
                logger.info("Produced %d events", events_produced)
            
            interval_ms = random.randint(Config.MIN_INTERVAL_MS, Config.MAX_INTERVAL_MS)
            time.sleep(interval_ms / 1000.0)
            
    except Exception as e:
        logger.exception("Fatal error in producer: %s", e)
        sys.exit(1)
    finally:
        logger.info("Flushing remaining messages")
        publisher.flush()
        logger.info("Producer shutdown complete. Total events produced: %d", events_produced)


if __name__ == "__main__":
    main()

