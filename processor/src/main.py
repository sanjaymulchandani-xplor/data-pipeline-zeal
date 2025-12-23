import logging
import signal
import sys
import time
from threading import Event

from prometheus_client import start_http_server

from admin_server import AdminServer
from application.aggregation_service import AggregationService
from config import Config
from infrastructure.kafka_consumer import KafkaEventConsumer
from infrastructure.postgres_repository import PostgresAggregationRepository


logging.basicConfig(
    level=getattr(logging, Config.LOG_LEVEL),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


shutdown_event = Event()


def signal_handler(signum, frame):
    # Handle shutdown signals by setting the shutdown event
    logger.info("Shutdown signal received")
    shutdown_event.set()


def main():
    # Entry point for the stream processor service
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    logger.info("Starting metrics server on port %d", Config.METRICS_PORT)
    start_http_server(Config.METRICS_PORT)

    admin_server = AdminServer(Config.ADMIN_PORT)

    consumer = KafkaEventConsumer(
        bootstrap_servers=Config.KAFKA_BOOTSTRAP_SERVERS,
        topic=Config.KAFKA_TOPIC,
        group_id=Config.KAFKA_GROUP_ID,
        auto_offset_reset=Config.KAFKA_AUTO_OFFSET_RESET,
    )
    
    repository = PostgresAggregationRepository(Config.postgres_connection_string())
    
    service = AggregationService(
        consumer=consumer,
        repository=repository,
        flush_interval_seconds=Config.FLUSH_INTERVAL_SECONDS,
        grace_period_seconds=Config.GRACE_PERIOD_SECONDS,
    )
    
    admin_server.set_flush_callback(service.manual_flush)
    admin_server.set_status_callback(service.get_status)
    admin_server.start()
    
    logger.info(
        "Starting stream processor - Kafka: %s, Topic: %s, Group: %s",
        Config.KAFKA_BOOTSTRAP_SERVERS,
        Config.KAFKA_TOPIC,
        Config.KAFKA_GROUP_ID,
    )

    total_events = 0
    last_lag_update = time.time()
    
    try:
        while not shutdown_event.is_set():
            events_processed = service.process_batch(
                max_messages=Config.BATCH_SIZE,
                timeout_seconds=Config.CONSUME_TIMEOUT_SECONDS,
            )
            total_events += events_processed
            
            service.maybe_flush()
            
            if time.time() - last_lag_update > Config.LAG_UPDATE_INTERVAL_SECONDS:
                consumer.update_lag_metrics()
                last_lag_update = time.time()
                
    except Exception as e:
        logger.exception("Fatal error in processor: %s", e)
        sys.exit(1)
    finally:
        logger.info("Flushing remaining aggregations")
        service.flush_all()
        admin_server.stop()
        consumer.close()
        repository.close()
        logger.info("Processor shutdown complete. Total events processed: %d", total_events)


if __name__ == "__main__":
    main()

