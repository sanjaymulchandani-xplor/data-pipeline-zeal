import argparse
import logging
import signal
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Event, Lock

from confluent_kafka import Producer

from config import Config
from domain.event_generator import EventGenerator
from domain.events import UserActivityEvent

import json


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


shutdown_event = Event()
produced_count = 0
produced_lock = Lock()


def signal_handler(signum, frame):
    logger.info("Shutdown signal received")
    shutdown_event.set()


class BulkProducer:
    def __init__(self, bootstrap_servers: str, topic: str, batch_size: int = 10000):
        self._topic = topic
        self._batch_size = batch_size
        self._producer = Producer({
            "bootstrap.servers": bootstrap_servers,
            "acks": "1",
            "linger.ms": 50,
            "batch.size": 65536,
            "queue.buffering.max.messages": 1000000,
            "queue.buffering.max.kbytes": 1048576,
            "compression.type": "lz4",
        })
        self._delivered = 0
        self._errors = 0

    def _delivery_callback(self, err, msg):
        if err is not None:
            self._errors += 1
        else:
            self._delivered += 1

    def produce_batch(self, events: list[UserActivityEvent]) -> int:
        for event in events:
            if shutdown_event.is_set():
                break
            payload = json.dumps(event.to_dict()).encode("utf-8")
            self._producer.produce(
                self._topic,
                key=event.user_id.encode("utf-8"),
                value=payload,
                callback=self._delivery_callback,
            )
        self._producer.poll(0)
        return len(events)

    def flush(self) -> tuple[int, int]:
        self._producer.flush(timeout=60)
        return self._delivered, self._errors


def generate_events_batch(generator: EventGenerator, count: int) -> list[UserActivityEvent]:
    return list(generator.generate_batch(count))


def main():
    parser = argparse.ArgumentParser(description="Bulk event producer")
    parser.add_argument(
        "--count",
        type=int,
        default=1000000,
        help="Number of events to produce (default: 1000000)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=10000,
        help="Batch size for production (default: 10000)",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=4,
        help="Number of generator workers (default: 4)",
    )
    args = parser.parse_args()

    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    total_events = args.count
    batch_size = args.batch_size
    num_workers = args.workers

    logger.info(
        "Starting bulk producer - Target: %d events, Batch size: %d, Workers: %d",
        total_events,
        batch_size,
        num_workers,
    )
    logger.info("Kafka: %s, Topic: %s", Config.KAFKA_BOOTSTRAP_SERVERS, Config.KAFKA_TOPIC)

    producer = BulkProducer(
        bootstrap_servers=Config.KAFKA_BOOTSTRAP_SERVERS,
        topic=Config.KAFKA_TOPIC,
        batch_size=batch_size,
    )

    generators = [EventGenerator(user_pool_size=Config.USER_POOL_SIZE) for _ in range(num_workers)]

    start_time = time.time()
    total_produced = 0
    batches_completed = 0
    total_batches = (total_events + batch_size - 1) // batch_size

    logger.info("Generating and producing %d batches...", total_batches)

    try:
        with ThreadPoolExecutor(max_workers=num_workers) as executor:
            batch_idx = 0
            pending_futures = []

            while total_produced < total_events and not shutdown_event.is_set():
                remaining = total_events - total_produced
                current_batch_size = min(batch_size, remaining)

                generator = generators[batch_idx % num_workers]
                events = generate_events_batch(generator, current_batch_size)
                
                produced = producer.produce_batch(events)
                total_produced += produced
                batches_completed += 1
                batch_idx += 1

                if batches_completed % 10 == 0:
                    elapsed = time.time() - start_time
                    rate = total_produced / elapsed if elapsed > 0 else 0
                    progress = (total_produced / total_events) * 100
                    logger.info(
                        "Progress: %d/%d (%.1f%%) - Rate: %.0f events/sec",
                        total_produced,
                        total_events,
                        progress,
                        rate,
                    )

    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    except Exception as e:
        logger.exception("Error during bulk production: %s", e)
        sys.exit(1)

    logger.info("Flushing remaining messages...")
    delivered, errors = producer.flush()

    end_time = time.time()
    elapsed = end_time - start_time

    logger.info("=" * 60)
    logger.info("Bulk production complete!")
    logger.info("Total events produced: %d", total_produced)
    logger.info("Delivered: %d, Errors: %d", delivered, errors)
    logger.info("Total time: %.2f seconds", elapsed)
    logger.info("Average rate: %.0f events/second", total_produced / elapsed if elapsed > 0 else 0)
    logger.info("=" * 60)


if __name__ == "__main__":
    main()

