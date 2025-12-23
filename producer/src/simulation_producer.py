import argparse
import logging
import signal
import sys
import time
import json
from threading import Event

from confluent_kafka import Producer

from config import Config
from domain.event_generator import EventGenerator
from utils.domain.events import UserActivityEvent


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


shutdown_event = Event()


def signal_handler(signum, frame):
    # Handle shutdown signals
    logger.info("Shutdown signal received")
    shutdown_event.set()


class SimulationProducer:
    """Producer that generates events at a configurable rate for simulation."""

    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        events_per_second: float,
    ):
        self._topic = topic
        self._events_per_second = events_per_second
        self._interval = 1.0 / events_per_second if events_per_second > 0 else 1.0
        self._producer = Producer({
            "bootstrap.servers": bootstrap_servers,
            "acks": "1",
            "linger.ms": 10,
            "batch.size": 32768,
            "queue.buffering.max.messages": 100000,
        })
        self._delivered = 0
        self._errors = 0

    def _delivery_callback(self, err, msg):
        if err is not None:
            self._errors += 1
        else:
            self._delivered += 1

    def produce(self, event: UserActivityEvent) -> None:
        # Produce a single event to Kafka
        payload = json.dumps(event.to_dict()).encode("utf-8")
        self._producer.produce(
            self._topic,
            key=event.user_id.encode("utf-8"),
            value=payload,
            callback=self._delivery_callback,
        )
        self._producer.poll(0)

    def flush(self) -> tuple[int, int]:
        # Flush pending messages and return counts
        self._producer.flush(timeout=30)
        return self._delivered, self._errors

    @property
    def interval(self) -> float:
        return self._interval


def calculate_events_per_second(total_events: int, hours: int) -> float:
    # Calculate required events per second to reach total in given hours
    total_seconds = hours * 3600
    return total_events / total_seconds


def main():
    parser = argparse.ArgumentParser(description="Simulation event producer")
    parser.add_argument(
        "--target-events",
        type=int,
        default=5000000,
        help="Target total events (default: 5000000)",
    )
    parser.add_argument(
        "--hours",
        type=int,
        default=24,
        help="Hours to reach target (default: 24)",
    )
    parser.add_argument(
        "--events-per-second",
        type=float,
        default=0,
        help="Override: specific events per second (0 = calculate from target)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=10,
        help="Events to generate per batch (default: 10)",
    )
    args = parser.parse_args()

    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    if args.events_per_second > 0:
        events_per_second = args.events_per_second
    else:
        events_per_second = calculate_events_per_second(args.target_events, args.hours)

    logger.info("=" * 60)
    logger.info("SIMULATION PRODUCER")
    logger.info("=" * 60)
    logger.info("Target: %d events in %d hours", args.target_events, args.hours)
    logger.info("Rate: %.2f events/second", events_per_second)
    logger.info("Batch size: %d events", args.batch_size)
    logger.info("Kafka: %s, Topic: %s", Config.KAFKA_BOOTSTRAP_SERVERS, Config.KAFKA_TOPIC)
    logger.info("=" * 60)

    producer = SimulationProducer(
        bootstrap_servers=Config.KAFKA_BOOTSTRAP_SERVERS,
        topic=Config.KAFKA_TOPIC,
        events_per_second=events_per_second,
    )

    generator = EventGenerator(user_pool_size=Config.USER_POOL_SIZE)

    batch_interval = args.batch_size / events_per_second
    
    start_time = time.time()
    total_produced = 0
    last_log_time = start_time

    try:
        while not shutdown_event.is_set():
            batch_start = time.time()
            
            for _ in range(args.batch_size):
                event = generator.generate()
                producer.produce(event)
                total_produced += 1

            current_time = time.time()
            if current_time - last_log_time >= 60:
                elapsed_hours = (current_time - start_time) / 3600
                current_rate = total_produced / (current_time - start_time)
                projected_total = current_rate * args.hours * 3600
                
                logger.info(
                    "Progress: %d events | Rate: %.1f/sec | Elapsed: %.2f hrs | Projected 24h: %.0f",
                    total_produced,
                    current_rate,
                    elapsed_hours,
                    projected_total,
                )
                last_log_time = current_time

            elapsed = time.time() - batch_start
            sleep_time = max(0, batch_interval - elapsed)
            if sleep_time > 0:
                time.sleep(sleep_time)

    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    except Exception as e:
        logger.exception("Error during simulation: %s", e)
        sys.exit(1)

    logger.info("Flushing remaining messages...")
    delivered, errors = producer.flush()

    end_time = time.time()
    elapsed = end_time - start_time
    elapsed_hours = elapsed / 3600

    logger.info("=" * 60)
    logger.info("SIMULATION COMPLETE")
    logger.info("=" * 60)
    logger.info("Total events produced: %d", total_produced)
    logger.info("Delivered: %d, Errors: %d", delivered, errors)
    logger.info("Total time: %.2f hours (%.0f seconds)", elapsed_hours, elapsed)
    logger.info("Actual rate: %.2f events/second", total_produced / elapsed if elapsed > 0 else 0)
    logger.info("=" * 60)


if __name__ == "__main__":
    main()

