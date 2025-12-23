import logging
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Iterator

from prometheus_client import Counter, Histogram, Gauge

from domain.aggregation import AggregationEngine, AggregationRecord
from utils.domain.events import UserActivityEvent


logger = logging.getLogger(__name__)


PROCESSING_LATENCY = Histogram(
    "processor_processing_latency_seconds",
    "Time spent processing a batch of events",
    buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0],
)

WINDOWS_FLUSHED = Counter(
    "processor_windows_flushed_total",
    "Total number of aggregation windows flushed",
)

ACTIVE_WINDOWS = Gauge(
    "processor_active_windows",
    "Number of currently active aggregation windows",
)

EVENTS_IN_MEMORY = Gauge(
    "processor_events_in_memory_total",
    "Total number of events currently held in memory",
)

EVENTS_IN_MEMORY_BY_TYPE = Gauge(
    "processor_events_in_memory_by_type",
    "Events in memory per event type",
    ["event_type"],
)

SECONDS_UNTIL_FLUSH = Gauge(
    "processor_seconds_until_next_flush",
    "Seconds until next scheduled flush check",
)

EARLIEST_WINDOW_END = Gauge(
    "processor_earliest_window_end_timestamp",
    "Unix timestamp of the earliest window end time",
)


class EventConsumer(ABC):
    """Abstract base class defining the interface for event consumption."""

    @abstractmethod
    def consume_batch(
        self,
        max_messages: int,
        timeout_seconds: float,
    ) -> Iterator[UserActivityEvent]:
        # Consume a batch of events from the message broker
        pass

    @abstractmethod
    def commit(self) -> None:
        # Commit consumed message offsets
        pass


class AggregationRepository(ABC):
    """Abstract base class defining the interface for aggregation persistence."""

    @abstractmethod
    def save_batch(self, records: list[AggregationRecord]) -> None:
        # Save a batch of aggregation records to storage
        pass


class AggregationService:
    """Service that orchestrates event consumption, aggregation, and persistence."""

    def __init__(
        self,
        consumer: EventConsumer,
        repository: AggregationRepository,
        flush_interval_seconds: int = 60,
        grace_period_seconds: int = 60,
    ):
        # Initialize the aggregation service with consumer and repository dependencies
        self._consumer = consumer
        self._repository = repository
        self._engine = AggregationEngine()
        self._flush_interval_seconds = flush_interval_seconds
        self._grace_period_seconds = grace_period_seconds
        self._last_flush = datetime.now(timezone.utc)

    def process_batch(
        self, max_messages: int = 100, timeout_seconds: float = 1.0
    ) -> int:
        # Consume and aggregate a batch of events, returning the count processed
        events_processed = 0

        with PROCESSING_LATENCY.time():
            for event in self._consumer.consume_batch(max_messages, timeout_seconds):
                self._engine.process_event(event)
                events_processed += 1

        self._update_memory_metrics()

        return events_processed

    def _update_memory_metrics(self) -> None:
        # Update Prometheus metrics for in-memory state
        windows = self._engine.get_all_windows()
        ACTIVE_WINDOWS.set(len(windows))

        total_events = 0
        earliest_end = None

        for window in windows:
            total_events += window.event_count
            EVENTS_IN_MEMORY_BY_TYPE.labels(event_type=window.event_type).set(
                window.event_count
            )
            if earliest_end is None or window.window_end < earliest_end:
                earliest_end = window.window_end

        EVENTS_IN_MEMORY.set(total_events)

        if earliest_end:
            EARLIEST_WINDOW_END.set(earliest_end.timestamp())

        elapsed = (datetime.now(timezone.utc) - self._last_flush).total_seconds()
        remaining = max(0, self._flush_interval_seconds - elapsed)
        SECONDS_UNTIL_FLUSH.set(remaining)

    def maybe_flush(self, force: bool = False) -> int:
        # Flush completed aggregation windows to storage if flush interval has passed
        now = datetime.now(timezone.utc)
        elapsed = (now - self._last_flush).total_seconds()

        if not force and elapsed < self._flush_interval_seconds:
            return 0

        completed_windows = self._engine.get_completed_windows(
            now, self._grace_period_seconds
        )

        if not completed_windows:
            return 0

        records = [
            AggregationRecord.from_aggregation(aggregation)
            for aggregation in completed_windows
        ]
        self._repository.save_batch(records)
        self._consumer.commit()

        WINDOWS_FLUSHED.inc(len(records))
        self._last_flush = now

        logger.info("Flushed %d completed windows", len(records))
        return len(records)

    def flush_all(self, clear_windows: bool = True) -> int:
        # Flush all pending aggregation windows to storage
        pending_windows = self._engine.get_all_windows()

        if not pending_windows:
            return 0

        records = [
            AggregationRecord.from_aggregation(aggregation)
            for aggregation in pending_windows
        ]
        self._repository.save_batch(records)
        self._consumer.commit()

        if clear_windows:
            self._engine.clear()

        WINDOWS_FLUSHED.inc(len(records))

        logger.info("Flushed all %d windows", len(records))
        return len(records)

    def get_status(self) -> dict:
        # Get current in-memory status for admin API
        windows = self._engine.get_all_windows()
        total_events = sum(w.event_count for w in windows)

        events_by_type = {w.event_type: w.event_count for w in windows}

        earliest_end = None
        for w in windows:
            if earliest_end is None or w.window_end < earliest_end:
                earliest_end = w.window_end

        elapsed = (datetime.now(timezone.utc) - self._last_flush).total_seconds()
        remaining = max(0, self._flush_interval_seconds - elapsed)

        return {
            "total_events_in_memory": total_events,
            "active_windows": len(windows),
            "events_by_type": events_by_type,
            "seconds_until_next_flush": round(remaining, 1),
            "earliest_window_end": earliest_end.isoformat() if earliest_end else None,
        }

    def manual_flush(self) -> dict:
        # Manually flush all windows to database
        windows = self._engine.get_all_windows()
        total_events = sum(w.event_count for w in windows)
        window_count = len(windows)

        flushed = self.flush_all(clear_windows=True)

        return {
            "success": True,
            "windows_flushed": flushed,
            "events_written": total_events,
            "message": f"Flushed {flushed} windows with {total_events} events to database",
        }
