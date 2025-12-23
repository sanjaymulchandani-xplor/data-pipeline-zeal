from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Optional

from shared.domain.events import UserActivityEvent


@dataclass
class HourlyAggregation:
    """Data class representing an hourly aggregation window for a specific event type."""

    window_start: datetime
    window_end: datetime
    event_type: str
    event_count: int = 0
    unique_users: set = field(default_factory=set)
    unique_sessions: set = field(default_factory=set)
    total_duration_ms: int = 0
    duration_event_count: int = 0

    @property
    def unique_user_count(self) -> int:
        # Return the count of unique users in this window
        return len(self.unique_users)

    @property
    def unique_session_count(self) -> int:
        # Return the count of unique sessions in this window
        return len(self.unique_sessions)

    @property
    def avg_duration_ms(self) -> Optional[float]:
        # Calculate the average event duration in milliseconds
        if self.duration_event_count == 0:
            return None
        return self.total_duration_ms / self.duration_event_count


class AggregationEngine:
    """Engine that manages time-windowed aggregations for streaming events."""

    def __init__(self):
        # Initialize the aggregation engine with an empty window store
        self._windows: dict[tuple[datetime, str], HourlyAggregation] = {}

    def process_event(self, event: UserActivityEvent) -> None:
        # Process an event by adding it to the appropriate hourly aggregation window
        window_start = event.timestamp.replace(minute=0, second=0, microsecond=0)
        window_end = window_start + timedelta(hours=1)
        
        window_key = (window_start, event.event_type)
        
        if window_key not in self._windows:
            self._windows[window_key] = HourlyAggregation(
                window_start=window_start,
                window_end=window_end,
                event_type=event.event_type,
            )
        
        aggregation = self._windows[window_key]
        aggregation.event_count += 1
        aggregation.unique_users.add(event.user_id)
        aggregation.unique_sessions.add(event.session_id)
        
        if event.duration_ms is not None:
            aggregation.total_duration_ms += event.duration_ms
            aggregation.duration_event_count += 1

    def get_completed_windows(self, current_time: datetime, grace_period_seconds: int = 60) -> list[HourlyAggregation]:
        # Return windows that have closed and remove them from the store
        completed_windows = []
        cutoff_time = current_time - timedelta(seconds=grace_period_seconds)
        
        for window_key, aggregation in list(self._windows.items()):
            if aggregation.window_end <= cutoff_time:
                completed_windows.append(aggregation)
                del self._windows[window_key]
        
        return completed_windows

    def get_all_windows(self) -> list[HourlyAggregation]:
        # Return all active aggregation windows
        return list(self._windows.values())

    def clear(self) -> None:
        # Clear all aggregation windows from the store
        self._windows.clear()


@dataclass(frozen=True)
class AggregationRecord:
    """Immutable record representing a completed aggregation for database storage."""

    window_start: datetime
    window_end: datetime
    event_type: str
    event_count: int
    unique_user_count: int
    unique_session_count: int
    total_duration_ms: int
    avg_duration_ms: Optional[float]
    created_at: datetime

    @staticmethod
    def from_aggregation(hourly_aggregation: HourlyAggregation) -> "AggregationRecord":
        # Create a database record from a completed hourly aggregation
        return AggregationRecord(
            window_start=hourly_aggregation.window_start,
            window_end=hourly_aggregation.window_end,
            event_type=hourly_aggregation.event_type,
            event_count=hourly_aggregation.event_count,
            unique_user_count=hourly_aggregation.unique_user_count,
            unique_session_count=hourly_aggregation.unique_session_count,
            total_duration_ms=hourly_aggregation.total_duration_ms,
            avg_duration_ms=hourly_aggregation.avg_duration_ms,
            created_at=datetime.utcnow(),
        )

