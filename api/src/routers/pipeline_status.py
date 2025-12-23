from datetime import datetime, timezone
from typing import List, Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
import httpx

from config import Config


router = APIRouter()


class WindowStatus(BaseModel):
    event_type: str
    events_in_memory: int


class PipelineMemoryStatus(BaseModel):
    timestamp: datetime
    total_events_in_memory: int
    active_windows: int
    events_by_type: List[WindowStatus]
    seconds_until_next_flush: float
    earliest_window_end: Optional[datetime]
    next_possible_db_write: Optional[datetime]


def parse_prometheus_metric(
    lines: List[str], metric_name: str, labels: dict = None
) -> Optional[float]:
    # Parse a single metric value from Prometheus text format
    for line in lines:
        if line.startswith("#"):
            continue
        if metric_name in line:
            if labels:
                label_match = all(f'{k}="{v}"' in line for k, v in labels.items())
                if not label_match:
                    continue
            parts = line.split()
            if len(parts) >= 2:
                try:
                    return float(parts[-1])
                except ValueError:
                    continue
    return None


def parse_prometheus_metrics_with_labels(lines: List[str], metric_name: str) -> dict:
    # Parse metrics with labels, returning dict of label_value -> metric_value
    results = {}
    for line in lines:
        if line.startswith("#"):
            continue
        if metric_name + "{" in line:
            try:
                label_part = line.split("{")[1].split("}")[0]
                value_part = line.split()[-1]
                label_value = label_part.split('="')[1].rstrip('"')
                results[label_value] = float(value_part)
            except (IndexError, ValueError):
                continue
    return results


PROCESSOR_ADMIN_URL = "http://processor:8002"


@router.get("/memory", response_model=PipelineMemoryStatus)
async def get_memory_status():
    # Fetch current in-memory state from processor metrics
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            response = await client.get(Config.PROCESSOR_METRICS_URL)
            response.raise_for_status()
    except httpx.RequestError as e:
        raise HTTPException(
            status_code=503, detail=f"Failed to reach processor: {str(e)}"
        )

    lines = response.text.split("\n")

    total_events = (
        parse_prometheus_metric(lines, "processor_events_in_memory_total") or 0
    )
    active_windows = parse_prometheus_metric(lines, "processor_active_windows") or 0
    seconds_until_flush = (
        parse_prometheus_metric(lines, "processor_seconds_until_next_flush") or 0
    )
    earliest_end_ts = parse_prometheus_metric(
        lines, "processor_earliest_window_end_timestamp"
    )

    events_by_type_raw = parse_prometheus_metrics_with_labels(
        lines, "processor_events_in_memory_by_type"
    )
    events_by_type = [
        WindowStatus(event_type=et, events_in_memory=int(count))
        for et, count in sorted(events_by_type_raw.items(), key=lambda x: -x[1])
    ]

    earliest_window_end = None
    next_possible_db_write = None

    if earliest_end_ts and earliest_end_ts > 0:
        earliest_window_end = datetime.fromtimestamp(earliest_end_ts)
        grace_period = 60
        next_possible_db_write = datetime.fromtimestamp(earliest_end_ts + grace_period)

    return PipelineMemoryStatus(
        timestamp=datetime.now(timezone.utc),
        total_events_in_memory=int(total_events),
        active_windows=int(active_windows),
        events_by_type=events_by_type,
        seconds_until_next_flush=seconds_until_flush,
        earliest_window_end=earliest_window_end,
        next_possible_db_write=next_possible_db_write,
    )


class FlushResponse(BaseModel):
    success: bool
    windows_flushed: int
    events_written: int
    message: str


@router.post("/flush", response_model=FlushResponse)
async def flush_to_database():
    # Manually flush all in-memory events to database
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(f"{PROCESSOR_ADMIN_URL}/admin/flush")
            response.raise_for_status()
            return FlushResponse(**response.json())
    except httpx.RequestError as e:
        raise HTTPException(
            status_code=503, detail=f"Failed to reach processor: {str(e)}"
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
