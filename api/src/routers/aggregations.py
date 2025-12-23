import os
import logging
from typing import List, Optional
from datetime import datetime, timezone

from fastapi import APIRouter, Query, HTTPException
from pydantic import BaseModel

from infrastructure.database import (
    Database,
    DatabaseConnectionError,
    DatabaseQueryError,
)
from utils.infrastructure.query_loader import load_query

QUERIES_DIR = os.path.join(os.path.dirname(__file__), "..", "infrastructure", "queries")

logger = logging.getLogger(__name__)

router = APIRouter()


class AggregationRecord(BaseModel):
    id: int
    window_start: datetime
    window_end: datetime
    event_type: str
    event_count: int
    unique_user_count: int
    unique_session_count: int
    total_duration_ms: int
    avg_duration_ms: Optional[float]
    created_at: datetime


class AggregationsResponse(BaseModel):
    timestamp: datetime
    count: int
    records: List[AggregationRecord]


class AggregationStats(BaseModel):
    event_type: str
    total_events: int
    total_unique_users: int
    total_unique_sessions: int
    avg_events_per_hour: float
    window_count: int


class StatsResponse(BaseModel):
    timestamp: datetime
    time_range_start: Optional[datetime]
    time_range_end: Optional[datetime]
    stats: List[AggregationStats]


@router.get("", response_model=AggregationsResponse)
async def get_aggregations(
    event_type: Optional[str] = Query(None, description="Filter by event type"),
    from_time: Optional[datetime] = Query(None, description="Filter from this time"),
    to_time: Optional[datetime] = Query(None, description="Filter to this time"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum records to return"),
    offset: int = Query(0, ge=0, description="Offset for pagination"),
):
    conditions = []
    params = []

    if event_type:
        conditions.append("event_type = %s")
        params.append(event_type)

    if from_time:
        conditions.append("window_start >= %s")
        params.append(from_time)

    if to_time:
        conditions.append("window_end <= %s")
        params.append(to_time)

    where_clause = ""
    if conditions:
        where_clause = "WHERE " + " AND ".join(conditions)

    sql = load_query(QUERIES_DIR, "get_aggregations").format(where_clause=where_clause)
    params.extend([limit, offset])

    try:
        rows = Database.query(sql, tuple(params))
        records = [AggregationRecord(**row) for row in rows]

        return AggregationsResponse(
            timestamp=datetime.now(timezone.utc),
            count=len(records),
            records=records,
        )
    except DatabaseConnectionError as e:
        logger.error("Database connection error: %s", e)
        raise HTTPException(
            status_code=503,
            detail="Database temporarily unavailable. Please try again later.",
        )
    except DatabaseQueryError as e:
        logger.error("Database query error: %s", e)
        raise HTTPException(status_code=500, detail="Failed to query aggregations.")
    except Exception as e:
        logger.exception("Unexpected error in get_aggregations: %s", e)
        raise HTTPException(status_code=500, detail="Internal server error.")


@router.get("/latest", response_model=AggregationsResponse)
async def get_latest_aggregations(
    limit: int = Query(10, ge=1, le=100, description="Number of latest records"),
):
    sql = load_query(QUERIES_DIR, "get_latest_aggregations")

    try:
        rows = Database.query(sql, (limit,))
        records = [AggregationRecord(**row) for row in rows]

        return AggregationsResponse(
            timestamp=datetime.now(timezone.utc),
            count=len(records),
            records=records,
        )
    except DatabaseConnectionError as e:
        logger.error("Database connection error: %s", e)
        raise HTTPException(
            status_code=503,
            detail="Database temporarily unavailable. Please try again later.",
        )
    except DatabaseQueryError as e:
        logger.error("Database query error: %s", e)
        raise HTTPException(
            status_code=500, detail="Failed to query latest aggregations."
        )
    except Exception as e:
        logger.exception("Unexpected error in get_latest_aggregations: %s", e)
        raise HTTPException(status_code=500, detail="Internal server error.")


@router.get("/stats", response_model=StatsResponse)
async def get_aggregation_stats(
    from_time: Optional[datetime] = Query(None, description="Filter from this time"),
    to_time: Optional[datetime] = Query(None, description="Filter to this time"),
):
    conditions = []
    params = []

    if from_time:
        conditions.append("window_start >= %s")
        params.append(from_time)

    if to_time:
        conditions.append("window_end <= %s")
        params.append(to_time)

    where_clause = ""
    if conditions:
        where_clause = "WHERE " + " AND ".join(conditions)

    sql = load_query(QUERIES_DIR, "get_stats").format(where_clause=where_clause)

    try:
        rows = Database.query(sql, tuple(params) if params else None)
        stats = [
            AggregationStats(
                event_type=row["event_type"],
                total_events=int(row["total_events"]),
                total_unique_users=int(row["total_unique_users"]),
                total_unique_sessions=int(row["total_unique_sessions"]),
                avg_events_per_hour=float(row["avg_events_per_hour"]),
                window_count=int(row["window_count"]),
            )
            for row in rows
        ]

        return StatsResponse(
            timestamp=datetime.now(timezone.utc),
            time_range_start=from_time,
            time_range_end=to_time,
            stats=stats,
        )
    except DatabaseConnectionError as e:
        logger.error("Database connection error: %s", e)
        raise HTTPException(
            status_code=503,
            detail="Database temporarily unavailable. Please try again later.",
        )
    except DatabaseQueryError as e:
        logger.error("Database query error: %s", e)
        raise HTTPException(status_code=500, detail="Failed to query stats.")
    except Exception as e:
        logger.exception("Unexpected error in get_aggregation_stats: %s", e)
        raise HTTPException(status_code=500, detail="Internal server error.")


@router.get("/event-types")
async def get_event_types():
    sql = load_query(QUERIES_DIR, "get_event_types")

    try:
        rows = Database.query(sql)
        return {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "event_types": [row["event_type"] for row in rows],
        }
    except DatabaseConnectionError as e:
        logger.error("Database connection error: %s", e)
        raise HTTPException(
            status_code=503,
            detail="Database temporarily unavailable. Please try again later.",
        )
    except DatabaseQueryError as e:
        logger.error("Database query error: %s", e)
        raise HTTPException(status_code=500, detail="Failed to query event types.")
    except Exception as e:
        logger.exception("Unexpected error in get_event_types: %s", e)
        raise HTTPException(status_code=500, detail="Internal server error.")
