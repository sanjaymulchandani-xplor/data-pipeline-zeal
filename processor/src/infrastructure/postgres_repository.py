import logging
import os
from abc import ABC, abstractmethod
from typing import List

import psycopg2
from psycopg2.extras import execute_values
from prometheus_client import Counter, Histogram

from domain.aggregation import AggregationRecord
from utils.infrastructure.query_loader import load_query

QUERIES_DIR = os.path.join(os.path.dirname(__file__), "queries")


logger = logging.getLogger(__name__)


RECORDS_WRITTEN = Counter(
    "processor_records_written_total",
    "Total number of aggregation records written to database",
)

WRITE_LATENCY = Histogram(
    "processor_db_write_latency_seconds",
    "Time spent writing to database",
    buckets=[0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5],
)

WRITE_ERRORS = Counter(
    "processor_db_write_errors_total",
    "Total number of database write errors",
)


class AggregationRepository(ABC):
    """Abstract base class defining the interface for aggregation data persistence."""

    @abstractmethod
    def save_batch(self, records: List[AggregationRecord]) -> None:
        # Save a batch of aggregation records
        pass


class PostgresAggregationRepository(AggregationRepository):
    """PostgreSQL implementation of the aggregation repository."""

    def __init__(self, connection_string: str):
        # Initialize the repository with a database connection string
        self._connection_string = connection_string
        self._conn = None

    def _get_connection(self):
        # Get or create a database connection
        if self._conn is None or self._conn.closed:
            self._conn = psycopg2.connect(self._connection_string)
        return self._conn

    def save_batch(self, records: List[AggregationRecord]) -> None:
        # Save aggregation records to PostgreSQL using upsert for idempotency
        if not records:
            return
        
        with WRITE_LATENCY.time():
            try:
                conn = self._get_connection()
                with conn.cursor() as cursor:
                    values = [
                        (
                            record.window_start,
                            record.window_end,
                            record.event_type,
                            record.event_count,
                            record.unique_user_count,
                            record.unique_session_count,
                            record.total_duration_ms,
                            record.avg_duration_ms,
                            record.created_at,
                        )
                        for record in records
                    ]
                    
                    execute_values(
                        cursor,
                        load_query(QUERIES_DIR, "upsert_aggregation"),
                        values,
                    )
                    conn.commit()
                
                RECORDS_WRITTEN.inc(len(records))
                logger.info("Saved %d aggregation records", len(records))
                
            except psycopg2.Error as e:
                WRITE_ERRORS.inc()
                logger.error("Database error: %s", e)
                if self._conn:
                    self._conn.rollback()
                raise

    def close(self) -> None:
        # Close the database connection if open
        if self._conn and not self._conn.closed:
            self._conn.close()

