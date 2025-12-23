import logging
from typing import Optional, List, Dict, Any

import psycopg2
from psycopg2 import pool
from psycopg2.extras import RealDictCursor


logger = logging.getLogger(__name__)


class DatabaseConnectionError(Exception):
    """Raised when database connection fails."""

    pass


class DatabaseQueryError(Exception):
    """Raised when a database query fails."""

    pass


class Database:
    _pool: Optional[pool.ThreadedConnectionPool] = None
    _connection_string: Optional[str] = None

    @classmethod
    def initialize(
        cls,
        connection_string: str,
        min_connections: int = 2,
        max_connections: int = 10,
    ) -> None:
        """Initialize the connection pool.

        Args:
            connection_string: PostgreSQL connection string
            min_connections: Minimum connections to keep in pool
            max_connections: Maximum connections allowed in pool
        """
        cls._connection_string = connection_string
        try:
            cls._pool = pool.ThreadedConnectionPool(
                min_connections,
                max_connections,
                connection_string,
            )
            logger.info(
                "Database connection pool initialized (min=%d, max=%d)",
                min_connections,
                max_connections,
            )
        except psycopg2.Error as e:
            logger.error("Failed to initialize database pool: %s", e)
            raise DatabaseConnectionError(f"Failed to connect to database: {e}")

    @classmethod
    def _get_connection(cls) -> psycopg2.extensions.connection:
        """Get a connection from the pool."""
        if cls._pool is None:
            raise DatabaseConnectionError("Database pool not initialized")
        try:
            conn = cls._pool.getconn()
            if conn is None:
                raise DatabaseConnectionError("Failed to get connection from pool")
            return conn
        except psycopg2.Error as e:
            logger.error("Failed to get connection from pool: %s", e)
            raise DatabaseConnectionError(f"Failed to get database connection: {e}")

    @classmethod
    def _return_connection(cls, conn: psycopg2.extensions.connection) -> None:
        """Return a connection to the pool."""
        if cls._pool is not None and conn is not None:
            cls._pool.putconn(conn)

    @classmethod
    def query(cls, sql: str, params: Optional[tuple] = None) -> List[Dict[str, Any]]:
        """Execute a query and return results.

        Args:
            sql: SQL query string
            params: Query parameters

        Returns:
            List of dictionaries representing rows

        Raises:
            DatabaseConnectionError: If connection fails
            DatabaseQueryError: If query execution fails
        """
        conn = cls._get_connection()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(sql, params)
                return [dict(row) for row in cursor.fetchall()]
        except psycopg2.OperationalError as e:
            logger.error("Database connection error during query: %s", e)
            conn.rollback()
            raise DatabaseConnectionError(f"Database connection error: {e}")
        except psycopg2.Error as e:
            logger.error("Database query error: %s", e)
            conn.rollback()
            raise DatabaseQueryError(f"Database query failed: {e}")
        finally:
            cls._return_connection(conn)

    @classmethod
    def close(cls) -> None:
        """Close all connections in the pool."""
        if cls._pool is not None:
            cls._pool.closeall()
            cls._pool = None
            logger.info("Database connection pool closed")
