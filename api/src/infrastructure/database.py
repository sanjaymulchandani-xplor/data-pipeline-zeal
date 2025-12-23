import logging
from typing import Optional, List, Dict, Any

import psycopg2
from psycopg2.extras import RealDictCursor


logger = logging.getLogger(__name__)


class Database:
    _connection: Optional[psycopg2.extensions.connection] = None
    _connection_string: Optional[str] = None

    @classmethod
    def initialize(cls, connection_string: str) -> None:
        cls._connection_string = connection_string
        cls._get_connection()
        logger.info("Database connection initialized")

    @classmethod
    def _get_connection(cls) -> psycopg2.extensions.connection:
        if cls._connection is None or cls._connection.closed:
            cls._connection = psycopg2.connect(cls._connection_string)
        return cls._connection

    @classmethod
    def query(cls, sql: str, params: tuple = None) -> List[Dict[str, Any]]:
        conn = cls._get_connection()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(sql, params)
                return [dict(row) for row in cursor.fetchall()]
        except psycopg2.Error as e:
            logger.error("Database query error: %s", e)
            conn.rollback()
            raise

    @classmethod
    def close(cls) -> None:
        if cls._connection and not cls._connection.closed:
            cls._connection.close()
            logger.info("Database connection closed")

