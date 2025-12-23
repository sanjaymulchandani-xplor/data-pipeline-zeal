import os


class PostgresConfig:
    """Shared PostgreSQL configuration."""

    HOST: str = os.getenv("POSTGRES_HOST", "localhost")
    PORT: int = int(os.getenv("POSTGRES_PORT", "5432"))
    DB: str = os.getenv("POSTGRES_DB", "pipeline")
    USER: str = os.getenv("POSTGRES_USER", "pipeline")
    PASSWORD: str = os.getenv("POSTGRES_PASSWORD", "pipeline")

    @classmethod
    def connection_string(cls) -> str:
        # Build psycopg2 connection string from config values
        return (
            f"host={cls.HOST} "
            f"port={cls.PORT} "
            f"dbname={cls.DB} "
            f"user={cls.USER} "
            f"password={cls.PASSWORD}"
        )

