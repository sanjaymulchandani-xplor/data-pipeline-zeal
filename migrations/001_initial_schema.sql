CREATE TABLE IF NOT EXISTS hourly_aggregations (
    id SERIAL PRIMARY KEY,
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    event_type VARCHAR(50) NOT NULL,
    event_count INTEGER NOT NULL DEFAULT 0,
    unique_user_count INTEGER NOT NULL DEFAULT 0,
    unique_session_count INTEGER NOT NULL DEFAULT 0,
    total_duration_ms BIGINT NOT NULL DEFAULT 0,
    avg_duration_ms DOUBLE PRECISION,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    
    CONSTRAINT uq_window_event_type UNIQUE (window_start, event_type)
);

CREATE INDEX IF NOT EXISTS idx_hourly_aggregations_window_start 
    ON hourly_aggregations (window_start DESC);

CREATE INDEX IF NOT EXISTS idx_hourly_aggregations_event_type 
    ON hourly_aggregations (event_type);

CREATE INDEX IF NOT EXISTS idx_hourly_aggregations_created_at 
    ON hourly_aggregations (created_at DESC);

