INSERT INTO hourly_aggregations (
    window_start,
    window_end,
    event_type,
    event_count,
    unique_user_count,
    unique_session_count,
    total_duration_ms,
    avg_duration_ms,
    created_at
) VALUES %s
ON CONFLICT (window_start, event_type)
DO UPDATE SET
    event_count = EXCLUDED.event_count,
    unique_user_count = EXCLUDED.unique_user_count,
    unique_session_count = EXCLUDED.unique_session_count,
    total_duration_ms = EXCLUDED.total_duration_ms,
    avg_duration_ms = EXCLUDED.avg_duration_ms,
    created_at = EXCLUDED.created_at

