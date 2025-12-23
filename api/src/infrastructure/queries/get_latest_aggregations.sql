SELECT id, window_start, window_end, event_type, event_count,
       unique_user_count, unique_session_count, total_duration_ms,
       avg_duration_ms, created_at
FROM hourly_aggregations
ORDER BY created_at DESC
LIMIT %s

