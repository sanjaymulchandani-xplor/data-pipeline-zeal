SELECT 
    event_type,
    SUM(event_count) as total_events,
    SUM(unique_user_count) as total_unique_users,
    SUM(unique_session_count) as total_unique_sessions,
    AVG(event_count) as avg_events_per_hour,
    COUNT(*) as window_count
FROM hourly_aggregations
{where_clause}
GROUP BY event_type
ORDER BY total_events DESC

