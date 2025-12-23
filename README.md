## Architecture

<img width="1037" height="398" alt="image" src="https://github.com/user-attachments/assets/12689f81-a4bb-49d0-9927-ff65246694fa" />


## Prerequisites

- Docker and Docker Compose

## Setup

Start all services:

```bash
docker compose up -d
```

Wait for services to be healthy (about 30 seconds), then verify:

```bash
docker compose ps
```

## Running Simulations

### Default Producer

The default producer generates events at irregular intervals automatically when you start the services.

### Bulk Producer (High Volume)

Generate a large number of events quickly:

```bash
# Generate 100,000 events
docker compose --profile bulk run --rm bulk-producer

# Custom count
docker compose --profile bulk run --rm -e EVENT_COUNT=500000 bulk-producer
```

### Simulation Producer (Continuous)

Generate events continuously at a steady rate:

```bash
# Run for 1 hour generating ~208 events/minute (5M events/day rate)
docker compose --profile simulation run --rm simulation-producer

# Custom duration and event count
docker compose --profile simulation run --rm \
  -e SIMULATION_DURATION_HOURS=24 \
  -e TARGET_EVENT_COUNT=5000000 \
  simulation-producer
```

## Observing Data

### Grafana Dashboards

Open http://localhost:3000

- Username: `admin`
- Password: `admin`

Navigate to Dashboards > Pipeline to view metrics.

### Prometheus Metrics

Open http://localhost:9090

Example queries:
- `events_produced_total` - Total events produced
- `events_consumed_total` - Total events consumed
- `aggregation_windows_flushed_total` - Windows written to database

### Database (pgAdmin)

Open http://localhost:5050

- Email: `admin@example.com`
- Password: `admin123`

Connect to the pre-configured server and query the `hourly_aggregations` table.

### Direct Database Query

```bash
docker compose exec postgres psql -U pipeline -d pipeline -c \
  "SELECT event_type, SUM(event_count) as total FROM hourly_aggregations GROUP BY event_type;"
```

## Using the API

Base URL: http://localhost:8080

### Health Check

```bash
curl http://localhost:8080/health
```

### Get Aggregations

```bash
# All aggregations
curl http://localhost:8080/api/aggregations

# Filter by event type
curl "http://localhost:8080/api/aggregations?event_type=page_view"

# With time range
curl "http://localhost:8080/api/aggregations?start=2024-01-01T00:00:00&end=2024-01-02T00:00:00"
```

### Get Latest Aggregations

```bash
curl http://localhost:8080/api/aggregations/latest
```

### Get Statistics

```bash
curl http://localhost:8080/api/stats
```

### Get Event Types

```bash
curl http://localhost:8080/api/event-types
```

### Pipeline Memory Status

View events currently in memory and next flush time:

```bash
curl http://localhost:8080/api/pipeline/memory
```

### Manual Flush

Force write in-memory events to database:

```bash
curl -X POST http://localhost:8080/api/pipeline/flush
```

## Running Tests

```bash
docker compose --profile test run --rm test-runner
```

## Stopping Services

```bash
docker compose down
```

To remove all data volumes:

```bash
docker compose down -v
```

## Configuration

Key environment variables (set in docker-compose.yml):

| Variable | Default | Description |
|----------|---------|-------------|
| KAFKA_BOOTSTRAP_SERVERS | kafka:29092 | Kafka broker address |
| POSTGRES_HOST | postgres | Database host |
| FLUSH_INTERVAL_SECONDS | 60 | How often to write to database |
| GRACE_PERIOD_SECONDS | 30 | Wait time after window closes |
