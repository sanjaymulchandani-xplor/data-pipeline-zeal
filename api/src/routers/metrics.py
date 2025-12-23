from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Query
from pydantic import BaseModel

from config import Config
from infrastructure.prometheus_client import PrometheusClient, parse_prometheus_metrics


router = APIRouter()
prometheus = PrometheusClient()


class MetricValue(BaseModel):
    labels: Dict[str, str]
    value: float


class Metric(BaseModel):
    name: str
    help: Optional[str]
    type: Optional[str]
    values: List[MetricValue]


class MetricsResponse(BaseModel):
    timestamp: datetime
    source: str
    metrics: Dict[str, Metric]


class PrometheusQueryResult(BaseModel):
    metric: Dict[str, str]
    value: List[Any]


class PrometheusResponse(BaseModel):
    timestamp: datetime
    query: str
    results: List[PrometheusQueryResult]


@router.get("/producer", response_model=MetricsResponse)
async def get_producer_metrics():
    metrics = await parse_prometheus_metrics(Config.PRODUCER_METRICS_URL)
    return MetricsResponse(
        timestamp=datetime.now(timezone.utc),
        source="producer",
        metrics={
            name: Metric(
                name=name,
                help=data.get("help"),
                type=data.get("type"),
                values=[MetricValue(**v) for v in data.get("values", [])],
            )
            for name, data in metrics.items()
            if not isinstance(data, str)
        },
    )


@router.get("/processor", response_model=MetricsResponse)
async def get_processor_metrics():
    metrics = await parse_prometheus_metrics(Config.PROCESSOR_METRICS_URL)
    return MetricsResponse(
        timestamp=datetime.now(timezone.utc),
        source="processor",
        metrics={
            name: Metric(
                name=name,
                help=data.get("help"),
                type=data.get("type"),
                values=[MetricValue(**v) for v in data.get("values", [])],
            )
            for name, data in metrics.items()
            if not isinstance(data, str)
        },
    )


@router.get("/query", response_model=PrometheusResponse)
async def query_prometheus(
    q: str = Query(..., description="PromQL query"),
):
    results = await prometheus.query(q)
    return PrometheusResponse(
        timestamp=datetime.now(timezone.utc),
        query=q,
        results=[
            PrometheusQueryResult(
                metric=r.get("metric", {}),
                value=r.get("value", []),
            )
            for r in results
        ],
    )


class SummaryMetric(BaseModel):
    event_type: str
    count: float


class PipelineSummary(BaseModel):
    timestamp: datetime
    events_produced: List[SummaryMetric]
    events_consumed: List[SummaryMetric]
    total_produced: float
    total_consumed: float
    consumer_lag: float
    active_windows: float
    errors: Dict[str, float]


@router.get("/summary", response_model=PipelineSummary)
async def get_pipeline_summary():
    produced = await prometheus.query("producer_events_total")
    consumed = await prometheus.query("processor_events_consumed_total")
    lag = await prometheus.query("sum(processor_consumer_lag)")
    windows = await prometheus.query("processor_active_windows")
    consume_errors = await prometheus.query("sum(processor_consume_errors_total)")
    publish_errors = await prometheus.query("sum(producer_publish_errors_total)")
    db_errors = await prometheus.query("sum(processor_db_write_errors_total)")

    events_produced = [
        SummaryMetric(
            event_type=r["metric"].get("event_type", "unknown"),
            count=float(r["value"][1]) if r.get("value") else 0,
        )
        for r in produced
    ]

    events_consumed = [
        SummaryMetric(
            event_type=r["metric"].get("event_type", "unknown"),
            count=float(r["value"][1]) if r.get("value") else 0,
        )
        for r in consumed
    ]

    def extract_value(results: list) -> float:
        if results and results[0].get("value"):
            try:
                return float(results[0]["value"][1])
            except (IndexError, ValueError):
                pass
        return 0.0

    return PipelineSummary(
        timestamp=datetime.now(timezone.utc),
        events_produced=events_produced,
        events_consumed=events_consumed,
        total_produced=sum(m.count for m in events_produced),
        total_consumed=sum(m.count for m in events_consumed),
        consumer_lag=extract_value(lag),
        active_windows=extract_value(windows),
        errors={
            "consume_errors": extract_value(consume_errors),
            "publish_errors": extract_value(publish_errors),
            "db_write_errors": extract_value(db_errors),
        },
    )
