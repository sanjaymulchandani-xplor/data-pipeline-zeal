import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from config import Config
from routers import health, metrics, aggregations, pipeline_status
from infrastructure.database import Database


logging.basicConfig(
    level=getattr(logging, Config.LOG_LEVEL),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting API server")
    Database.initialize(Config.postgres_connection_string())
    yield
    logger.info("Shutting down API server")
    Database.close()


app = FastAPI(
    title="Streaming Data Pipeline API",
    description="REST API for querying pipeline metrics and aggregated data",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(health.router, tags=["Health"])
app.include_router(metrics.router, prefix="/api/metrics", tags=["Metrics"])
app.include_router(aggregations.router, prefix="/api/aggregations", tags=["Aggregations"])
app.include_router(pipeline_status.router, prefix="/api/pipeline", tags=["Pipeline Status"])

