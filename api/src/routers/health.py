from datetime import datetime

from fastapi import APIRouter
from pydantic import BaseModel


router = APIRouter()


class HealthResponse(BaseModel):
    status: str
    timestamp: datetime
    service: str
    version: str


@router.get("/health", response_model=HealthResponse)
async def health_check():
    return HealthResponse(
        status="healthy",
        timestamp=datetime.utcnow(),
        service="streaming-pipeline-api",
        version="1.0.0",
    )


@router.get("/ready")
async def readiness_check():
    return {"ready": True}

