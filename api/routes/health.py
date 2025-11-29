"""Health check routes."""
from fastapi import APIRouter
from pydantic import BaseModel

router = APIRouter()


class HealthResponse(BaseModel):
    """Health check response model."""
    status: str
    service: str


@router.get("/health", response_model=HealthResponse, tags=["health"], include_in_schema=False)
async def health_check():
    """Health check endpoint."""
    return HealthResponse(status="healthy", service="beefy-databarn-api")

