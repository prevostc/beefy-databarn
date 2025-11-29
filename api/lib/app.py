"""FastAPI application factory and setup."""
import logging

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from prometheus_fastapi_instrumentator import Instrumentator
from slowapi.errors import RateLimitExceeded
from clickhouse_connect.driver.exceptions import DatabaseError, InterfaceError

from api.lib.config import settings
from api.lib.middleware import limiter, rate_limit_handler
from api.lib.exceptions import database_exception_handler, general_exception_handler
from api.routes import health, revenue_summary

logger = logging.getLogger(__name__)


def create_app() -> FastAPI:
    """Create and configure the FastAPI application."""
    app = FastAPI(
        title="Beefy Databarn API",
        description="API for querying Beefy Databarn analytics data",
        version="1.0.0",
        docs_url="/docs",
        redoc_url="/redoc",
        openapi_url="/openapi.json",
    )

    # Add rate limiter to app state
    app.state.limiter = limiter
    app.add_exception_handler(RateLimitExceeded, rate_limit_handler)
    
    # Add exception handlers for database and general errors
    app.add_exception_handler(DatabaseError, database_exception_handler)
    app.add_exception_handler(InterfaceError, database_exception_handler)
    app.add_exception_handler(Exception, general_exception_handler)

    # Add Prometheus metrics instrumentation
    Instrumentator().instrument(app).expose(app)

    # Add CORS middleware
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],  # Configure appropriately for production
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Add rate limit headers middleware
    @app.middleware("http")
    async def add_rate_limit_headers(request: Request, call_next):
        """Add rate limit headers to responses."""
        response = await call_next(request)
        if hasattr(request.state, "view_rate_limit"):
            limiter = request.app.state.limiter
            response = limiter._inject_headers(response, request.state.view_rate_limit)
        return response

    # Include routers
    app.include_router(health.router)
    app.include_router(revenue_summary.router)
    
    # Modify OpenAPI schema to exclude /metrics endpoint
    # /health is already excluded via include_in_schema=False on the route
    # This must be done after all routes are registered
    def custom_openapi():
        if app.openapi_schema:
            return app.openapi_schema
        from fastapi.openapi.utils import get_openapi
        openapi_schema = get_openapi(
            title=app.title,
            version=app.version,
            description=app.description,
            routes=app.routes,
        )
        # Remove /metrics from OpenAPI schema (Instrumentator doesn't support include_in_schema=False)
        openapi_schema.get("paths", {}).pop("/metrics", None)
        app.openapi_schema = openapi_schema
        return app.openapi_schema
    
    app.openapi = custom_openapi

    # Register shutdown event
    @app.on_event("shutdown")
    async def shutdown_event():
        """Cleanup on application shutdown."""
        from api.lib.db import close_connection
        close_connection()
        logger.info("Application shutdown complete")

    return app

