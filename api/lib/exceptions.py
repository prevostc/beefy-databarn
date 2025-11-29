"""Exception handlers for the API."""
import logging

from fastapi import Request, HTTPException
from fastapi.responses import JSONResponse
from clickhouse_connect.driver.exceptions import DatabaseError, InterfaceError

logger = logging.getLogger(__name__)


async def database_exception_handler(request: Request, exc: Exception):
    """Handle database-related exceptions."""
    logger.error(f"Database error: {exc}", exc_info=True)
    return JSONResponse(
        status_code=500,
        content={
            "error": "Database error",
            "message": "An error occurred while querying the database",
        },
    )


async def general_exception_handler(request: Request, exc: Exception):
    """Handle general exceptions."""
    logger.error(f"Unhandled error: {exc}", exc_info=True)
    return JSONResponse(
        status_code=500,
        content={
            "error": "Internal server error",
            "message": "An unexpected error occurred",
        },
    )

