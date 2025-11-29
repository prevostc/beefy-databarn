# API routes

from .health import router as health_router
# from .revenue_summary import router as revenue_summary_router
from .products import router as products_router

routers = [
    health_router,
    # revenue_summary_router,
    products_router,
]
