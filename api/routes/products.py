from typing import List

from fastapi import APIRouter, Depends
from sqlalchemy import select
from sqlalchemy.orm import Session

from fastapi_filters import create_filters_from_model, FilterValues
from fastapi_filters.ext.sqlalchemy import apply_filters
from api.lib.sqla_to_pyd import pydantic_from_sqla_model
from api.lib.db import get_db
from api.lib.db import Product

router = APIRouter(prefix="/api/v1", tags=["products"])

FiltersModel = pydantic_from_sqla_model(Product)
Filters = create_filters_from_model(FiltersModel)

@router.get("/products")
async def list_products(
    session: Session = Depends(get_db),
    filters: FilterValues = Depends(Filters),
) -> List[Product]:
    stmt = select(Product)
    stmt = apply_filters(stmt, filters)
    stmt = stmt.limit(500)
    results = session.exec(stmt).all()
    return results
