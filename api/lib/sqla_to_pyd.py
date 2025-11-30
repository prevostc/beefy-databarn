# lib/pyd_from_sqla.py
from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Any, Optional, List, Dict, Type

from pydantic import BaseModel
from sqlalchemy import (
    Boolean,
    Integer,
    Float,
    String,
    Date,
    DateTime,
    Numeric,
)
from sqlalchemy.sql.schema import Table
from sqlalchemy.orm import DeclarativeMeta

try:
    # optional, for ClickHouse-specific wrappers like Nullable, Array
    from clickhouse_sqlalchemy import types as ch_types
except ImportError:  # pragma: no cover
    ch_types = None


def _py_type(col_type) -> Any:
    """
    Map SQLAlchemy / ClickHouse types to Python types, for schema / Pydantic.
    Extend this as needed.
    """
    # ClickHouse Nullable / Array
    if ch_types is not None:
        if isinstance(col_type, ch_types.Nullable):
            inner = col_type.inner_type
            return Optional[_py_type(inner)]  # type: ignore

        if isinstance(col_type, ch_types.Array):
            inner = col_type.item_type
            return List[_py_type(inner)]  # type: ignore

    # Core scalar-ish
    if isinstance(col_type, Integer):
        return int
    if isinstance(col_type, Float):
        return float
    if isinstance(col_type, Boolean):
        return bool
    if isinstance(col_type, String):
        return str
    if isinstance(col_type, Date):
        return date
    if isinstance(col_type, DateTime):
        return datetime
    if isinstance(col_type, (Numeric, Decimal)):
        return Decimal

    # Fallback for weird types
    return Any


def pydantic_from_sqla_model(
    model: DeclarativeMeta,
    name: str | None = None,
) -> Type[BaseModel]:
    """
    Build a Pydantic v2 BaseModel from a SQLAlchemy ORM model.

    This model is for FastAPI / fastapi-filters (it has no DB behavior).
    """
    table: Table = model.__table__

    annotations: Dict[str, Any] = {}
    defaults: Dict[str, Any] = {}

    for col in table.columns:
        py_type = _py_type(col.type)
        annotations[col.name] = py_type

        if col.nullable:
            defaults[col.name] = None

    attrs = {"__annotations__": annotations}
    attrs.update(defaults)

    model_name = name or f"{model.__name__}Schema"
    return type(model_name, (BaseModel,), attrs)
