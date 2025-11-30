from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.orm import sessionmaker, declarative_base

from api.lib.config import settings

CLICKHOUSE_URL = f"clickhouse+native://{settings.CLICKHOUSE_USER}:{settings.CLICKHOUSE_PASSWORD}@{settings.CLICKHOUSE_HOST}:{settings.CLICKHOUSE_PORT}/{settings.CLICKHOUSE_DB}"

engine = create_engine(CLICKHOUSE_URL, echo=False)
SessionLocal = sessionmaker(bind=engine)

metadata = MetaData()

Base = declarative_base(metadata=metadata)

class Product(Base):
    t = Table("product", metadata, autoload_with=engine, extend_existing=True)
    __table__ = t
    __mapper_args__ = {
        "primary_key": [
            t.c.chain_id,
            t.c.product_address,
        ]
    }


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
