
from __future__ import annotations

from datetime import datetime  # noqa: TCH003

from pydantic import BaseModel

from tap_beefy_databarn.common.chains import Chain  # noqa: TCH001


class ContractWatch(BaseModel):
    chain: Chain
    contract_address: str


class ContractCreationInfo(BaseModel):
    chain: Chain
    contract_address: str
    block_number: int
    block_datetime: datetime
