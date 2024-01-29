
from __future__ import annotations

from datetime import datetime  # noqa: TCH003
from typing import TYPE_CHECKING

from pydantic import BaseModel

if TYPE_CHECKING:
    from tap_beefy_databarn.common.chains import Chain


class ContractWatch(BaseModel):
    chain: Chain
    contract_address: str


class ContractCreationInfo(BaseModel):
    chain: Chain
    contract_address: str
    block_number: int
    block_datetime: datetime
