"""
This module contains the models for the events emitted by the Beefy contracts.
We need to use pydantic to be able to generate a json schema on the various streams.
These json schemas are used to create target tables in the target database.
"""
import datetime
import typing as t

from eth_pydantic_types import Address, HexBytes
from pydantic import BaseModel

from tap_beefy_databarn.common.chains import ChainType


class BlockchainEvent(BaseModel):
    chain: ChainType
    contract_address: Address
    transaction_hash: HexBytes
    block_number: int
    block_datetime: datetime.datetime
    log_index: int


class Event_IERC20_Transfer(BaseModel):  # noqa: N801
    from_address: Address
    to_address: Address
    value: int


class Event_BeefyZapRouter_FulfilledOrder(BaseModel):  # noqa: N801
    # the order structs are indexed and we only get the caller and recipient addresses
    caller_address: Address
    recipient_address: Address


EventType = t.Literal[
    "IERC20_Transfer",
    "BeefyZapRouter_FulfilledOrder",
]

event_topic0_to_event_type: dict[str, EventType] = {
    "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef": "IERC20_Transfer",
    "0x1ba5b6ed656994657175705961138c96bd8ec133c35817fa85903f450129e0b1": "BeefyZapRouter_FulfilledOrder",
}
event_event_type_to_topic0: dict[EventType, str] = {v: k for k, v in event_topic0_to_event_type.items()}


class AnyEvent(BlockchainEvent):
    event_type: EventType
    ierc20_transfer: Event_IERC20_Transfer | None = None
    beefyzaprouter_fulfilledorder: Event_BeefyZapRouter_FulfilledOrder | None = None
