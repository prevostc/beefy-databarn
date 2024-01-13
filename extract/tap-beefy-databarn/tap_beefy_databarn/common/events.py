import datetime
import typing as t
from pydantic.dataclasses import dataclass

from tap_beefy_databarn.common.chains import ChainType


@dataclass
class BlockchainEvent:
    chain: ChainType
    contract_address: str
    transaction_hash: str
    block_number: int
    block_datetime: datetime.datetime
    log_index: int


@dataclass
class Event_IERC20_Transfer:  # noqa: N801
    from_address: str
    to_address: str
    amount: int


@dataclass
class Event_BeefyZapRouter_Input:  # noqa: N801
    token_address: str
    amount: int


@dataclass
class Event_BeefyZapRouter_Output:  # noqa: N801
    token_address: str
    min_output_amount: str


@dataclass
class Event_BeefyZapRouter_Relay:  # noqa: N801
    target_address: str
    value: int
    data: bytes


@dataclass
class Event_BeefyZapRouter_Order:  # noqa: N801
    inputs: list[Event_BeefyZapRouter_Input]
    outputs: list[Event_BeefyZapRouter_Output]
    relay: Event_BeefyZapRouter_Relay
    user_address: str
    recipient_address: str


@dataclass
class Event_BeefyZapRouter_FulfilledOrder:  # noqa: N801
    order: Event_BeefyZapRouter_Order
    caller_address: str
    recipient_address: str


EventType = t.Literal[
    "IERC20:Transfer",
    "BeefyZapRouter:FulfilledOrder",
]


@dataclass
class AnyEvent(BlockchainEvent):
    event_type: EventType
    erc20_transfer: Event_IERC20_Transfer | None = None
    beefy_zap_router_fulfilled_order: Event_BeefyZapRouter_FulfilledOrder | None = None
