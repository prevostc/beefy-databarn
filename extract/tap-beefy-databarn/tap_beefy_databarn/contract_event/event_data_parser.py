import datetime
import json
import typing as t
from pathlib import Path

from web3 import Web3
from web3.contract.base_contract import BaseContractEvent
from web3.types import LogReceipt

from tap_beefy_databarn.common.chains import ChainType
from tap_beefy_databarn.contract_event.event_models import AnyEvent, EventType, event_event_type_to_topic0


class BeefyEventParser:
    _topic0_to_contract_event: dict[str, tuple[EventType, BaseContractEvent]]

    def __init__(self, abi_dir: Path = Path(__file__).parent / "abi") -> None:
        erc20_abi = json.load(Path.open(abi_dir / "IERC20.json"))
        beefy_zap_router_abi = json.load(Path.open(abi_dir / "BeefyZapRouter.json"))

        w3 = Web3()

        erc20_contract = w3.eth.contract(abi=erc20_abi)
        beefy_zap_router_contract = w3.eth.contract(abi=beefy_zap_router_abi)

        self._topic0_to_contract_event = {
            event_event_type_to_topic0["IERC20_Transfer"]: ("IERC20_Transfer", erc20_contract.events.Transfer()),
            event_event_type_to_topic0["BeefyZapRouter_FulfilledOrder"]: ("BeefyZapRouter_FulfilledOrder", beefy_zap_router_contract.events.FulfilledOrder()),
        }

    def parse_any_event(self, chain: ChainType, log_receipt: LogReceipt, block_datetime: datetime.datetime) -> AnyEvent:
        topic0 = next(iter(log_receipt["topics"]))
        assert topic0 is not None, "topics must not be empty"

        topic0_str = topic0.hex()
        assert topic0_str in self._topic0_to_contract_event, f"unknown topic0: {topic0_str}"

        (event_type, contract_event) = self._topic0_to_contract_event[topic0_str]
        parsed_event = contract_event.process_log(log_receipt)

        any_event_data: dict[str, t.Any] = {
            "chain": chain,
            "contract_address": log_receipt["address"],
            "transaction_hash": log_receipt["transactionHash"],
            "block_number": log_receipt["blockNumber"],
            "block_datetime": block_datetime,
            "log_index": log_receipt["logIndex"],
            "event_type": event_type,
            "ierc20_transfer": None,
            "beefyzaprouter_fulfilledorder": None,
        }

        if event_type == "IERC20_Transfer":
            any_event_data["ierc20_transfer"] = {
                "from_address": parsed_event["args"]["from"],
                "to_address": parsed_event["args"]["to"],
                "value": parsed_event["args"]["value"],
            }
        elif event_type == "BeefyZapRouter_FulfilledOrder":
            any_event_data["beefyzaprouter_fulfilledorder"] = parsed_event["args"]

        return AnyEvent(**any_event_data)
