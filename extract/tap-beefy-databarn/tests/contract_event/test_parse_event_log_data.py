import typing as t
from datetime import UTC, datetime

from eth_typing.evm import BlockNumber, ChecksumAddress, HexAddress, HexStr
from web3.types import HexBytes, LogReceipt

from tap_beefy_databarn.contract_event.event_data_parser import BeefyEventParser


class TestBeefyEventParser:
    parser = BeefyEventParser()

    def test_parse_transfer_event(self) -> None:
        raw_data = {
            "address": "0xec7c0205a6f426c2cb1667d783b5b4fd2f875434",
            "data": "0x000000000000000000000000000000000000000000000000454c584c3725bc4d",
            "topics": [
                "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
                "0x0000000000000000000000000000000000000000000000000000000000000000",
                "0x000000000000000000000000982f264ce97365864181df65df4931c593a515ad",
            ],
            "transaction_hash": "0x7887b6e1719877e9487afc77b37ac396b9a3f891203b2e8ab7eb09b81181445f",
        }

        log_receipt = self._log_data_to_log_receipt(raw_data)
        block_datetime = datetime.now(tz=UTC)
        event = self.parser.parse_any_event("bsc", log_receipt, block_datetime)

        assert event.event_type == "IERC20_Transfer"
        assert event.ierc20_transfer is not None
        assert event.ierc20_transfer.value == 4993463171213016141
        assert event.ierc20_transfer.from_address == "0x0000000000000000000000000000000000000000"
        assert event.ierc20_transfer.to_address == "0x982F264ce97365864181df65dF4931C593A515ad"

    def test_parse_beefyzaprouter_fulfilledorder_event(self) -> None:
        raw_data = {
            "address": "0x13761d473ff1478957adb80cb4e58e0af76d2c51",
            "topics": [
                "0x1ba5b6ed656994657175705961138c96bd8ec133c35817fa85903f450129e0b1",
                "0x12c747f67190f4b64fff477c0e1997b021e55e2fd489e79ee3095af0054b47cf",
                "0x0000000000000000000000003edb7d5b494ccb9bb84d11ca25f320af2bb15f40",
                "0x0000000000000000000000003edb7d5b494ccb9bb84d11ca25f320af2bb15f40",
            ],
            "data": "0x",
            "transaction_hash": "0x50b094afb5a403191f8d13726af946cd5a9e81303f005863dafeed873e9b3c08",
        }

        log_receipt = self._log_data_to_log_receipt(raw_data)
        block_datetime = datetime.now(tz=UTC)
        event = self.parser.parse_any_event("bsc", log_receipt, block_datetime)

        assert event.event_type == "BeefyZapRouter_FulfilledOrder"
        assert event.beefyzaprouter_fulfilledorder is not None
        assert event.beefyzaprouter_fulfilledorder

    def _log_data_to_log_receipt(self, raw_data: dict[str, t.Any]) -> LogReceipt:
        return LogReceipt(
            topics=[HexBytes(topic) for topic in raw_data["topics"]],
            data=HexBytes(t.cast(str, raw_data["data"])),
            # mock other unused fields
            logIndex=0,
            blockHash=HexBytes("0x0"),
            blockNumber=BlockNumber(0),
            address=ChecksumAddress(HexAddress(HexStr(t.cast(str, raw_data["address"])))),
            transactionIndex=0,
            transactionHash=HexBytes(t.cast(str, raw_data["transaction_hash"])),
            removed=False,
        )
