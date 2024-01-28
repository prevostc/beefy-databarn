import typing as t
from datetime import UTC, datetime

from eth_typing.evm import BlockNumber, ChecksumAddress, HexAddress, HexStr
from tap_beefy_databarn.contract_event.event_data_parser import BeefyEventParser
from web3.types import HexBytes, LogReceipt


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

        assert event.data is not None
        assert event.data.event_type == "IERC20_Transfer"
        assert event.data.value == 4993463171213016141
        assert event.data.from_address == "0x0000000000000000000000000000000000000000"
        assert event.data.to_address == "0x982F264ce97365864181df65dF4931C593A515ad"

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

        assert event.data is not None
        assert event.data.event_type == "BeefyZapRouter_FulfilledOrder"
        assert event.data.caller_address == "0x3EDB7d5b494cCB9bb84D11CA25F320Af2bb15f40"
        assert event.data.recipient_address == "0x3EDB7d5b494cCB9bb84D11CA25F320Af2bb15f40"

    def test_parse_beefyvault_upgradestrat_event(self) -> None:
        raw_data = {
            "address": "0x0383E88A19E5c387FeBafbF51E5bA642d2ad8bE0",
            "topics": [
                "0x7f37d440e85aba7fbf641c4bda5ca4ef669a80bffaacde2aa8d9feb1b048c82c",
            ],
            "data": "0x000000000000000000000000a8bf778716e0630f56a4bdca9ae9a3e0b2bd29f5",
            "transaction_hash": "0xefd2789ac10235a835bcb24d15f3fa4ec34f3572e53ae9eb066fc3eb1030715e",
        }

        log_receipt = self._log_data_to_log_receipt(raw_data)
        block_datetime = datetime.now(tz=UTC)
        event = self.parser.parse_any_event("bsc", log_receipt, block_datetime)

        assert event.data is not None
        assert event.data.event_type == "BeefyVault_UpgradeStrat"
        assert event.data.implementation == "0xA8bf778716e0630F56A4bDCa9AE9A3e0B2BD29f5"

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
