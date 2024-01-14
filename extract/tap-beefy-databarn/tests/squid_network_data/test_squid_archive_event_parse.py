# test that we can indeed parse the archive event

from tap_beefy_databarn.squid_network_data.squid_models import SquidArchiveBlockResponse


class TestSquidArchiveEventParse:
    def test_squid_archive_empty_event_parse(self) -> None:
        response = {
            "header": {
                "number": 1403857,
                "hash": "0xec46945d4ae7647a8929fedf12bc91d2c740ab51efd746ba67d51564b9274422",
                "parentHash": "0x15f84ff16ca89995aefc0a0a9517b3d6a299a4653badf7877b0434ed5a782e51",
                "timestamp": 1632224639.0,
                "gasUsed": "0x12310a",
            },
            "transactions": [],
            "logs": [],
        }

        squid_response = SquidArchiveBlockResponse(**response)

        assert squid_response.header.number == 1403857
        assert squid_response.header.parent_hash == "0x15f84ff16ca89995aefc0a0a9517b3d6a299a4653badf7877b0434ed5a782e51"
        assert squid_response.header.timestamp == 1632224639.0
        assert squid_response.header.gas_used == "0x12310a"
        assert squid_response.transactions == []
        assert squid_response.logs == []

    def test_squid_archive_event_parse(self) -> None:
        response = {
            "header": {
                "number": 1403882,
                "hash": "0x7887b6e1719877e9487afc77b37ac396b9a3f891203b2e8ab7eb09b81181445f",
                "parentHash": "0x0975fb81d6d386ad1cf51122057a0f380063015d29e5bd5c674179b3c5324294",
                "timestamp": 1632224639.0,
                "gasUsed": "0x15cac5",
            },
            "transactions": [
                {
                    "transactionIndex": 0,
                    "gas": "0x4c4b40",
                    "gasPrice": "0x4fc4f95a",
                    "maxFeePerGas": None,
                    "maxPriorityFeePerGas": None,
                    "value": "0x0",
                    "gasUsed": "0x1fae91",
                    "cumulativeGasUsed": "0x15cac5",
                    "effectiveGasPrice": "0x27e27cad",
                    "contractAddress": None,
                    "type": 120,
                    "status": 1,
                    "sighash": "0xde5f6268",
                },
            ],
            "logs": [
                {
                    "logIndex": 5,
                    "transactionIndex": 0,
                    "address": "0xec7c0205a6f426c2cb1667d783b5b4fd2f875434",
                    "data": "0x000000000000000000000000000000000000000000000000454c584c3725bc4d",
                    "topics": [
                        "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
                        "0x0000000000000000000000000000000000000000000000000000000000000000",
                        "0x000000000000000000000000982f264ce97365864181df65df4931c593a515ad",
                    ],
                    "transactionHash": "0x88d81b4c2fa56919d0e0f45b088f2dc31dbc0b756646692cf83024c3c109bf63",
                },
            ],
        }

        squid_response = SquidArchiveBlockResponse(**response)

        assert squid_response.header.number == 1403882
        assert squid_response.header.parent_hash == "0x0975fb81d6d386ad1cf51122057a0f380063015d29e5bd5c674179b3c5324294"
        assert squid_response.header.timestamp == 1632224639.0
        assert squid_response.header.gas_used == "0x15cac5"
        assert squid_response.transactions[0].transaction_index == 0
        assert squid_response.transactions[0].gas == "0x4c4b40"
        assert squid_response.transactions[0].gas_price == "0x4fc4f95a"
        assert squid_response.transactions[0].max_fee_per_gas is None
        assert squid_response.transactions[0].max_priority_fee_per_gas is None
        assert squid_response.transactions[0].value == "0x0"
        assert squid_response.transactions[0].gas_used == "0x1fae91"
        assert squid_response.transactions[0].cumulative_gas_used == "0x15cac5"
        assert squid_response.transactions[0].effective_gas_price == "0x27e27cad"
        assert squid_response.transactions[0].contract_address is None
        assert squid_response.transactions[0].status == 1
        assert squid_response.transactions[0].sighash == "0xde5f6268"
        assert squid_response.logs[0].log_index == 5
        assert squid_response.logs[0].transaction_index == 0
        assert squid_response.logs[0].address == "0xec7c0205a6f426c2cb1667d783b5b4fd2f875434"
        assert squid_response.logs[0].data == "0x000000000000000000000000000000000000000000000000454c584c3725bc4d"
        assert squid_response.logs[0].topics == [
            "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
            "0x0000000000000000000000000000000000000000000000000000000000000000",
            "0x000000000000000000000000982f264ce97365864181df65df4931c593a515ad",
        ]
        assert squid_response.logs[0].transaction_hash == "0x88d81b4c2fa56919d0e0f45b088f2dc31dbc0b756646692cf83024c3c109bf63"
