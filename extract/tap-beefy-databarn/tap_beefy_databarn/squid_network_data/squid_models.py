from pydantic import BaseModel, ConfigDict
from pydantic.alias_generators import to_camel

class BaseSquidModel(BaseModel):
    model_config = ConfigDict(
        alias_generator=to_camel,
        populate_by_name=True,
        from_attributes=True,
        extra="ignore",
        frozen=True,
    )

class SquidBlockHeader(BaseSquidModel):
    number: int
    parent_hash: str
    timestamp: float
    gas_used: str

    def query_block_fields() -> list[str]:
        # https://docs.subsquid.io/sdk/reference/processors/evm-batch/field-selection/#block-headers
        return [
            "timestamp",
            "gasUsed",
        ]


class SquidLog(BaseSquidModel):
    log_index: int
    transaction_index: int
    address: str
    data: str
    topics: list[str]
    transaction_hash: str

    def query_log_fields() -> list[str]:
        # https://docs.subsquid.io/sdk/reference/processors/evm-batch/field-selection/#logs
        return [
            "address",
            "data",
            "topics",
            "transactionHash",
        ]


class SquidTransaction(BaseSquidModel):
    transaction_index: int
    gas: str
    gas_price: str
    max_fee_per_gas: str | None
    max_priority_fee_per_gas: str | None
    value: str
    gas_used: str
    cumulative_gas_used: str
    effective_gas_price: str
    contract_address: str | None
    status: int
    sighash: str

    def query_transaction_fields() -> list[str]:
        # https://docs.subsquid.io/sdk/reference/processors/evm-batch/field-selection/#transactions
        return [
            "gas",
            "gasPrice",
            "maxFeePerGas",
            "maxPriorityFeePerGas",
            "value",
            "gasUsed",
            "cumulativeGasUsed",
            "effectiveGasPrice",
            "contractAddress",
            "type",
            "status",
            "sighash",
        ]


class SquidArchiveBlockResponse(BaseSquidModel):
    header: SquidBlockHeader
    transactions: list[SquidTransaction]
    logs: list[SquidLog]
