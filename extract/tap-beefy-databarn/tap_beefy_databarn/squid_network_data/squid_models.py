
import datetime
from pydantic import BaseModel, ConfigDict, field_validator
from pydantic.alias_generators import to_camel


# https://docs.pydantic.dev/latest/api/config/
_squid_model_config = ConfigDict(
    extra = "ignore",
    frozen = True,
    populate_by_name = True,
    validate_assignment = True,
    arbitrary_types_allowed = False,
    from_attributes = True,
    loc_by_alias = True,
    alias_generator = to_camel,
    allow_inf_nan = False,
    strict = True,
)


class SquidBlockHeader(BaseModel):
    model_config = _squid_model_config

    number: int
    parent_hash: str
    timestamp: datetime.datetime
    gas_used: str

    @field_validator("timestamp", mode="before")
    def transform_datetime(cls, raw: str) -> tuple[int, int]:
        x, y = raw.split("x")
        return int(x), int(y)


class SquidLog(BaseModel):
    model_config = _squid_model_config

    log_index: int
    transaction_index: int
    address: str
    data: str
    topics: list[str]
    transaction_hash: str
    
class SquidTransaction(BaseModel):
    model_config = _squid_model_config
    
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



class SquidArchiveBlockResponse(BaseModel):
    model_config = _squid_model_config
    
    header: SquidBlockHeader
    transactions: list[SquidTransaction]
    logs: list[SquidLog]

    @classmethod
    def get_archive_query_fields(cls) -> dict[str, dict[str, bool]]:

        # https://docs.subsquid.io/sdk/reference/processors/evm-batch/field-selection/#logs
        log_fields = [
            "address",
            "data",
            "topics",
            "transactionHash",
        ]
        # https://docs.subsquid.io/sdk/reference/processors/evm-batch/field-selection/#block-headers
        block_fields = [
            "timestamp",
            "gasUsed",
        ]
        # https://docs.subsquid.io/sdk/reference/processors/evm-batch/field-selection/#transactions
        transaction_fields = [
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

        return {"block": {f: True for f in block_fields}, "log": {f: True for f in log_fields}, "transaction": {f: True for f in transaction_fields}}