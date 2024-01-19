import datetime

from pydantic import BaseModel, ConfigDict, Field, field_validator
from pydantic.alias_generators import to_camel

# https://docs.pydantic.dev/latest/api/config/
_squid_model_config = ConfigDict(
    extra="ignore",
    frozen=True,
    populate_by_name=True,
    validate_assignment=True,
    arbitrary_types_allowed=False,
    from_attributes=True,
    loc_by_alias=True,
    alias_generator=to_camel,
    allow_inf_nan=False,
    strict=True,
)


def transform_datetime(raw: float) -> datetime.datetime:
    return datetime.datetime.fromtimestamp(raw, tz=datetime.timezone.utc)


def transform_hex_to_int(raw: str) -> int:
    return int(raw, 16)


def transform_maybe_hex_to_int(raw: str | None) -> int | None:
    if raw is None:
        return None
    return int(raw, 16)


class SquidBlockHeader(BaseModel):
    model_config = _squid_model_config

    block_hash: str = Field(alias="hash")
    number: int
    parent_hash: str
    timestamp: datetime.datetime
    gas_used: int

    @field_validator("timestamp", mode="before")
    def transform_timestamp(cls, v: float) -> datetime.datetime:  # noqa: N805
        return transform_datetime(v)

    @field_validator("gas_used", mode="before")
    def transform_gas_used(cls, v: str) -> int:  # noqa: N805
        return transform_hex_to_int(v)


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
    gas: int
    gas_price: int
    max_fee_per_gas: int | None
    max_priority_fee_per_gas: int | None
    value: int
    gas_used: int
    cumulative_gas_used: int
    effective_gas_price: int
    contract_address: str | None

    @field_validator("gas", mode="before")
    def transform_gas(cls, v: str) -> int:  # noqa: N805
        return transform_hex_to_int(v)

    @field_validator("gas_price", mode="before")
    def transform_gas_price(cls, v: str) -> int:  # noqa: N805
        return transform_hex_to_int(v)

    @field_validator("max_fee_per_gas", mode="before")
    def transform_max_fee_per_gas(cls, v: str | None) -> int | None:  # noqa: N805
        return transform_maybe_hex_to_int(v)

    @field_validator("max_priority_fee_per_gas", mode="before")
    def transform_max_priority_fee_per_gas(cls, v: str | None) -> int | None:  # noqa: N805
        return transform_maybe_hex_to_int(v)

    @field_validator("value", mode="before")
    def transform_value(cls, v: str) -> int:  # noqa: N805
        return transform_hex_to_int(v)

    @field_validator("gas_used", mode="before")
    def transform_gas_used(cls, v: str) -> int:  # noqa: N805
        return transform_hex_to_int(v)

    @field_validator("cumulative_gas_used", mode="before")
    def transform_cumulative_gas_used(cls, v: str) -> int:  # noqa: N805
        return transform_hex_to_int(v)

    @field_validator("effective_gas_price", mode="before")
    def transform_effective_gas_price(cls, v: str) -> int:  # noqa: N805
        return transform_hex_to_int(v)


class SquidArchiveBlockResponse(BaseModel):
    model_config = _squid_model_config

    block: SquidBlockHeader = Field(alias="header")
    transactions: list[SquidTransaction]
    logs: list[SquidLog]

    @classmethod
    def get_archive_query_fields(cls) -> dict[str, dict[str, bool]]:  # noqa: ANN102
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
