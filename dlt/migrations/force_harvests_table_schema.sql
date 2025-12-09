-- One-shot dedup of dlt.beefy_db_incremental___harvests by (chain_id, block_number, txn_idx, event_idx)
-- Final table columns use explicit compression CODEC(ZSTD(3))
-- Ans is a ReplacingMergeTree so it will automatically deduplicate by (chain_id, block_number, txn_idx, event_idx)

-- 0. Clean leftovers from previous attempts
DROP TABLE IF EXISTS dlt.beefy_db_incremental___harvests_tmp SYNC;

-- 1. Temporary ReplacingMergeTree for deduplication
CREATE TABLE IF NOT EXISTS dlt.beefy_db_incremental___harvests_tmp
(
    `chain_id` Int64 CODEC(Delta, ZSTD),
    `block_number` Int64 CODEC(Delta, ZSTD),
    `txn_idx` Int32 CODEC(Delta, ZSTD),
    `event_idx` Int32 CODEC(Delta, ZSTD),
    `txn_timestamp` Nullable(DateTime64(6, 'UTC')) CODEC(DoubleDelta, ZSTD),
    `txn_hash` Nullable(String) CODEC(ZSTD),
    `vault_id` Nullable(String) CODEC(ZSTD),
    `call_fee` Nullable(Decimal(76, 20)) CODEC(ZSTD),
    `gas_fee` Nullable(Decimal(76, 20)) CODEC(ZSTD),
    `platform_fee` Nullable(Decimal(76, 20)) CODEC(ZSTD),
    `strategist_fee` Nullable(Decimal(76, 20)) CODEC(ZSTD),
    `harvest_amount` Nullable(Decimal(76, 20)) CODEC(ZSTD),
    `native_price` Nullable(Decimal(76, 20)) CODEC(ZSTD),
    `want_price` Nullable(Decimal(76, 20)) CODEC(ZSTD),
    `is_cowllector` Nullable(Bool) CODEC(LZ4),
    `strategist_address` Nullable(String) CODEC(ZSTD)
)
ENGINE = ReplacingMergeTree
PRIMARY KEY (chain_id, block_number, txn_idx, event_idx)
ORDER BY (chain_id, block_number, txn_idx, event_idx)
SETTINGS index_granularity = 8192;

-- 2. Load all raw data (includes duplicates)
INSERT INTO dlt.beefy_db_incremental___harvests_tmp
SELECT *
FROM dlt.beefy_db_incremental___harvests;

-- 3. Force merges so RMTree collapses dups
OPTIMIZE TABLE dlt.beefy_db_incremental___harvests_tmp FINAL;

-- 6. Swap tables
RENAME TABLE IF EXISTS dlt.beefy_db_incremental___harvests TO dlt.beefy_db_incremental___harvests_old;
RENAME TABLE
    dlt.beefy_db_incremental___harvests_tmp TO dlt.beefy_db_incremental___harvests;

-- 7. Optional cleanup
DROP TABLE IF EXISTS dlt.beefy_db_incremental___harvests_old SYNC;