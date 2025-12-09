-- One-shot dedup of dlt.beefy_db_incremental___prices by (oracle_id, t)
-- Final table columns use explicit compression CODEC(ZSTD(3))
-- Ans is a ReplacingMergeTree so it will automatically deduplicate by (oracle_id, t)

-- 0. Clean leftovers from previous attempts
DROP TABLE IF EXISTS dlt.beefy_db_incremental___prices_tmp SYNC;

-- 1. Temporary ReplacingMergeTree for deduplication
CREATE TABLE dlt.beefy_db_incremental___prices_tmp
(
    `oracle_id` Int64 CODEC(ZSTD(3)),
    `t`         DateTime64(6, 'UTC') CODEC(ZSTD(3)),
    `val`       Nullable(Decimal(76, 20)) CODEC(ZSTD(3))
)
ENGINE = ReplacingMergeTree
PRIMARY KEY (oracle_id, t)
ORDER BY (oracle_id, t)
SETTINGS index_granularity = 8192;

-- 2. Load all raw data (includes duplicates)
INSERT INTO dlt.beefy_db_incremental___prices_tmp
SELECT *
FROM dlt.beefy_db_incremental___prices;

-- 3. Force merges so RMTree collapses dups
OPTIMIZE TABLE dlt.beefy_db_incremental___prices_tmp FINAL;

-- 6. Swap tables
RENAME TABLE IF EXISTS dlt.beefy_db_incremental___prices TO dlt.beefy_db_incremental___prices_old;
RENAME TABLE
    dlt.beefy_db_incremental___prices_tmp TO dlt.beefy_db_incremental___prices;

-- 7. Optional cleanup
DROP TABLE IF EXISTS dlt.beefy_db_incremental___prices_old SYNC;