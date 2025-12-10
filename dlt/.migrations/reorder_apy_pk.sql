CREATE TABLE IF NOT EXISTS dlt.beefy_db___apys_new
(
    `vault_id` Int64 CODEC(ZSTD(3)),
    `t`         DateTime64(6, 'UTC') CODEC(ZSTD(3)),
    `val`       Nullable(Float64) CODEC(ZSTD(3))
)
ENGINE = ReplacingMergeTree
PRIMARY KEY (vault_id, t)
ORDER BY (vault_id, t)
SETTINGS index_granularity = 8192;


INSERT INTO dlt.beefy_db___apys_new
SELECT * FROM dlt.beefy_db___apys;

OPTIMIZE TABLE dlt.beefy_db___apys_new FINAL;

RENAME TABLE IF EXISTS dlt.beefy_db___apys TO dlt.beefy_db___apys_old;
RENAME TABLE IF EXISTS dlt.beefy_db___apys_new TO dlt.beefy_db___apys;

DROP TABLE IF EXISTS dlt.beefy_db___apys_old SYNC;