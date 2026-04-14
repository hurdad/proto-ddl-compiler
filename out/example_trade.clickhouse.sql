CREATE TABLE trades
(
  timestamp DateTime64(3),
  symbol String,
  price Float64,
  size Int32,
  side LowCardinality(String),
  trade_id UUID
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (timestamp);
