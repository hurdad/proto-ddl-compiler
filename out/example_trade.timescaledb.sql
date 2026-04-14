CREATE TYPE side AS ENUM ('SIDE_UNKNOWN', 'SIDE_BUY', 'SIDE_SELL');

CREATE TABLE trades (
  timestamp BIGINT NOT NULL,
  symbol TEXT NOT NULL,
  price DOUBLE PRECISION NOT NULL,
  size INTEGER NOT NULL,
  side side NOT NULL,
  trade_id UUID NOT NULL
);

SELECT create_hypertable('trades', by_range('timestamp'));
