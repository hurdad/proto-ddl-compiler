# proto-ddl-compiler

A `protoc` plugin (`protoc-gen-dbddl`) that reads proto3 message definitions annotated with custom options and generates DDL for **ClickHouse** and **TimescaleDB**.

## Overview

Annotate your messages with table/column options, then run `protoc` with this plugin to emit `.clickhouse.sql` and `.timescaledb.sql` files.

```proto
syntax = "proto3";

import "db_options.proto";
import "google/protobuf/timestamp.proto";

enum Side {
  SIDE_UNKNOWN = 0;
  SIDE_BUY     = 1;
  SIDE_SELL    = 2;
}

message Trade {
  option (dbddl.ch_table)        = "trades";
  option (dbddl.ch_partition_by) = "toYYYYMM(timestamp)";
  option (dbddl.ch_order_by)     = "timestamp";

  option (dbddl.ts_table)        = "trades";
  option (dbddl.ts_time_column)  = "timestamp";

  google.protobuf.Timestamp timestamp = 1 [
    (dbddl.db_nullable) = false,
    (dbddl.db_comment)  = "UTC event timestamp"
  ];
  string     symbol   = 2 [(dbddl.db_comment) = "Instrument symbol"];
  double     price    = 3 [(dbddl.db_comment) = "Execution price"];
  int32      size     = 4 [(dbddl.db_comment) = "Quantity traded"];
  Side       side     = 5 [(dbddl.db_comment) = "Aggressor side"];
  dbddl.UUID trade_id = 6 [(dbddl.db_comment) = "Exchange-assigned trade ID"];
}
```

Given the example above, the plugin produces:

**`example_trade.clickhouse.sql`**
```sql
CREATE TABLE trades
(
  timestamp DateTime64(3) COMMENT 'UTC event timestamp',
  symbol String COMMENT 'Instrument symbol',
  price Float64 COMMENT 'Execution price',
  size Int32 COMMENT 'Quantity traded',
  side LowCardinality(String) COMMENT 'Aggressor side',
  trade_id Nullable(UUID) COMMENT 'Exchange-assigned trade ID'
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (timestamp);
```

**`example_trade.timescaledb.sql`**
```sql
CREATE TYPE side AS ENUM ('SIDE_UNKNOWN', 'SIDE_BUY', 'SIDE_SELL');

CREATE TABLE trades (
  timestamp TIMESTAMPTZ NOT NULL,
  symbol TEXT NOT NULL,
  price DOUBLE PRECISION NOT NULL,
  size INTEGER NOT NULL,
  side side NOT NULL,
  trade_id UUID
);
COMMENT ON COLUMN trades.timestamp IS 'UTC event timestamp';
COMMENT ON COLUMN trades.symbol IS 'Instrument symbol';
COMMENT ON COLUMN trades.price IS 'Execution price';
COMMENT ON COLUMN trades.size IS 'Quantity traded';
COMMENT ON COLUMN trades.side IS 'Aggressor side';
COMMENT ON COLUMN trades.trade_id IS 'Exchange-assigned trade ID';

SELECT create_hypertable('trades', by_range('timestamp'));
```

## Options reference

### Message options

| Option               | Description                                        |
|----------------------|----------------------------------------------------|
| `ch_table`           | ClickHouse table name (enables CH output)          |
| `ch_engine`          | ClickHouse engine clause (default: `MergeTree()`)  |
| `ch_partition_by`    | `PARTITION BY` expression                          |
| `ch_order_by`        | `ORDER BY` expression (**required** for CH tables) |
| `ts_table`           | TimescaleDB table name (enables TS output)         |
| `ts_time_column`     | Hypertable time column (**required** for TS tables)|
| `ts_chunk_interval`  | Chunk time interval passed to `by_range()`         |
| `ch_ttl`             | ClickHouse TTL expression, e.g. `timestamp + INTERVAL 3 MONTH`       |
| `ch_sample_by`       | ClickHouse `SAMPLE BY` expression                                     |
| `ch_settings`        | ClickHouse `SETTINGS` clause, e.g. `index_granularity = 8192`        |
| `ts_compress_after`  | Enable TimescaleDB compression policy, e.g. `7 days`                 |
| `ts_compress_segmentby` | `timescaledb.compress_segmentby` column(s), e.g. `symbol`         |
| `ts_compress_orderby`   | `timescaledb.compress_orderby` expression, e.g. `timestamp DESC`  |
| `ts_retention`       | TimescaleDB retention policy interval, e.g. `1 year`                 |
| `db_auto_pk`         | Add a named integer identity PK column (PG: `BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY`, CH: `UInt64`) |
| `db_uuid_pk`         | Add an `id UUID` PK column with a random default (`gen_random_uuid()` / `generateUUIDv4()`) |

### Field options

| Option                      | Description                                                                 |
|-----------------------------|-----------------------------------------------------------------------------|
| `ch_column_type`            | Override ClickHouse column type                                             |
| `pg_column_type`            | Override PostgreSQL/TimescaleDB column type                                 |
| `db_nullable`               | Override nullability (`true`/`false`)                                       |
| `db_name`                   | Override column name in the output DDL                                      |
| `db_uuid`                   | Treat a `bytes` field as a 128-bit UUID (`UUID` type in both backends)      |
| `db_index`                  | Create an index on this column                                              |
| `pg_index_using`            | PG index method: `hash`, `brin`, `gin`, etc. (default: `btree`)            |
| `ch_skip_index_type`        | CH data-skipping index type: `minmax`, `set(N)`, `bloom_filter` (default: `minmax`) |
| `ch_skip_index_granularity` | CH skip index granularity (default: `1`)                             |
| `ch_codec`                  | CH column codec(s), e.g. `Delta, LZ4` or `ZSTD(1)`                  |
| `db_default`                | Column `DEFAULT` expression (passed through as-is to both backends)  |
| `db_comment`                | Column comment (CH: inline `COMMENT`; PG: `COMMENT ON COLUMN`)      |
| `db_embed_prefix`           | Flatten a message-type field into the parent table; sub-fields are prefixed with the option value (e.g. `"loc"` → `loc_lat`, `loc_lon`). Not applicable to `Timestamp` or `UUID` fields. |

## Proto3 type mapping

| Proto3 type          | ClickHouse   | PostgreSQL        |
|----------------------|--------------|-------------------|
| `int32`/`sint32`/`sfixed32` | `Int32` | `INTEGER`    |
| `int64`/`sint64`/`sfixed64` | `Int64` | `BIGINT`     |
| `uint32`/`fixed32`   | `UInt32`     | `INTEGER`         |
| `uint64`/`fixed64`   | `UInt64`     | `BIGINT`          |
| `float`              | `Float32`    | `REAL`            |
| `double`             | `Float64`    | `DOUBLE PRECISION`|
| `bool`               | `Bool`       | `BOOLEAN`         |
| `string`             | `String`     | `TEXT`            |
| `bytes`              | `String`     | `BYTEA`           |
| `google.protobuf.Timestamp` | `DateTime64(3)` | `TIMESTAMPTZ` |
| `repeated T`         | `Array(T)`   | `T[]`             |
| `enum`               | `LowCardinality(String)` | native `ENUM` type |
| `dbddl.UUID`         | `UUID`       | `UUID`            |

Nullability defaults to `false` for implicit proto3 fields and `true` for `optional`-qualified fields. Message fields (`google.protobuf.Timestamp`, `dbddl.UUID`) always have presence and are nullable by default — use `db_nullable = false` to make them `NOT NULL`. The `db_nullable` option always overrides.

> **Note**: `db_nullable = true` requires the field to be declared `optional` (or be a message type). Using `db_nullable = true` on a plain (non-optional) proto3 scalar is a validation error — the generated insert code relies on `has_X()` proto presence methods that only exist for `optional` fields.

## Requirements

- CMake 3.20+
- C++20 compiler (GCC 11+ or Clang 14+)
- `libprotobuf` / `libprotoc` (protobuf 3.x)
- `protoc` on `$PATH`
- `libpqxx` 7.x (for the TimescaleDB C++ insert compile test)

On Ubuntu/Debian:
```bash
apt install protobuf-compiler libprotobuf-dev libpqxx-dev
```

`clickhouse-cpp` is bundled as a git submodule (`third_party/clickhouse-cpp`) and built automatically if not found on the system.

## Build

```bash
# First-time clone: initialise the clickhouse-cpp submodule
git submodule update --init

cmake -S . -B build
cmake --build build
```

The plugin binary is `build/protoc-gen-dbddl`. The build also compiles a `generated_inserts` static library that runs the plugin against the bundled example protos and compiles the output — a build failure here means the generator produced invalid C++.

## Install

```bash
cmake --install build --prefix /usr/local
```

This installs:
- `protoc-gen-dbddl` to `/usr/local/bin` (automatically discoverable by `protoc`)
- `db_options.proto` to `/usr/local/share/proto-ddl-compiler/proto/`

After installing, reference the proto file via `protoc`'s `-I` flag:

```bash
protoc \
  --dbddl_out=out/ \
  -I /usr/local/share/proto-ddl-compiler/proto \
  -I proto/ \
  proto/example_trade.proto
```

## Running the plugin

```bash
protoc \
  --plugin=protoc-gen-dbddl=./build/protoc-gen-dbddl \
  --dbddl_out=out/ \
  -I proto/ \
  proto/example_trade.proto
```

This produces:
- `out/example_trade.clickhouse.sql` — ClickHouse DDL
- `out/example_trade.timescaledb.sql` — TimescaleDB DDL
- `out/example_trade.ch_insert.h` / `out/example_trade.ch_insert.cc` — ClickHouse C++ insert functions
- `out/example_trade.pg_insert.h` / `out/example_trade.pg_insert.cc` — TimescaleDB C++ insert functions

## C++ insert backends

For each annotated message the plugin generates typed C++ insert functions inside the `dbddl` namespace.

### ClickHouse

Uses the [`clickhouse-cpp`](https://github.com/ClickHouse/clickhouse-cpp) client library.

**`example_trade.ch_insert.h`**
```cpp
// Generated by protoc-gen-dbddl. DO NOT EDIT.
#pragma once

#include <memory>
#include <vector>

#include <clickhouse/client.h>

#include "example_trade.pb.h"

namespace dbddl {

// Insert rows into ClickHouse table 'trades'.
void InsertTrades(
    clickhouse::Client& client,
    const std::vector<example::Trade>& rows);

}  // namespace dbddl
```

The generated source builds typed column objects (`ColumnDateTime64`, `ColumnString`, `ColumnFloat64`, `ColumnInt32`, `ColumnLowCardinalityT<clickhouse::ColumnString>`, `ColumnNullableT<clickhouse::ColumnUUID>`) and appends all rows before calling `client.Insert("trades", block)`.

### TimescaleDB

Uses [`libpqxx`](https://github.com/jtv/libpqxx) 7.x with `pqxx::stream_to`.

**`example_trade.pg_insert.h`**
```cpp
// Generated by protoc-gen-dbddl. DO NOT EDIT.
#pragma once

#include <optional>
#include <string>
#include <vector>

#include <pqxx/pqxx>

#include "example_trade.pb.h"

namespace dbddl {

// Insert rows into TimescaleDB table 'trades'.
void InsertTrades(
    pqxx::connection& conn,
    const std::vector<example::Trade>& rows);

}  // namespace dbddl
```

The generated source opens a `pqxx::work` transaction, streams rows via `pqxx::stream_to::table` using `std::make_tuple`, then calls `stream.complete()` and `txn.commit()`.

### Field-type handling in insert code

| Field kind | ClickHouse | TimescaleDB |
|---|---|---|
| Timestamp | `ColumnDateTime64(N)` with `seconds()*mult + nanos()/div` | `gmtime_r` + `strftime` formatted as `"YYYY-MM-DD HH:MM:SS.nnnnnnnnn+00"` |
| Enum | `ColumnLowCardinalityT<clickhouse::ColumnString>` + `EnumType_Name(row.field())` | `EnumType_Name(row.field())` string |
| Nullable | `ColumnNullableT<clickhouse::ColT>` + `has_field()` / `std::nullopt` | `std::optional<T>` + `has_field()` ternary |
| Repeated | `ColumnArrayT<clickhouse::ColT>` + `AppendAsColumn` | PostgreSQL array literal `{v1,v2,...}` |
| `dbddl.UUID` | `ColumnUUID` + `memcpy` of 16 bytes into `{first, second}` | formatted UUID string via `snprintf` |

### Additional runtime dependencies

Add to your project's link libraries when using the generated insert code:

| Backend | Library |
|---|---|
| ClickHouse | `clickhouse-cpp` |
| TimescaleDB | `libpqxx` |

## Running tests

```bash
cmake -S . -B build
cmake --build build
ctest --test-dir build --output-on-failure
```
