# proto-ddl-compiler

A `protoc` plugin (`protoc-gen-dbddl`) that reads proto3 message definitions annotated with custom options and generates DDL for **ClickHouse** and **TimescaleDB**.

## Overview

Annotate your messages with table/column options, then run `protoc` with this plugin to emit `.clickhouse.sql` and `.timescaledb.sql` files.

```proto
syntax = "proto3";

import "db_options.proto";

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

  int64  timestamp = 1 [(dbddl.ch_column_type) = "DateTime64(3)"];
  string symbol    = 2;
  double price     = 3;
  int32  size      = 4;
  Side   side      = 5;
}
```

Given the example above, the plugin produces:

**`example_trade.clickhouse.sql`**
```sql
CREATE TABLE trades
(
  timestamp DateTime64(3),
  symbol String,
  price Float64,
  size Int32,
  side LowCardinality(String)
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
  side side NOT NULL
);

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
| `ts_chunk_interval`  | Chunk time interval (informational)                |

### Field options

| Option           | Description                                          |
|------------------|------------------------------------------------------|
| `ch_column_type` | Override ClickHouse column type                      |
| `pg_column_type` | Override PostgreSQL/TimescaleDB column type          |
| `db_nullable`    | Override nullability (`true`/`false`)                |
| `db_name`        | Override column name in the output DDL               |

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
| `string`/`bytes`     | `String`     | `TEXT`            |
| `google.protobuf.Timestamp` | `DateTime64(3)` | `TIMESTAMPTZ` |
| `repeated T`         | `Array(T)`   | `T[]`             |
| `enum`               | `LowCardinality(String)` | native `ENUM` type |

Nullability defaults to `false` for implicit proto3 fields and `true` for `optional`-qualified fields. Use the `db_nullable` option to override.

## Requirements

- CMake 3.20+
- C++20 compiler (GCC 11+ or Clang 14+)
- `libprotobuf` / `libprotoc` (protobuf 3.x)
- `protoc` on `$PATH`

On Ubuntu/Debian:
```bash
apt install protobuf-compiler libprotobuf-dev
```

## Build

```bash
cmake -S . -B build
cmake --build build
```

The plugin binary is `build/protoc-gen-dbddl`.

## Install

```bash
cmake --install build --prefix /usr/local
```

This installs `protoc-gen-dbddl` to `/usr/local/bin`, making it automatically discoverable by `protoc`.

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
#include <clickhouse/client.h>
#include "example_trade.pb.h"

namespace dbddl {
void InsertTrades(clickhouse::Client& client,
                  const std::vector<example::Trade>& rows);
}
```

The generated source builds typed column objects (`ColumnDateTime64`, `ColumnString`, `ColumnFloat64`, `ColumnInt32`) and appends all rows before calling `client.Insert("trades", block)`.

### TimescaleDB

Uses [`libpqxx`](https://github.com/jtv/libpqxx) with binary streaming.

**`example_trade.pg_insert.h`**
```cpp
#include <pqxx/pqxx>
#include "example_trade.pb.h"

namespace dbddl {
void InsertTrades(pqxx::connection& conn,
                  const std::vector<example::Trade>& rows);
}
```

The generated source opens a `pqxx::work` transaction, streams rows via `pqxx::stream_to::table` using `std::make_tuple`, then calls `stream.complete()` and `txn.commit()`.

### Field-type handling in insert code

| Field kind | ClickHouse | TimescaleDB |
|---|---|---|
| Timestamp | `ColumnDateTime64(N)` with `seconds()*mult + nanos()/div` | `std::chrono::system_clock::from_time_t` + `nanoseconds()` |
| Enum | `ColumnLowCardinalityT<ColumnString>` + `EnumType_Name(row.field())` | `EnumType_Name(row.field())` string |
| Nullable | `ColumnNullableT<ColT>` + `has_field()` / `std::nullopt` | `std::optional<T>` + `has_field()` ternary |
| Repeated | `ColumnArrayT<ColT>` + `AppendAsColumn` | PostgreSQL array literal `{v1,v2,...}` |

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
