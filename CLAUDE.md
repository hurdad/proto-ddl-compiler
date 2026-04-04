# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build & Test Commands

```bash
# Configure
cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug

# Build everything (plugin + tests)
cmake --build build -j$(nproc)

# Run all tests
ctest --test-dir build --output-on-failure

# Run a single test binary directly (supports --gtest_filter)
./build/dbddl_tests --gtest_filter='MapperTest.*'
./build/dbddl_tests --gtest_filter='ValidateTimescaleTest.NullableTimeColumn'

# Run the plugin against the example proto
mkdir -p out
protoc --plugin=protoc-gen-dbddl=./build/protoc-gen-dbddl --dbddl_out=out/ -I proto/ proto/example_trade.proto
```

## Architecture

This is a `protoc` plugin. `protoc` invokes `protoc-gen-dbddl` via stdin/stdout using the protobuf `CodeGeneratorRequest`/`CodeGeneratorResponse` wire protocol — `PluginMain` in `main.cpp` handles that framing.

### Pipeline

```
.proto file
  → protoc (parses, resolves imports, builds FileDescriptor)
    → DbddlGenerator::Generate()          # generator.cpp
      → ExtractTablesFromFile()            # descriptor_utils.cpp
          BuildColumn() per field          # mapper.cpp: type mapping
      → ValidateClickHouseTables()         # validate.cpp
      → ValidateTimescaleTables()          # validate.cpp
      → RenderClickHouseDDL()              # backends/clickhouse_renderer.cpp
      → RenderTimescaleDDL()              # backends/timescale_renderer.cpp
      → WriteFile() × 2                   # generator.cpp
```

### IR (Intermediate Representation)

`ir.hpp` defines `TableIR` and `ColumnIR` — the decoupled representation that flows between extraction, validation, and rendering. Neither the renderers nor the validators touch protobuf descriptors directly.

### Custom options

`proto/db_options.proto` defines all annotation options (e.g. `ch_table`, `ts_time_column`, `db_nullable`). It is compiled into `db_options.pb.h/.cc` at build time via `protobuf_generate_cpp`. `descriptor_utils.cpp` reads these options via `HasExtension`/`GetExtension`.

### Nullability (proto3)

In proto3, `is_required()` is always false. Nullability defaults to `has_presence() && !is_required()` — so implicit singular fields are `NOT NULL`, while `optional`-qualified fields are nullable. The `db_nullable` field option always overrides.

### Output

Only backend files with at least one annotated table are written. A proto with only `ch_table` annotations produces only a `.clickhouse.sql` file.

### Tests

Tests live in `tests/`. The mapper tests use a dedicated `tests/mapper_test_types.proto` (compiled by CMake alongside the tests) to obtain real `FieldDescriptor` instances. Validate and renderer tests construct `TableIR`/`ColumnIR` structs directly — no protobuf involvement.
