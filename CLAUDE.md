# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build & Test Commands

```bash
# First-time clone: initialise the clickhouse-cpp submodule
git submodule update --init

# Configure
cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug

# Build everything (plugin + unit tests + generated insert compile test)
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
    → DbddlGenerator::Generate()              # generator.cpp
      → ExtractTablesFromFile()               # descriptor_utils.cpp
          BuildColumn() per field             # mapper.cpp: type mapping
      → ValidateClickHouseTables()            # validate.cpp
      → ValidateTimescaleTables()             # validate.cpp
      → RenderClickHouseDDL()                 # backends/clickhouse_renderer.cpp
      → RenderClickHouseInsert()              # backends/clickhouse_insert_renderer.cpp
      → RenderTimescaleDDL()                  # backends/timescale_renderer.cpp
      → RenderTimescaleInsert()               # backends/timescale_insert_renderer.cpp
      → WriteFile() × 6                       # generator.cpp (.sql + .h + .cc per backend)
```

### IR (Intermediate Representation)

`ir.hpp` defines `TableIR` and `ColumnIR` — the decoupled representation that flows between extraction, validation, and rendering. Neither the renderers nor the validators touch protobuf descriptors directly.

### Custom options

`proto/db_options.proto` defines all annotation options (e.g. `ch_table`, `ts_time_column`, `db_nullable`). It is compiled into `db_options.pb.h/.cc` at build time via `protobuf_generate_cpp`. `descriptor_utils.cpp` reads these options via `HasExtension`/`GetExtension`.

### Nullability (proto3)

In proto3, `is_required()` is always false. Nullability defaults to `has_presence() && !is_required()` — so implicit singular fields are `NOT NULL`, while `optional`-qualified fields are nullable. Message fields (e.g. `google.protobuf.Timestamp`, `dbddl.UUID`) always have presence and are therefore nullable unless overridden with `db_nullable = false`. The `db_nullable` field option always overrides.

`ColumnIR` carries two related but distinct flags:
- `nullable` — drives SQL DDL (`NOT NULL` vs. nullable column)
- `has_proto_presence` — `true` when `field.has_presence()` — guards `has_X()` calls and `ColumnNullableT` / `std::optional` in generated insert code

Using `db_nullable = true` on a field without proto presence (a plain proto3 scalar) is a validation error: the generated insert code cannot represent NULL without a `has_X()` method.

### Embedded sub-messages (`db_embed_prefix`)

The `db_embed_prefix` field option (number 51013) on a message-type field causes its sub-fields to be flattened into the parent table with a column name prefix. For example, `GeoPoint loc = 3 [(dbddl.db_embed_prefix) = "loc"]` produces columns `loc_lat`, `loc_lon`, `loc_label`.

- `ColumnIR.embed_accessor_prefix` holds the full proto accessor chain including trailing dot (e.g. `"loc()."`) so renderers emit `row.loc().lat()`.
- `BuildEmbeddedColumns()` in `descriptor_utils.cpp` handles recursion for nested embeds.
- `Timestamp` and `dbddl.UUID` fields within an embedded message are first-class types and expand normally via `BuildColumn`.

### Proto3 optional support

`DbddlGenerator::GetSupportedFeatures()` advertises `FEATURE_PROTO3_OPTIONAL`. Without this, protoc rejects any proto3 file that uses the `optional` keyword.

### Output

Only backend files with at least one annotated table are written. A proto with only `ch_table` annotations produces only `.clickhouse.sql`, `.ch_insert.h`, and `.ch_insert.cc`.

### Tests

Tests live in `tests/`. The mapper tests use a dedicated `tests/mapper_test_types.proto` (compiled by CMake alongside the tests) to obtain real `FieldDescriptor` instances. Validate and renderer tests construct `TableIR`/`ColumnIR` structs directly — no protobuf involvement.

The `generated_inserts` CMake target provides a compile-time correctness check: it runs the plugin against `proto/example_trade.proto` and `proto/kitchen_sink.proto` at build time and compiles the generated `.cc` files as a static library. A build failure means the generator produced invalid C++. Add new protos to this target via the `generate_insert_code()` CMake helper in `CMakeLists.txt`.

### Third-party dependencies

`third_party/clickhouse-cpp` is a git submodule. CMake prefers a system install of `clickhouse-cpp` (`find_library` + `find_path`); if not found it falls back to the submodule. `libpqxx` is expected to be installed system-wide and is located via `pkg-config`.
