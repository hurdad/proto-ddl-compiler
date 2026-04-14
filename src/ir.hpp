#pragma once

#include <cstdint>
#include <string>
#include <vector>

enum class FieldKind {
  kUnknown,
  kInt32,
  kInt64,
  kUInt32,
  kUInt64,
  kFloat,
  kDouble,
  kBool,
  kString,
  kBytes,
  kEnum,
  kTimestamp,
  kUUID,       // bytes field annotated with db_uuid = true
};

struct ColumnIR {
  std::string name;
  std::string type_clickhouse;
  std::string type_postgres;
  bool nullable = true;
  bool repeated = false;

  // Insert code generation
  std::string proto_field_name;           // proto accessor name (may differ from name if db_name was set)
  FieldKind   field_kind = FieldKind::kUnknown;
  std::string enum_cpp_type;              // e.g. "example::Side" — non-empty only for kEnum
  bool        uuid_via_message = false;   // true when field type is dbddl.UUID (accessor needs .value())

  // Index generation
  bool        db_index = false;
  std::string pg_index_using;             // e.g. "hash" — empty means btree
  std::string ch_skip_index_type;         // e.g. "minmax", "set(100)" — empty means minmax
  uint32_t    ch_skip_index_granularity = 1;

  // Per-column options
  std::string ch_codec;                   // e.g. "Delta, LZ4" → CODEC(Delta, LZ4)
  std::string db_default;                 // DEFAULT expression, passed through as-is
  std::string db_comment;                 // column comment
};

struct TableIR {
  std::string name;
  std::vector<ColumnIR> columns;

  std::string ch_engine;
  std::string ch_partition_by;
  std::string ch_order_by;
  std::string ch_ttl;
  std::string ch_settings;
  std::string ch_sample_by;

  std::string ts_time_column;
  std::string ts_chunk_interval;
  std::string ts_compress_after;
  std::string ts_compress_segmentby;
  std::string ts_compress_orderby;
  std::string ts_retention;

  // Synthetic PK column (prepended by renderers; not a proto field).
  // If auto_pk_name is non-empty: integer identity PK with that name.
  // If uuid_pk is true: UUID PK named "id" (overrides auto_pk_name).
  std::string auto_pk_name;
  bool        uuid_pk = false;

  // Insert code generation
  std::string proto_cpp_type;   // e.g. "example::Trade"
  std::string proto_include;    // e.g. "example_trade.pb.h"
};

// Returned by insert renderers — header (.h) and source (.cc) file contents.
struct InsertFiles {
  std::string header;
  std::string source;
};
