#pragma once

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
};

struct TableIR {
  std::string name;
  std::vector<ColumnIR> columns;

  std::string ch_engine;
  std::string ch_partition_by;
  std::string ch_order_by;

  std::string ts_time_column;
  std::string ts_chunk_interval;

  // Insert code generation
  std::string proto_cpp_type;   // e.g. "example::Trade"
  std::string proto_include;    // e.g. "example_trade.pb.h"
};

// Returned by insert renderers — header (.h) and source (.cc) file contents.
struct InsertFiles {
  std::string header;
  std::string source;
};
