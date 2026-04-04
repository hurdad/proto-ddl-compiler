#pragma once

#include <string>
#include <vector>

#include "ir.hpp"

namespace google::protobuf {
class Descriptor;
class FileDescriptor;
}

struct TableExtractionResult {
  std::vector<TableIR> clickhouse_tables;
  std::vector<TableIR> timescale_tables;
  std::vector<std::string> pg_enum_types;  // CREATE TYPE ... AS ENUM (...) statements
  std::vector<std::string> errors;
};

TableExtractionResult ExtractTablesFromFile(const google::protobuf::FileDescriptor& file);
