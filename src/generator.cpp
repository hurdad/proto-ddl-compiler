#include "generator.hpp"

#include <google/protobuf/descriptor.h>
#include <google/protobuf/io/zero_copy_stream.h>

#include <algorithm>
#include <cstring>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include "backends/clickhouse_renderer.hpp"
#include "backends/timescale_renderer.hpp"
#include "descriptor_utils.hpp"
#include "validate.hpp"

namespace {

std::string BaseName(const std::string& path) {
  const std::string::size_type slash = path.find_last_of('/');
  const std::string name = (slash == std::string::npos) ? path : path.substr(slash + 1);
  const std::string::size_type dot = name.rfind('.');
  return (dot == std::string::npos) ? name : name.substr(0, dot);
}

bool WriteFile(google::protobuf::compiler::GeneratorContext* context,
               const std::string& path,
               const std::string& contents,
               std::string* error) {
  auto output = std::unique_ptr<google::protobuf::io::ZeroCopyOutputStream>(context->Open(path));
  if (!output) {
    *error = "Failed to open output file: " + path;
    return false;
  }

  size_t written = 0;
  while (written < contents.size()) {
    void* data = nullptr;
    int size = 0;
    if (!output->Next(&data, &size) || data == nullptr || size <= 0) {
      *error = "Failed while writing output file: " + path;
      return false;
    }

    const size_t remaining = contents.size() - written;
    const size_t to_copy = std::min(remaining, static_cast<size_t>(size));
    std::memcpy(data, contents.data() + written, to_copy);
    written += to_copy;

    if (to_copy < static_cast<size_t>(size)) {
      output->BackUp(size - static_cast<int>(to_copy));
    }
  }
  return true;
}

std::string JoinErrors(const std::vector<std::string>& errors) {
  std::ostringstream out;
  for (size_t i = 0; i < errors.size(); ++i) {
    out << "[" << (i + 1) << "] " << errors[i];
    if (i + 1 < errors.size()) {
      out << '\n';
    }
  }
  return out.str();
}

}  // namespace

bool DbddlGenerator::Generate(const google::protobuf::FileDescriptor* file,
                              const std::string& /*parameter*/,
                              google::protobuf::compiler::GeneratorContext* context,
                              std::string* error) const {
  if (file == nullptr) {
    *error = "Internal error: null FileDescriptor";
    return false;
  }

  auto extracted = ExtractTablesFromFile(*file);

  auto ch_errors = ValidateClickHouseTables(extracted.clickhouse_tables);
  auto ts_errors = ValidateTimescaleTables(extracted.timescale_tables);
  extracted.errors.insert(extracted.errors.end(), ch_errors.begin(), ch_errors.end());
  extracted.errors.insert(extracted.errors.end(), ts_errors.begin(), ts_errors.end());

  if (!extracted.errors.empty()) {
    *error = "dbddl validation failed for file '" + file->name() + "':\n" + JoinErrors(extracted.errors);
    return false;
  }

  const std::string base = BaseName(file->name());

  if (!extracted.clickhouse_tables.empty()) {
    const std::string ch_sql = RenderClickHouseDDL(extracted.clickhouse_tables);
    if (!WriteFile(context, base + ".clickhouse.sql", ch_sql, error)) {
      return false;
    }
  }

  if (!extracted.timescale_tables.empty()) {
    const std::string ts_sql = RenderTimescaleDDL(extracted.timescale_tables, extracted.pg_enum_types);
    if (!WriteFile(context, base + ".timescaledb.sql", ts_sql, error)) {
      return false;
    }
  }

  return true;
}
