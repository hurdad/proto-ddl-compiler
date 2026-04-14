#include "validate.hpp"

#include <algorithm>

namespace {

bool HasColumnNamed(const TableIR& table, const std::string& name) {
  return std::any_of(table.columns.begin(), table.columns.end(), [&](const ColumnIR& col) {
    return col.name == name;
  });
}

}  // namespace

std::vector<std::string> ValidateClickHouseTables(const std::vector<TableIR>& tables) {
  std::vector<std::string> errors;
  for (const auto& table : tables) {
    if (table.ch_order_by.empty()) {
      errors.push_back("ClickHouse table '" + table.name + "' is missing required ch_order_by option");
    }
  }
  return errors;
}

std::vector<std::string> ValidateTimescaleTables(const std::vector<TableIR>& tables) {
  std::vector<std::string> errors;
  for (const auto& table : tables) {
    if (table.ts_time_column.empty()) {
      errors.push_back("Timescale table '" + table.name + "' is missing required ts_time_column option");
      continue;
    }
    if (!HasColumnNamed(table, table.ts_time_column)) {
      errors.push_back(
          "Timescale table '" + table.name + "' references missing time column '" + table.ts_time_column + "'");
    }
  }
  return errors;
}
