#include "validate.hpp"

#include <algorithm>

std::vector<std::string> ValidateClickHouseTables(const std::vector<TableIR>& tables) {
  std::vector<std::string> errors;
  for (const auto& table : tables) {
    if (table.ch_order_by.empty()) {
      errors.push_back("ClickHouse table '" + table.name + "' is missing required ch_order_by option");
    }
    for (const auto& col : table.columns) {
      if (col.nullable && !col.has_proto_presence && !col.repeated) {
        errors.push_back(
            "ClickHouse table '" + table.name + "', column '" + col.name +
            "': db_nullable=true requires the 'optional' keyword (field has no proto presence)");
      }
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
    auto it = std::find_if(table.columns.begin(), table.columns.end(),
                           [&](const ColumnIR& c) { return c.name == table.ts_time_column; });
    if (it == table.columns.end()) {
      errors.push_back(
          "Timescale table '" + table.name + "' references missing time column '" + table.ts_time_column + "'");
    } else if (it->nullable) {
      errors.push_back(
          "Timescale table '" + table.name + "' time column '" + table.ts_time_column +
          "' must be NOT NULL (remove 'optional' or set db_nullable=false)");
    }
    for (const auto& col : table.columns) {
      if (col.nullable && !col.has_proto_presence && !col.repeated) {
        errors.push_back(
            "Timescale table '" + table.name + "', column '" + col.name +
            "': db_nullable=true requires the 'optional' keyword (field has no proto presence)");
      }
    }
  }
  return errors;
}
