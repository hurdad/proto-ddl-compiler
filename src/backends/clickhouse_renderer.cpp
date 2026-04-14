#include "backends/clickhouse_renderer.hpp"

#include <sstream>

std::string RenderClickHouseDDL(const std::vector<TableIR>& tables) {
  std::ostringstream out;
  for (size_t t = 0; t < tables.size(); ++t) {
    const auto& table = tables[t];
    out << "CREATE TABLE " << table.name << "\n(\n";
    for (size_t i = 0; i < table.columns.size(); ++i) {
      const auto& col = table.columns[i];
      out << "  " << col.name << " ";
      if (col.nullable && !col.repeated) {
        out << "Nullable(" << col.type_clickhouse << ")";
      } else {
        out << col.type_clickhouse;
      }
      if (i + 1 < table.columns.size()) {
        out << ",";
      }
      out << "\n";
    }
    out << ")\n";
    out << "ENGINE = " << table.ch_engine << "\n";
    if (!table.ch_partition_by.empty()) {
      out << "PARTITION BY " << table.ch_partition_by << "\n";
    }
    out << "ORDER BY (" << table.ch_order_by << ");\n";

    if (t + 1 < tables.size()) {
      out << "\n";
    }
  }
  return out.str();
}
