#include "backends/timescale_renderer.hpp"

#include <sstream>

std::string RenderTimescaleDDL(const std::vector<TableIR>& tables) {
  std::ostringstream out;

  for (size_t t = 0; t < tables.size(); ++t) {
    const auto& table = tables[t];
    out << "CREATE TABLE " << table.name << " (\n";
    for (size_t i = 0; i < table.columns.size(); ++i) {
      const auto& col = table.columns[i];
      out << "  " << col.name << " " << col.type_postgres;
      if (!col.nullable && !col.repeated) {
        out << " NOT NULL";
      }
      if (i + 1 < table.columns.size()) {
        out << ",";
      }
      out << "\n";
    }
    out << ");\n\n";
    out << "SELECT create_hypertable('" << table.name << "', by_range('" << table.ts_time_column << "'";
    if (!table.ts_chunk_interval.empty()) {
      out << ", INTERVAL '" << table.ts_chunk_interval << "'";
    }
    out << "));\n";

    if (t + 1 < tables.size()) {
      out << "\n";
    }
  }

  return out.str();
}
