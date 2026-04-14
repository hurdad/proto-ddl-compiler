#include "backends/clickhouse_renderer.hpp"

#include <sstream>

namespace {

std::string EscapeSqlString(const std::string& s) {
  std::string out;
  out.reserve(s.size());
  for (char c : s) {
    if (c == '\'') out += "''";
    else out += c;
  }
  return out;
}

}  // namespace

std::string RenderClickHouseDDL(const std::vector<TableIR>& tables) {
  std::ostringstream out;
  for (size_t t = 0; t < tables.size(); ++t) {
    const auto& table = tables[t];

    // Collect columns that need data-skipping indexes.
    std::vector<const ColumnIR*> indexed;
    for (const auto& col : table.columns) {
      if (col.db_index) indexed.push_back(&col);
    }

    out << "CREATE TABLE " << table.name << "\n(\n";
    if (table.uuid_pk) {
      out << "  id UUID DEFAULT generateUUIDv4(),\n";
    } else if (!table.auto_pk_name.empty()) {
      out << "  " << table.auto_pk_name << " UInt64,\n";
    }
    for (size_t i = 0; i < table.columns.size(); ++i) {
      const auto& col = table.columns[i];
      out << "  " << col.name << " ";
      if (col.nullable && !col.repeated) {
        // ClickHouse forbids Nullable(LowCardinality(T)); the correct form is
        // LowCardinality(Nullable(T)).  Detect by prefix and invert the wrapping.
        const auto& ct = col.type_clickhouse;
        if (ct.size() > 15 && ct.compare(0, 15, "LowCardinality(") == 0 &&
            ct.back() == ')') {
          const std::string inner = ct.substr(15, ct.size() - 16);
          out << "LowCardinality(Nullable(" << inner << "))";
        } else {
          out << "Nullable(" << ct << ")";
        }
      } else {
        out << col.type_clickhouse;
      }
      if (!col.db_default.empty()) {
        out << " DEFAULT " << col.db_default;
      }
      if (!col.ch_codec.empty()) {
        out << " CODEC(" << col.ch_codec << ")";
      }
      if (!col.db_comment.empty()) {
        out << " COMMENT '" << EscapeSqlString(col.db_comment) << "'";
      }
      if (i + 1 < table.columns.size() || !indexed.empty()) {
        out << ",";
      }
      out << "\n";
    }
    for (size_t i = 0; i < indexed.size(); ++i) {
      const auto* col = indexed[i];
      const std::string idx_type =
          col->ch_skip_index_type.empty() ? "minmax" : col->ch_skip_index_type;
      const uint32_t gran =
          col->ch_skip_index_granularity == 0 ? 1u : col->ch_skip_index_granularity;
      out << "  INDEX idx_" << col->name << " " << col->name
          << " TYPE " << idx_type << " GRANULARITY " << gran;
      if (i + 1 < indexed.size()) out << ",";
      out << "\n";
    }
    out << ")\n";
    out << "ENGINE = " << table.ch_engine << "\n";
    if (!table.ch_partition_by.empty()) {
      out << "PARTITION BY " << table.ch_partition_by << "\n";
    }
    out << "ORDER BY (" << table.ch_order_by << ")\n";
    if (!table.ch_sample_by.empty()) {
      out << "SAMPLE BY " << table.ch_sample_by << "\n";
    }
    if (!table.ch_ttl.empty()) {
      out << "TTL " << table.ch_ttl << "\n";
    }
    if (!table.ch_settings.empty()) {
      out << "SETTINGS " << table.ch_settings << "\n";
    }
    out << ";\n";

    if (t + 1 < tables.size()) {
      out << "\n";
    }
  }
  return out.str();
}
