#include "backends/timescale_renderer.hpp"

#include <sstream>

std::string RenderTimescaleDDL(const std::vector<TableIR>& tables,
                               const std::vector<std::string>& pg_enum_types) {
  std::ostringstream out;

  for (const auto& ddl : pg_enum_types) {
    out << ddl << "\n";
  }
  if (!pg_enum_types.empty()) {
    out << "\n";
  }

  for (size_t t = 0; t < tables.size(); ++t) {
    const auto& table = tables[t];

    // CREATE TABLE
    out << "CREATE TABLE " << table.name << " (\n";
    if (table.uuid_pk) {
      out << "  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,\n";
    } else if (!table.auto_pk_name.empty()) {
      out << "  " << table.auto_pk_name << " BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,\n";
    }
    for (size_t i = 0; i < table.columns.size(); ++i) {
      const auto& col = table.columns[i];
      out << "  " << col.name << " " << col.type_postgres;
      if (!col.db_default.empty()) {
        out << " DEFAULT " << col.db_default;
      }
      if (!col.nullable && !col.repeated) {
        out << " NOT NULL";
      }
      if (i + 1 < table.columns.size()) {
        out << ",";
      }
      out << "\n";
    }
    out << ");\n";

    // COMMENT ON COLUMN
    for (const auto& col : table.columns) {
      if (col.db_comment.empty()) continue;
      out << "COMMENT ON COLUMN " << table.name << "." << col.name
          << " IS '" << col.db_comment << "';\n";
    }

    out << "\n";

    // create_hypertable
    out << "SELECT create_hypertable('" << table.name << "', by_range('" << table.ts_time_column << "'";
    if (!table.ts_chunk_interval.empty()) {
      out << ", INTERVAL '" << table.ts_chunk_interval << "'";
    }
    out << "));\n";

    // Indexes
    for (const auto& col : table.columns) {
      if (!col.db_index) continue;
      out << "CREATE INDEX ON " << table.name;
      if (!col.pg_index_using.empty()) {
        out << " USING " << col.pg_index_using;
      }
      out << " (" << col.name << ");\n";
    }

    // Compression
    const bool has_compress = !table.ts_compress_after.empty() ||
                              !table.ts_compress_segmentby.empty() ||
                              !table.ts_compress_orderby.empty();
    if (has_compress) {
      out << "ALTER TABLE " << table.name << " SET (\n";
      out << "  timescaledb.compress";
      if (!table.ts_compress_segmentby.empty()) {
        out << ",\n  timescaledb.compress_segmentby = '" << table.ts_compress_segmentby << "'";
      }
      if (!table.ts_compress_orderby.empty()) {
        out << ",\n  timescaledb.compress_orderby = '" << table.ts_compress_orderby << "'";
      }
      out << "\n);\n";
      if (!table.ts_compress_after.empty()) {
        out << "SELECT add_compression_policy('" << table.name
            << "', INTERVAL '" << table.ts_compress_after << "');\n";
      }
    }

    // Retention
    if (!table.ts_retention.empty()) {
      out << "SELECT add_retention_policy('" << table.name
          << "', INTERVAL '" << table.ts_retention << "');\n";
    }

    if (t + 1 < tables.size()) {
      out << "\n";
    }
  }

  return out.str();
}
