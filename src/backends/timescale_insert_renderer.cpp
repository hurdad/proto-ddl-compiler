#include "backends/timescale_insert_renderer.hpp"

#include <algorithm>
#include <cctype>
#include <sstream>
#include <string>

namespace {

std::string Capitalize(const std::string& s) {
  if (s.empty()) return s;
  std::string r = s;
  r[0] = static_cast<char>(std::toupper(static_cast<unsigned char>(r[0])));
  return r;
}

// Accessor helpers that respect embed_accessor_prefix.
std::string FieldAcc(const ColumnIR& col, const std::string& row, const std::string& index = "") {
  return row + "." + col.embed_accessor_prefix + col.proto_field_name + "(" + index + ")";
}
std::string HasAcc(const ColumnIR& col, const std::string& row) {
  return row + "." + col.embed_accessor_prefix + "has_" + col.proto_field_name + "()";
}
std::string SizeAcc(const ColumnIR& col, const std::string& row) {
  return row + "." + col.embed_accessor_prefix + col.proto_field_name + "_size()";
}

// Emit the tuple element expression for one column into `src`.
void EmitTupleElem(std::ostream& src, const ColumnIR& col,
                   const std::string& row) {
  if (col.repeated) {
    // Build PostgreSQL array literal: {val1,val2,...}
    src << "        [&]() -> std::string {\n"
        << "          std::string arr = \"{\";\n"
        << "          for (int i = 0; i < " << SizeAcc(col, row) << "; ++i) {\n"
        << "            if (i > 0) arr += \",\";\n";
    if (col.field_kind == FieldKind::kEnum) {
      src << "            arr += " << col.enum_cpp_type << "_Name("
          << FieldAcc(col, row, "i") << ");\n";
    } else if (col.field_kind == FieldKind::kString ||
               col.field_kind == FieldKind::kBytes) {
      // Double-quote each element and escape backslash/double-quote for PG.
      src << "            {\n"
          << "              const auto& _elem = " << FieldAcc(col, row, "i") << ";\n"
          << "              arr += '\"';\n"
          << "              for (char _c : _elem) {\n"
          << "                if (_c == '\\\\') arr += \"\\\\\\\\\";\n"
          << "                else if (_c == '\"') arr += \"\\\\\\\"\";\n"
          << "                else arr += _c;\n"
          << "              }\n"
          << "              arr += '\"';\n"
          << "            }\n";
    } else {
      src << "            arr += std::to_string(" << FieldAcc(col, row, "i") << ");\n";
    }
    src << "          }\n"
        << "          arr += \"}\";\n"
        << "          return arr;\n"
        << "        }()";
    return;
  }

  if (col.field_kind == FieldKind::kUUID) {
    const std::string bytes_acc = row + "." + col.embed_accessor_prefix + col.proto_field_name + "()" +
                                   (col.uuid_via_message ? ".value()" : "");
    auto emit_uuid_lambda = [&](const std::string& acc) {
      src << "[&]() -> std::string {\n"
          << "          const auto& _b = " << acc << ";\n"
          << "          if (_b.size() != 16) return \"\";\n"
          << "          char _buf[37];\n"
          << "          const auto* _u = reinterpret_cast<const uint8_t*>(_b.data());\n"
          << "          snprintf(_buf, sizeof(_buf),\n"
          << "              \"%02x%02x%02x%02x-%02x%02x-%02x%02x-%02x%02x-%02x%02x%02x%02x%02x%02x\",\n"
          << "              _u[0],_u[1],_u[2],_u[3],_u[4],_u[5],_u[6],_u[7],\n"
          << "              _u[8],_u[9],_u[10],_u[11],_u[12],_u[13],_u[14],_u[15]);\n"
          << "          return _buf;\n"
          << "        }()";
    };
    if (col.has_proto_presence && col.nullable) {
      // Nullable UUID: return std::optional<std::string>{} when absent.
      src << HasAcc(col, row) << "\n"
          << "            ? std::optional<std::string>{";
      emit_uuid_lambda(bytes_acc);
      src << "}\n"
          << "            : std::optional<std::string>{}";
    } else {
      emit_uuid_lambda(bytes_acc);
    }
    return;
  }

  if (col.field_kind == FieldKind::kTimestamp) {
    // Format as "YYYY-MM-DD HH:MM:SS.nnnnnnnnn+00" for pqxx TIMESTAMPTZ.
    const std::string acc = row + "." + col.embed_accessor_prefix + col.proto_field_name + "()";
    auto emit_ts_lambda = [&]() {
      src << "[&]() -> std::string {\n"
          << "          char _buf[32], _out[48];\n"
          << "          time_t _sec = static_cast<time_t>(" << acc << ".seconds());\n"
          << "          struct tm _tm{};\n"
          << "          gmtime_r(&_sec, &_tm);\n"
          << "          strftime(_buf, sizeof(_buf), \"%Y-%m-%d %H:%M:%S\", &_tm);\n"
          << "          snprintf(_out, sizeof(_out), \"%s.%09d+00\", _buf, "
          << acc << ".nanos());\n"
          << "          return _out;\n"
          << "        }()";
    };
    if (col.has_proto_presence && col.nullable) {
      src << HasAcc(col, row) << "\n"
          << "            ? std::optional<std::string>{";
      emit_ts_lambda();
      src << "}\n"
          << "            : std::optional<std::string>{}";
    } else {
      emit_ts_lambda();
    }
    return;
  }

  if (col.has_proto_presence && col.nullable) {
    const std::string cpp_opt_type = [&]() -> std::string {
      switch (col.field_kind) {
        case FieldKind::kInt32:  return "std::optional<int32_t>";
        case FieldKind::kInt64:  return "std::optional<int64_t>";
        case FieldKind::kUInt32: return "std::optional<uint32_t>";
        case FieldKind::kUInt64: return "std::optional<uint64_t>";
        case FieldKind::kFloat:  return "std::optional<float>";
        case FieldKind::kDouble: return "std::optional<double>";
        case FieldKind::kBool:   return "std::optional<bool>";
        default:                 return "std::optional<std::string>";
      }
    }();
    src << HasAcc(col, row) << "\n"
        << "            ? " << cpp_opt_type << "{";
    if (col.field_kind == FieldKind::kEnum) {
      src << col.enum_cpp_type << "_Name(" << FieldAcc(col, row) << ")";
    } else {
      src << FieldAcc(col, row);
    }
    src << "}\n"
        << "            : " << cpp_opt_type << "{}";
    return;
  }

  if (col.field_kind == FieldKind::kEnum) {
    src << col.enum_cpp_type << "_Name(" << FieldAcc(col, row) << ")";
    return;
  }

  src << FieldAcc(col, row);
}

void RenderFunction(std::ostream& hdr, std::ostream& src, const TableIR& table) {
  const std::string fn = "Insert" + Capitalize(table.name);
  const std::string sig_args = "    pqxx::connection& conn,\n"
                               "    const std::vector<" + table.proto_cpp_type + ">& rows)";

  // Header declaration
  hdr << "// Insert rows into TimescaleDB table '" << table.name << "'.\n";
  hdr << "void " << fn << "(\n" << sig_args << ";\n\n";

  // Build column name list for stream_to
  std::ostringstream col_list;
  col_list << "{";
  for (size_t i = 0; i < table.columns.size(); ++i) {
    if (i > 0) col_list << ", ";
    col_list << "\"" << table.columns[i].name << "\"";
  }
  col_list << "}";

  // Source definition
  std::ostringstream body;
  body << "  pqxx::work txn{conn};\n";
  body << "  auto stream = pqxx::stream_to::table(txn, {\"" << table.name << "\"}, "
       << col_list.str() << ");\n";
  body << "  for (const auto& row : rows) {\n";
  body << "    stream << std::make_tuple(\n";
  for (size_t i = 0; i < table.columns.size(); ++i) {
    body << "        ";
    EmitTupleElem(body, table.columns[i], "row");
    if (i + 1 < table.columns.size()) body << ",\n";
    else body << "\n";
  }
  body << "    );\n";
  body << "  }\n";
  body << "  stream.complete();\n";
  body << "  txn.commit();\n";

  // Emit #error for any unsupported field types before the function definition
  // so compilation always halts with a clear message rather than a cryptic
  // "has no member" error from calling a nonexistent proto accessor.
  for (const auto& col : table.columns) {
    if (col.field_kind == FieldKind::kUnknown) {
      src << "#error \"protoc-gen-dbddl: unsupported field type for column '" << col.name << "'\"\n";
    }
  }

  src << "void " << fn << "(\n" << sig_args << " {\n";
  src << "  if (rows.empty()) return;\n\n";
  src << body.str();
  src << "}\n";
}

}  // namespace

InsertFiles RenderTimescaleInsert(const std::vector<TableIR>& tables,
                                  const std::string& base_name) {
  if (tables.empty()) return {};

  std::ostringstream hdr, src;

  // --- Header ---
  hdr << "// Generated by protoc-gen-dbddl. DO NOT EDIT.\n"
      << "#pragma once\n\n"
      << "#include <optional>\n"
      << "#include <string>\n"
      << "#include <vector>\n\n"
      << "#include <pqxx/pqxx>\n\n"
      << "#include \"" << tables[0].proto_include << "\"\n\n"
      << "namespace dbddl {\n\n";

  // --- Source ---
  src << "// Generated by protoc-gen-dbddl. DO NOT EDIT.\n"
      << "#include \"" << base_name << ".pg_insert.h\"\n\n"
      << "#include <cstdint>\n"
      << "#include <cstdio>\n"
      << "#include <cstring>\n"
      << "#include <ctime>\n"
      << "#include <optional>\n"
      << "#include <string>\n\n"
      << "namespace dbddl {\n";

  for (const auto& table : tables) {
    src << "\n";
    RenderFunction(hdr, src, table);
  }

  hdr << "}  // namespace dbddl\n";
  src << "\n}  // namespace dbddl\n";

  return {hdr.str(), src.str()};
}
