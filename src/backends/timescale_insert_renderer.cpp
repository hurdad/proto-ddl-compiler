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

// Emit the tuple element expression for one column into `src`.
// Returns true if the expression needed a pre-loop local variable.
void EmitTupleElem(std::ostream& src, const ColumnIR& col,
                   const std::string& row, bool& needs_chrono) {
  if (col.repeated) {
    // Build PostgreSQL array literal: {val1,val2,...}
    src << "        [&]() -> std::string {\n"
        << "          std::string arr = \"{\";\n"
        << "          for (int i = 0; i < " << row << "." << col.proto_field_name << "_size(); ++i) {\n"
        << "            if (i > 0) arr += \",\";\n";
    if (col.field_kind == FieldKind::kEnum) {
      src << "            arr += " << col.enum_cpp_type << "_Name("
          << row << "." << col.proto_field_name << "(i));\n";
    } else if (col.field_kind == FieldKind::kString ||
               col.field_kind == FieldKind::kBytes) {
      src << "            arr += " << row << "." << col.proto_field_name << "(i);\n";
    } else {
      src << "            arr += std::to_string(" << row << "." << col.proto_field_name << "(i));\n";
    }
    src << "          }\n"
        << "          arr += \"}\";\n"
        << "          return arr;\n"
        << "        }()";
    return;
  }

  if (col.field_kind == FieldKind::kUUID) {
    const std::string bytes_acc = row + "." + col.proto_field_name + "()" +
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
    if (col.nullable) {
      // Nullable UUID: return std::optional<std::string>{} when absent.
      src << row << ".has_" << col.proto_field_name << "()\n"
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
    needs_chrono = true;
    src << "std::chrono::system_clock::from_time_t(" << row << "." << col.proto_field_name
        << "().seconds()) +\n"
        << "            std::chrono::duration_cast<std::chrono::system_clock::duration>(\n"
        << "                std::chrono::nanoseconds(" << row << "." << col.proto_field_name
        << "().nanos()))";
    return;
  }

  if (col.nullable) {
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
    src << row << ".has_" << col.proto_field_name << "()\n"
        << "            ? " << cpp_opt_type << "{";
    if (col.field_kind == FieldKind::kEnum) {
      src << col.enum_cpp_type << "_Name(" << row << "." << col.proto_field_name << "())";
    } else {
      src << row << "." << col.proto_field_name << "()";
    }
    src << "}\n"
        << "            : " << cpp_opt_type << "{}";
    return;
  }

  if (col.field_kind == FieldKind::kEnum) {
    src << col.enum_cpp_type << "_Name(" << row << "." << col.proto_field_name << "())";
    return;
  }

  src << row << "." << col.proto_field_name << "()";
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
  bool needs_chrono = false;
  std::ostringstream body;
  body << "  pqxx::work txn{conn};\n";
  body << "  auto stream = pqxx::stream_to::table(txn, {\"" << table.name << "\"}, "
       << col_list.str() << ");\n";
  body << "  for (const auto& row : rows) {\n";
  body << "    stream << std::make_tuple(\n";
  for (size_t i = 0; i < table.columns.size(); ++i) {
    body << "        ";
    EmitTupleElem(body, table.columns[i], "row", needs_chrono);
    if (i + 1 < table.columns.size()) body << ",\n";
    else body << "\n";
  }
  body << "    );\n";
  body << "  }\n";
  body << "  stream.complete();\n";
  body << "  txn.commit();\n";

  src << "void " << fn << "(\n" << sig_args << " {\n";
  src << "  if (rows.empty()) return;\n";
  if (needs_chrono) {
    src << "  using namespace std::chrono;\n";
  }
  src << "\n" << body.str();
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
      << "#include <chrono>\n"
      << "#include <cstdint>\n"
      << "#include <cstdio>\n"
      << "#include <cstring>\n"
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
