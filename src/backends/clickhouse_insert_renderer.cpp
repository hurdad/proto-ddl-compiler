#include "backends/clickhouse_insert_renderer.hpp"

#include <algorithm>
#include <cctype>
#include <sstream>
#include <string>

namespace {

// Parse the precision out of "DateTime64(3)" or "Array(DateTime64(3))".
int ExtractDateTime64Precision(const std::string& ch_type) {
  const auto pos = ch_type.find("DateTime64(");
  if (pos == std::string::npos) return 3;
  const auto start = pos + 11;  // len("DateTime64(")
  const auto end = ch_type.find(')', start);
  if (end == std::string::npos) return 3;
  try { return std::stoi(ch_type.substr(start, end - start)); }
  catch (...) { return 3; }
}

std::string Capitalize(const std::string& s) {
  if (s.empty()) return s;
  std::string r = s;
  r[0] = static_cast<char>(std::toupper(static_cast<unsigned char>(r[0])));
  return r;
}

// Base ClickHouse column type (without nullable/array wrapping).
std::string ChBaseType(const ColumnIR& col) {
  switch (col.field_kind) {
    case FieldKind::kInt32:     return "ColumnInt32";
    case FieldKind::kInt64:     return "ColumnInt64";
    case FieldKind::kUInt32:    return "ColumnUInt32";
    case FieldKind::kUInt64:    return "ColumnUInt64";
    case FieldKind::kFloat:     return "ColumnFloat32";
    case FieldKind::kDouble:    return "ColumnFloat64";
    case FieldKind::kBool:      return "ColumnUInt8";
    case FieldKind::kString:
    case FieldKind::kBytes:     return "ColumnString";
    case FieldKind::kEnum:      return "ColumnLowCardinalityT<clickhouse::ColumnString>";
    case FieldKind::kTimestamp: return "ColumnDateTime64";
    case FieldKind::kUUID:      return "ColumnUUID";
    default:                    return "";
  }
}

// Append expression for a single value (scalar, non-nullable).
std::string ChScalarExpr(const ColumnIR& col, const std::string& row,
                          const std::string& index = "") {
  const std::string acc = row + "." + col.proto_field_name + "(" + index + ")";
  switch (col.field_kind) {
    case FieldKind::kTimestamp: {
      const int prec = ExtractDateTime64Precision(col.type_clickhouse);
      int64_t mult = 1, div = 1;
      for (int i = 0; i < prec; ++i) mult *= 10;
      for (int i = prec; i < 9; ++i) div *= 10;
      return "static_cast<uint64_t>(" + acc + ".seconds() * " +
             std::to_string(mult) + "LL + " + acc + ".nanos() / " +
             std::to_string(div) + "LL)";
    }
    case FieldKind::kEnum:
      return col.enum_cpp_type + "_Name(" + acc + ")";
    case FieldKind::kBool:
      return "static_cast<uint8_t>(" + acc + ")";
    default:
      return acc;
  }
}

void EmitColumnDecl(std::ostream& out, const ColumnIR& col) {
  const std::string base = ChBaseType(col);
  if (base.empty()) {
    out << "#error \"protoc-gen-dbddl: unsupported field type for column '" << col.name << "'\"\n";
    return;
  }

  out << "  auto col_" << col.name << " = std::make_shared<clickhouse::";
  if (col.repeated) {
    out << "ColumnArrayT<clickhouse::" << base << ">>();\n";
  } else if (col.nullable) {
    out << "ColumnNullableT<clickhouse::" << base << ">>();\n";
  } else if (col.field_kind == FieldKind::kTimestamp) {
    out << base << ">(" << ExtractDateTime64Precision(col.type_clickhouse) << ");\n";
  } else {
    out << base << ">();\n";
  }
}

void EmitAppend(std::ostream& out, const ColumnIR& col) {
  const std::string base = ChBaseType(col);
  if (base.empty()) return;

  const std::string cvar = "col_" + col.name;

  if (col.field_kind == FieldKind::kUUID && !col.repeated) {
    // UUID: copy 16 bytes into a clickhouse::UUID{first, second} pair.
    const std::string bvar = "_b_" + col.name;
    const std::string uvar = "_uuid_" + col.name;
    const std::string bytes_acc = "row." + col.proto_field_name + "()" +
                                   (col.uuid_via_message ? ".value()" : "");
    auto emit_uuid_append = [&](const std::string& accessor) {
      out << "      const auto& " << bvar << " = " << accessor << ";\n";
      out << "      clickhouse::UUID " << uvar << "{0, 0};\n";
      out << "      if (" << bvar << ".size() == 16) {\n";
      out << "        memcpy(&" << uvar << ".first,  " << bvar << ".data(),     8);\n";
      out << "        memcpy(&" << uvar << ".second, " << bvar << ".data() + 8, 8);\n";
      out << "      }\n";
    };
    if (col.nullable) {
      out << "    if (row.has_" << col.proto_field_name << "()) {\n";
      emit_uuid_append(bytes_acc);
      out << "      " << cvar << "->Append(" << uvar << ");\n";
      out << "    } else {\n";
      out << "      " << cvar << "->Append(std::nullopt);\n";
      out << "    }\n";
    } else {
      out << "    {\n";
      emit_uuid_append(bytes_acc);
      out << "      " << cvar << "->Append(" << uvar << ");\n";
      out << "    }\n";
    }
  } else if (col.repeated) {
    // Build an inner column, fill it, then AppendAsColumn.
    out << "    {\n";
    out << "      auto inner = std::make_shared<clickhouse::" << base << ">(";
    if (col.field_kind == FieldKind::kTimestamp) {
      out << ExtractDateTime64Precision(col.type_clickhouse);
    }
    out << ");\n";
    out << "      for (int i = 0; i < row." << col.proto_field_name << "_size(); ++i) {\n";
    out << "        inner->Append(" << ChScalarExpr(col, "row", "i") << ");\n";
    out << "      }\n";
    out << "      " << cvar << "->AppendAsColumn(inner);\n";
    out << "    }\n";
  } else if (col.nullable) {
    out << "    if (row.has_" << col.proto_field_name << "()) {\n";
    out << "      " << cvar << "->Append(" << ChScalarExpr(col, "row") << ");\n";
    out << "    } else {\n";
    out << "      " << cvar << "->Append(std::nullopt);\n";
    out << "    }\n";
  } else {
    out << "    " << cvar << "->Append(" << ChScalarExpr(col, "row") << ");\n";
  }
}

void RenderFunction(std::ostream& hdr, std::ostream& src,
                    const TableIR& table) {
  const std::string fn = "Insert" + Capitalize(table.name);
  const std::string sig_args = "    clickhouse::Client& client,\n"
                               "    const std::vector<" + table.proto_cpp_type + ">& rows)";

  // Header declaration
  hdr << "// Insert rows into ClickHouse table '" << table.name << "'.\n";
  hdr << "void " << fn << "(\n" << sig_args << ";\n\n";

  // Source definition
  src << "void " << fn << "(\n" << sig_args << " {\n";
  src << "  if (rows.empty()) return;\n\n";

  for (const auto& col : table.columns) EmitColumnDecl(src, col);

  src << "\n  for (const auto& row : rows) {\n";
  for (const auto& col : table.columns) EmitAppend(src, col);
  src << "  }\n\n";

  src << "  clickhouse::Block block;\n";
  for (const auto& col : table.columns) {
    if (ChBaseType(col).empty()) continue;
    src << "  block.AppendColumn(\"" << col.name << "\", col_" << col.name << ");\n";
  }
  src << "\n  client.Insert(\"" << table.name << "\", block);\n";
  src << "}\n";
}

}  // namespace

InsertFiles RenderClickHouseInsert(const std::vector<TableIR>& tables,
                                   const std::string& base_name) {
  if (tables.empty()) return {};

  std::ostringstream hdr, src;

  // --- Header ---
  hdr << "// Generated by protoc-gen-dbddl. DO NOT EDIT.\n"
      << "#pragma once\n\n"
      << "#include <memory>\n"
      << "#include <vector>\n\n"
      << "#include <clickhouse/client.h>\n\n"
      << "#include \"" << tables[0].proto_include << "\"\n\n"
      << "namespace dbddl {\n\n";

  // --- Source ---
  src << "// Generated by protoc-gen-dbddl. DO NOT EDIT.\n"
      << "#include \"" << base_name << ".ch_insert.h\"\n\n"
      << "#include <cstring>\n\n"
      << "#include <clickhouse/columns/array.h>\n"
      << "#include <clickhouse/columns/date.h>\n"
      << "#include <clickhouse/columns/lowcardinality.h>\n"
      << "#include <clickhouse/columns/nullable.h>\n"
      << "#include <clickhouse/columns/numeric.h>\n"
      << "#include <clickhouse/columns/string.h>\n"
      << "#include <clickhouse/columns/uuid.h>\n\n"
      << "namespace dbddl {\n";

  for (const auto& table : tables) {
    src << "\n";
    RenderFunction(hdr, src, table);
  }

  hdr << "}  // namespace dbddl\n";
  src << "\n}  // namespace dbddl\n";

  return {hdr.str(), src.str()};
}
