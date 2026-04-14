#include "descriptor_utils.hpp"

#include <google/protobuf/descriptor.h>

#include <algorithm>
#include <cctype>
#include <optional>
#include <set>
#include <sstream>
#include <string>
#include <vector>

#include "db_options.pb.h"
#include "mapper.hpp"

namespace {

// Convert a proto fully-qualified name to its C++ equivalent.
// Package segments use "::", nested type segments use "_".
// e.g. "example.Trade"        (pkg "example") → "example::Trade"
// e.g. "example.Trade.Status" (pkg "example") → "example::Trade_Status"
std::string ProtoToCpp(const std::string& full_name, const std::string& package) {
  if (package.empty()) {
    std::string r = full_name;
    std::replace(r.begin(), r.end(), '.', '_');
    return r;
  }
  std::string ns;
  for (char c : package) { ns += (c == '.') ? ':' : c; }
  // Package uses ":" as a temporary placeholder; replace with "::"
  std::string cpp_ns;
  for (size_t i = 0; i < ns.size(); ++i) {
    if (ns[i] == ':') { cpp_ns += "::"; }
    else               { cpp_ns += ns[i]; }
  }
  std::string type_path = full_name.substr(package.size() + 1);
  std::replace(type_path.begin(), type_path.end(), '.', '_');
  return cpp_ns + "::" + type_path;
}

// Strip directory prefix and replace ".proto" with ".pb.h".
std::string ProtoToPbHeader(const std::string& proto_file) {
  const auto slash = proto_file.rfind('/');
  const std::string base =
      (slash == std::string::npos) ? proto_file : proto_file.substr(slash + 1);
  const auto ext = base.rfind(".proto");
  return (ext == std::string::npos) ? base + ".pb.h" : base.substr(0, ext) + ".pb.h";
}

FieldKind ToFieldKind(const google::protobuf::FieldDescriptor& field) {
  using FD = google::protobuf::FieldDescriptor;
  switch (field.cpp_type()) {
    case FD::CPPTYPE_INT32:  return FieldKind::kInt32;
    case FD::CPPTYPE_INT64:  return FieldKind::kInt64;
    case FD::CPPTYPE_UINT32: return FieldKind::kUInt32;
    case FD::CPPTYPE_UINT64: return FieldKind::kUInt64;
    case FD::CPPTYPE_FLOAT:  return FieldKind::kFloat;
    case FD::CPPTYPE_DOUBLE: return FieldKind::kDouble;
    case FD::CPPTYPE_BOOL:   return FieldKind::kBool;
    case FD::CPPTYPE_STRING:
      return field.type() == FD::TYPE_BYTES ? FieldKind::kBytes : FieldKind::kString;
    case FD::CPPTYPE_ENUM:   return FieldKind::kEnum;
    case FD::CPPTYPE_MESSAGE:
      if (field.message_type() != nullptr &&
          field.message_type()->full_name() == "dbddl.UUID")
        return FieldKind::kUUID;
      return FieldKind::kTimestamp;  // only Timestamp reaches here
    default:                  return FieldKind::kUnknown;
  }
}

std::optional<ColumnIR> BuildColumn(const google::protobuf::FieldDescriptor& field,
                                    std::vector<std::string>* errors) {
  const auto mapped = MapFieldTypes(field);
  if (!mapped.has_value()) {
    errors->push_back(
        "Unsupported field type for " + field.containing_type()->full_name() + "." + field.name());
    return std::nullopt;
  }

  ColumnIR col;
  col.name = field.options().HasExtension(dbddl::db_name)
      ? field.options().GetExtension(dbddl::db_name)
      : field.name();
  col.type_clickhouse = field.options().HasExtension(dbddl::ch_column_type)
      ? field.options().GetExtension(dbddl::ch_column_type)
      : mapped->clickhouse;
  col.type_postgres = field.options().HasExtension(dbddl::pg_column_type)
      ? field.options().GetExtension(dbddl::pg_column_type)
      : mapped->postgres;
  col.repeated = field.is_repeated();
  // In proto3, is_required() is always false; use has_presence() so that
  // explicitly-optional fields (proto2 optional / proto3 optional) are
  // nullable while implicit proto3 singular fields default to NOT NULL.
  // db_nullable always overrides this heuristic.
  col.nullable = field.options().HasExtension(dbddl::db_nullable)
      ? field.options().GetExtension(dbddl::db_nullable)
      : (field.has_presence() && !field.is_required());

  col.proto_field_name = field.name();
  col.field_kind = ToFieldKind(field);
  if (col.field_kind == FieldKind::kEnum) {
    const auto* ed = field.enum_type();
    col.enum_cpp_type = ProtoToCpp(ed->full_name(), ed->file()->package());
  }

  // db_uuid annotation on a bytes field — explicit opt-in.
  if (field.options().HasExtension(dbddl::db_uuid) &&
      field.options().GetExtension(dbddl::db_uuid)) {
    col.field_kind = FieldKind::kUUID;
    if (!field.options().HasExtension(dbddl::ch_column_type))
      col.type_clickhouse = "UUID";
    if (!field.options().HasExtension(dbddl::pg_column_type))
      col.type_postgres = "UUID";
  }

  // dbddl.UUID message type — accessor needs .value() to reach the bytes.
  if (col.field_kind == FieldKind::kUUID &&
      field.cpp_type() == google::protobuf::FieldDescriptor::CPPTYPE_MESSAGE) {
    col.uuid_via_message = true;
  }

  col.db_index = field.options().HasExtension(dbddl::db_index) &&
                 field.options().GetExtension(dbddl::db_index);
  if (col.db_index) {
    col.pg_index_using = field.options().HasExtension(dbddl::pg_index_using)
        ? field.options().GetExtension(dbddl::pg_index_using) : "";
    col.ch_skip_index_type = field.options().HasExtension(dbddl::ch_skip_index_type)
        ? field.options().GetExtension(dbddl::ch_skip_index_type) : "";
    col.ch_skip_index_granularity =
        field.options().HasExtension(dbddl::ch_skip_index_granularity)
        ? field.options().GetExtension(dbddl::ch_skip_index_granularity) : 1u;
  }
  col.ch_codec = field.options().HasExtension(dbddl::ch_codec)
      ? field.options().GetExtension(dbddl::ch_codec) : "";
  col.db_default = field.options().HasExtension(dbddl::db_default)
      ? field.options().GetExtension(dbddl::db_default) : "";
  col.db_comment = field.options().HasExtension(dbddl::db_comment)
      ? field.options().GetExtension(dbddl::db_comment) : "";

  return col;
}

void VisitMessage(const google::protobuf::Descriptor& message,
                  TableExtractionResult* result,
                  std::set<std::string>* seen_enums) {
  const auto& opts = message.options();
  const bool has_ch = opts.HasExtension(dbddl::ch_table);
  const bool has_ts = opts.HasExtension(dbddl::ts_table);

  if (has_ch || has_ts) {
    // Collect CREATE TYPE DDL only for enums used in annotated messages.
    for (int i = 0; i < message.field_count(); ++i) {
      const auto* field = message.field(i);
      if (field->cpp_type() != google::protobuf::FieldDescriptor::CPPTYPE_ENUM) continue;
      const auto* ed = field->enum_type();
      if (!seen_enums->insert(ed->full_name()).second) continue;

      std::string pg_type = ed->name();
      std::transform(pg_type.begin(), pg_type.end(), pg_type.begin(), ::tolower);

      std::ostringstream ddl;
      ddl << "CREATE TYPE " << pg_type << " AS ENUM (";
      for (int j = 0; j < ed->value_count(); ++j) {
        if (j > 0) ddl << ", ";
        ddl << "'" << ed->value(j)->name() << "'";
      }
      ddl << ");";
      result->pg_enum_types.push_back(ddl.str());
    }

    TableIR base;
    base.ch_engine = opts.HasExtension(dbddl::ch_engine)
        ? opts.GetExtension(dbddl::ch_engine)
        : "MergeTree()";
    base.ch_partition_by = opts.HasExtension(dbddl::ch_partition_by)
        ? opts.GetExtension(dbddl::ch_partition_by)
        : "";
    base.ch_order_by = opts.HasExtension(dbddl::ch_order_by)
        ? opts.GetExtension(dbddl::ch_order_by)
        : "";
    base.ch_ttl = opts.HasExtension(dbddl::ch_ttl)
        ? opts.GetExtension(dbddl::ch_ttl) : "";
    base.ch_settings = opts.HasExtension(dbddl::ch_settings)
        ? opts.GetExtension(dbddl::ch_settings) : "";
    base.ch_sample_by = opts.HasExtension(dbddl::ch_sample_by)
        ? opts.GetExtension(dbddl::ch_sample_by) : "";
    base.ts_compress_after = opts.HasExtension(dbddl::ts_compress_after)
        ? opts.GetExtension(dbddl::ts_compress_after) : "";
    base.ts_compress_segmentby = opts.HasExtension(dbddl::ts_compress_segmentby)
        ? opts.GetExtension(dbddl::ts_compress_segmentby) : "";
    base.ts_compress_orderby = opts.HasExtension(dbddl::ts_compress_orderby)
        ? opts.GetExtension(dbddl::ts_compress_orderby) : "";
    base.ts_retention = opts.HasExtension(dbddl::ts_retention)
        ? opts.GetExtension(dbddl::ts_retention) : "";
    base.ts_time_column = opts.HasExtension(dbddl::ts_time_column)
        ? opts.GetExtension(dbddl::ts_time_column)
        : "";
    base.ts_chunk_interval = opts.HasExtension(dbddl::ts_chunk_interval)
        ? opts.GetExtension(dbddl::ts_chunk_interval)
        : "";
    base.uuid_pk = opts.HasExtension(dbddl::db_uuid_pk) &&
                   opts.GetExtension(dbddl::db_uuid_pk);
    base.auto_pk_name = (!base.uuid_pk && opts.HasExtension(dbddl::db_auto_pk))
        ? opts.GetExtension(dbddl::db_auto_pk) : "";
    base.proto_cpp_type = ProtoToCpp(message.full_name(), message.file()->package());
    base.proto_include  = ProtoToPbHeader(message.file()->name());

    base.columns.reserve(static_cast<size_t>(message.field_count()));
    for (int i = 0; i < message.field_count(); ++i) {
      auto col = BuildColumn(*message.field(i), &result->errors);
      if (col.has_value()) {
        base.columns.push_back(std::move(*col));
      }
    }

    if (has_ch) {
      TableIR ch = base;
      ch.name = opts.GetExtension(dbddl::ch_table);
      result->clickhouse_tables.push_back(std::move(ch));
    }

    if (has_ts) {
      TableIR ts = base;
      ts.name = opts.GetExtension(dbddl::ts_table);
      result->timescale_tables.push_back(std::move(ts));
    }
  }

  for (int i = 0; i < message.nested_type_count(); ++i) {
    VisitMessage(*message.nested_type(i), result, seen_enums);
  }
}

}  // namespace

TableExtractionResult ExtractTablesFromFile(const google::protobuf::FileDescriptor& file) {
  TableExtractionResult result;
  std::set<std::string> seen_enums;
  for (int i = 0; i < file.message_type_count(); ++i) {
    VisitMessage(*file.message_type(i), &result, &seen_enums);
  }
  return result;
}
