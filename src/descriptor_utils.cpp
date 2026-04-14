#include "descriptor_utils.hpp"

#include <google/protobuf/descriptor.h>

#include <string>
#include <vector>

#include "db_options.pb.h"
#include "mapper.hpp"

namespace {

ColumnIR BuildColumn(const google::protobuf::FieldDescriptor& field, std::vector<std::string>* errors) {
  ColumnIR col;
  col.name = field.options().HasExtension(dbddl::db_name) ?
      field.options().GetExtension(dbddl::db_name) : field.name();

  const auto mapped = MapFieldTypes(field);
  if (!mapped.has_value()) {
    errors->push_back(
        "Unsupported field type for " + field.containing_type()->full_name() + "." + field.name());
    return col;
  }

  col.type_clickhouse = field.options().HasExtension(dbddl::ch_column_type)
      ? field.options().GetExtension(dbddl::ch_column_type)
      : mapped->clickhouse;
  col.type_postgres = field.options().HasExtension(dbddl::pg_column_type)
      ? field.options().GetExtension(dbddl::pg_column_type)
      : mapped->postgres;

  col.repeated = field.is_repeated();
  col.nullable = field.options().HasExtension(dbddl::db_nullable)
      ? field.options().GetExtension(dbddl::db_nullable)
      : !field.is_required();

  return col;
}

void VisitMessage(const google::protobuf::Descriptor& message, TableExtractionResult* result) {
  const auto& opts = message.options();
  const bool has_ch = opts.HasExtension(dbddl::ch_table);
  const bool has_ts = opts.HasExtension(dbddl::ts_table);

  if (has_ch || has_ts) {
    TableIR base;
    if (has_ch) {
      base.name = opts.GetExtension(dbddl::ch_table);
    } else {
      base.name = opts.GetExtension(dbddl::ts_table);
    }

    base.ch_engine = opts.HasExtension(dbddl::ch_engine)
        ? opts.GetExtension(dbddl::ch_engine)
        : "MergeTree()";
    base.ch_partition_by = opts.HasExtension(dbddl::ch_partition_by)
        ? opts.GetExtension(dbddl::ch_partition_by)
        : "";
    base.ch_order_by = opts.HasExtension(dbddl::ch_order_by)
        ? opts.GetExtension(dbddl::ch_order_by)
        : "";
    base.ts_time_column = opts.HasExtension(dbddl::ts_time_column)
        ? opts.GetExtension(dbddl::ts_time_column)
        : "";

    base.columns.reserve(static_cast<size_t>(message.field_count()));
    for (int i = 0; i < message.field_count(); ++i) {
      base.columns.push_back(BuildColumn(*message.field(i), &result->errors));
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
    VisitMessage(*message.nested_type(i), result);
  }
}

}  // namespace

TableExtractionResult ExtractTablesFromFile(const google::protobuf::FileDescriptor& file) {
  TableExtractionResult result;
  for (int i = 0; i < file.message_type_count(); ++i) {
    VisitMessage(*file.message_type(i), &result);
  }
  return result;
}
