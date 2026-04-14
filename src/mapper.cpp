#include "mapper.hpp"

#include <google/protobuf/descriptor.h>

#include <algorithm>
#include <cctype>
#include <string>

namespace {

bool IsTimestamp(const google::protobuf::FieldDescriptor& field) {
  return field.cpp_type() == google::protobuf::FieldDescriptor::CPPTYPE_MESSAGE &&
         field.message_type() != nullptr &&
         field.message_type()->full_name() == "google.protobuf.Timestamp";
}

bool IsUUID(const google::protobuf::FieldDescriptor& field) {
  return field.cpp_type() == google::protobuf::FieldDescriptor::CPPTYPE_MESSAGE &&
         field.message_type() != nullptr &&
         field.message_type()->full_name() == "dbddl.UUID";
}

}  // namespace

std::optional<MappedTypes> MapFieldTypes(const google::protobuf::FieldDescriptor& field) {
  using FD = google::protobuf::FieldDescriptor;

  MappedTypes mapped;
  switch (field.cpp_type()) {
    case FD::CPPTYPE_INT32:
      mapped = {"Int32", "INTEGER"};
      break;
    case FD::CPPTYPE_INT64:
      mapped = {"Int64", "BIGINT"};
      break;
    case FD::CPPTYPE_DOUBLE:
      mapped = {"Float64", "DOUBLE PRECISION"};
      break;
    case FD::CPPTYPE_STRING:
      mapped = {"String", field.type() == FD::TYPE_BYTES ? "BYTEA" : "TEXT"};
      break;
    case FD::CPPTYPE_UINT32:
      mapped = {"UInt32", "INTEGER"};
      break;
    case FD::CPPTYPE_UINT64:
      mapped = {"UInt64", "BIGINT"};
      break;
    case FD::CPPTYPE_FLOAT:
      mapped = {"Float32", "REAL"};
      break;
    case FD::CPPTYPE_BOOL:
      mapped = {"Bool", "BOOLEAN"};
      break;
    case FD::CPPTYPE_ENUM: {
      std::string pg_type = field.enum_type()->name();
      std::transform(pg_type.begin(), pg_type.end(), pg_type.begin(), ::tolower);
      mapped = {"LowCardinality(String)", std::move(pg_type)};
      break;
    }
    case FD::CPPTYPE_MESSAGE:
      if (IsTimestamp(field)) {
        mapped = {"DateTime64(3)", "TIMESTAMPTZ"};
        break;
      }
      if (IsUUID(field)) {
        mapped = {"UUID", "UUID"};
        break;
      }
      return std::nullopt;
    default:
      return std::nullopt;
  }

  if (field.is_repeated()) {
    mapped.clickhouse = "Array(" + mapped.clickhouse + ")";
    mapped.postgres += "[]";
  }

  return mapped;
}
