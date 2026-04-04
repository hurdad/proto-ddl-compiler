#include "mapper.hpp"

#include <google/protobuf/descriptor.h>

#include <string>

namespace {

bool IsTimestamp(const google::protobuf::FieldDescriptor& field) {
  return field.cpp_type() == google::protobuf::FieldDescriptor::CPPTYPE_MESSAGE &&
         field.message_type() != nullptr &&
         field.message_type()->full_name() == "google.protobuf.Timestamp";
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
    case FD::CPPTYPE_ENUM:
      mapped = {"String", "TEXT"};
      break;
    case FD::CPPTYPE_MESSAGE:
      if (IsTimestamp(field)) {
        mapped = {"DateTime64(3)", "TIMESTAMPTZ"};
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
