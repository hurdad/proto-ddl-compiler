#pragma once

#include <optional>
#include <string>

namespace google::protobuf {
class FieldDescriptor;
}

struct MappedTypes {
  std::string clickhouse;
  std::string postgres;
};

std::optional<MappedTypes> MapFieldTypes(const google::protobuf::FieldDescriptor& field);
