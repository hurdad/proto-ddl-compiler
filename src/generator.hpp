#pragma once

#include <google/protobuf/compiler/code_generator.h>

class DbddlGenerator final : public google::protobuf::compiler::CodeGenerator {
 public:
  bool Generate(const google::protobuf::FileDescriptor* file,
                const std::string& parameter,
                google::protobuf::compiler::GeneratorContext* context,
                std::string* error) const override;
};
