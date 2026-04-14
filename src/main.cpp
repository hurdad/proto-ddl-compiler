#include <google/protobuf/compiler/plugin.h>

#include "generator.hpp"

int main(int argc, char* argv[]) {
  DbddlGenerator generator;
  return google::protobuf::compiler::PluginMain(argc, argv, &generator);
}
