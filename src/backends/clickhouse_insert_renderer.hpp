#pragma once

#include <string>
#include <vector>

#include "ir.hpp"

// Renders C++ insert functions targeting clickhouse-cpp.
// Returns header (.ch_insert.h) and source (.ch_insert.cc) file contents.
InsertFiles RenderClickHouseInsert(const std::vector<TableIR>& tables,
                                   const std::string& base_name);
