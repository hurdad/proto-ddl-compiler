#pragma once

#include <string>
#include <vector>

#include "ir.hpp"

// Renders C++ insert functions targeting pqxx (libpqxx).
// Returns header (.pg_insert.h) and source (.pg_insert.cc) file contents.
InsertFiles RenderTimescaleInsert(const std::vector<TableIR>& tables,
                                  const std::string& base_name);
