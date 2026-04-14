#pragma once

#include <string>
#include <vector>

#include "ir.hpp"

std::vector<std::string> ValidateClickHouseTables(const std::vector<TableIR>& tables);
std::vector<std::string> ValidateTimescaleTables(const std::vector<TableIR>& tables);
