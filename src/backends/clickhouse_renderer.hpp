#pragma once

#include <string>
#include <vector>

#include "ir.hpp"

std::string RenderClickHouseDDL(const std::vector<TableIR>& tables);
