#pragma once

#include <string>
#include <vector>

#include "ir.hpp"

std::string RenderTimescaleDDL(const std::vector<TableIR>& tables,
                               const std::vector<std::string>& pg_enum_types = {});
