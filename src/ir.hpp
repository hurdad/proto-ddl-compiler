#pragma once

#include <string>
#include <vector>

struct ColumnIR {
  std::string name;
  std::string type_clickhouse;
  std::string type_postgres;
  bool nullable = true;
  bool repeated = false;
};

struct TableIR {
  std::string name;
  std::vector<ColumnIR> columns;

  std::string ch_engine;
  std::string ch_partition_by;
  std::string ch_order_by;

  std::string ts_time_column;
};
