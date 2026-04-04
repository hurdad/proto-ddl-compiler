#include <gtest/gtest.h>

#include "backends/clickhouse_renderer.hpp"
#include "backends/timescale_renderer.hpp"
#include "ir.hpp"

namespace {

ColumnIR MakeCol(const std::string& name,
                 const std::string& ch_type,
                 const std::string& pg_type,
                 bool nullable = false,
                 bool repeated = false) {
  ColumnIR c;
  c.name = name;
  c.type_clickhouse = ch_type;
  c.type_postgres = pg_type;
  c.nullable = nullable;
  c.repeated = repeated;
  return c;
}

TableIR MakeTradeTable() {
  TableIR t;
  t.name = "trades";
  t.ch_engine = "MergeTree()";
  t.ch_partition_by = "toYYYYMM(ts)";
  t.ch_order_by = "ts";
  t.ts_time_column = "ts";
  t.columns.push_back(MakeCol("ts", "DateTime64(3)", "TIMESTAMPTZ", false));
  t.columns.push_back(MakeCol("symbol", "String", "TEXT", false));
  t.columns.push_back(MakeCol("price", "Float64", "DOUBLE PRECISION", true));
  return t;
}

}  // namespace

// --- ClickHouse renderer ---

TEST(ClickHouseRendererTest, EmptyInput) {
  EXPECT_EQ(RenderClickHouseDDL({}), "");
}

TEST(ClickHouseRendererTest, ContainsTableName) {
  auto sql = RenderClickHouseDDL({MakeTradeTable()});
  EXPECT_NE(sql.find("CREATE TABLE trades"), std::string::npos);
}

TEST(ClickHouseRendererTest, ContainsEngine) {
  auto sql = RenderClickHouseDDL({MakeTradeTable()});
  EXPECT_NE(sql.find("ENGINE = MergeTree()"), std::string::npos);
}

TEST(ClickHouseRendererTest, ContainsPartitionBy) {
  auto sql = RenderClickHouseDDL({MakeTradeTable()});
  EXPECT_NE(sql.find("PARTITION BY toYYYYMM(ts)"), std::string::npos);
}

TEST(ClickHouseRendererTest, ContainsOrderBy) {
  auto sql = RenderClickHouseDDL({MakeTradeTable()});
  EXPECT_NE(sql.find("ORDER BY (ts)"), std::string::npos);
}

TEST(ClickHouseRendererTest, NullableColumnWrapped) {
  auto sql = RenderClickHouseDDL({MakeTradeTable()});
  // price is nullable
  EXPECT_NE(sql.find("Nullable(Float64)"), std::string::npos);
}

TEST(ClickHouseRendererTest, NonNullableColumnNotWrapped) {
  auto sql = RenderClickHouseDDL({MakeTradeTable()});
  // ts and symbol are not nullable — should appear without Nullable(...)
  EXPECT_NE(sql.find("ts DateTime64(3)"), std::string::npos);
}

TEST(ClickHouseRendererTest, NoPartitionBy) {
  TableIR t = MakeTradeTable();
  t.ch_partition_by = "";
  auto sql = RenderClickHouseDDL({t});
  EXPECT_EQ(sql.find("PARTITION BY"), std::string::npos);
}

TEST(ClickHouseRendererTest, MultipleTables) {
  TableIR t2 = MakeTradeTable();
  t2.name = "orders";
  auto sql = RenderClickHouseDDL({MakeTradeTable(), t2});
  EXPECT_NE(sql.find("CREATE TABLE trades"), std::string::npos);
  EXPECT_NE(sql.find("CREATE TABLE orders"), std::string::npos);
}

// --- Timescale renderer ---

// helpers for enum type tests
const std::vector<std::string> kEnumTypes = {
    "CREATE TYPE side AS ENUM ('SIDE_UNKNOWN', 'SIDE_BUY', 'SIDE_SELL');",
};


TEST(TimescaleRendererTest, EmptyInput) {
  EXPECT_EQ(RenderTimescaleDDL({}), "");
}

TEST(TimescaleRendererTest, ContainsTableName) {
  auto sql = RenderTimescaleDDL({MakeTradeTable()});
  EXPECT_NE(sql.find("CREATE TABLE trades"), std::string::npos);
}

TEST(TimescaleRendererTest, ContainsHypertableCall) {
  auto sql = RenderTimescaleDDL({MakeTradeTable()});
  EXPECT_NE(sql.find("create_hypertable('trades'"), std::string::npos);
  EXPECT_NE(sql.find("by_range('ts')"), std::string::npos);
}

TEST(TimescaleRendererTest, NullableColumnNoNotNull) {
  auto sql = RenderTimescaleDDL({MakeTradeTable()});
  // price is nullable — should NOT have NOT NULL
  const size_t price_pos = sql.find("price");
  ASSERT_NE(price_pos, std::string::npos);
  const size_t next_comma = sql.find('\n', price_pos);
  const std::string price_line = sql.substr(price_pos, next_comma - price_pos);
  EXPECT_EQ(price_line.find("NOT NULL"), std::string::npos);
}

TEST(TimescaleRendererTest, NonNullableColumnHasNotNull) {
  auto sql = RenderTimescaleDDL({MakeTradeTable()});
  // ts and symbol are not nullable
  EXPECT_NE(sql.find("ts TIMESTAMPTZ NOT NULL"), std::string::npos);
}

TEST(TimescaleRendererTest, EnumCreateTypeEmitted) {
  auto sql = RenderTimescaleDDL({MakeTradeTable()}, kEnumTypes);
  EXPECT_NE(sql.find("CREATE TYPE side AS ENUM"), std::string::npos);
  EXPECT_NE(sql.find("'SIDE_BUY'"), std::string::npos);
}

TEST(TimescaleRendererTest, EnumCreateTypeBeforeTable) {
  auto sql = RenderTimescaleDDL({MakeTradeTable()}, kEnumTypes);
  EXPECT_LT(sql.find("CREATE TYPE"), sql.find("CREATE TABLE"));
}

TEST(TimescaleRendererTest, NoEnumTypesWhenEmpty) {
  auto sql = RenderTimescaleDDL({MakeTradeTable()});
  EXPECT_EQ(sql.find("CREATE TYPE"), std::string::npos);
}

TEST(TimescaleRendererTest, ChunkInterval) {
  TableIR t = MakeTradeTable();
  t.ts_chunk_interval = "1 day";
  auto sql = RenderTimescaleDDL({t});
  EXPECT_NE(sql.find("INTERVAL '1 day'"), std::string::npos);
}

TEST(TimescaleRendererTest, NoChunkInterval) {
  auto sql = RenderTimescaleDDL({MakeTradeTable()});
  EXPECT_EQ(sql.find("INTERVAL"), std::string::npos);
}

TEST(TimescaleRendererTest, MultipleTables) {
  TableIR t2 = MakeTradeTable();
  t2.name = "orders";
  auto sql = RenderTimescaleDDL({MakeTradeTable(), t2});
  EXPECT_NE(sql.find("CREATE TABLE trades"), std::string::npos);
  EXPECT_NE(sql.find("CREATE TABLE orders"), std::string::npos);
}
