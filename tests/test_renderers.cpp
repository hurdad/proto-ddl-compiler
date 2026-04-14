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

TEST(TimescaleRendererTest, IndexDefaultBtree) {
  TableIR t = MakeTradeTable();
  t.columns[1].db_index = true;  // symbol
  auto sql = RenderTimescaleDDL({t});
  EXPECT_NE(sql.find("CREATE INDEX ON trades (symbol)"), std::string::npos);
  EXPECT_EQ(sql.find("USING"), std::string::npos);
}

TEST(TimescaleRendererTest, IndexWithMethod) {
  TableIR t = MakeTradeTable();
  t.columns[1].db_index = true;
  t.columns[1].pg_index_using = "hash";
  auto sql = RenderTimescaleDDL({t});
  EXPECT_NE(sql.find("CREATE INDEX ON trades USING hash (symbol)"), std::string::npos);
}

TEST(TimescaleRendererTest, IndexAfterHypertable) {
  TableIR t = MakeTradeTable();
  t.columns[1].db_index = true;
  auto sql = RenderTimescaleDDL({t});
  EXPECT_LT(sql.find("create_hypertable"), sql.find("CREATE INDEX"));
}

TEST(TimescaleRendererTest, NoIndexWhenNotSet) {
  auto sql = RenderTimescaleDDL({MakeTradeTable()});
  EXPECT_EQ(sql.find("CREATE INDEX"), std::string::npos);
}

// --- ClickHouse skip index tests ---

TEST(ClickHouseRendererTest, SkipIndexDefaultMinmax) {
  TableIR t = MakeTradeTable();
  t.columns[1].db_index = true;  // symbol
  auto sql = RenderClickHouseDDL({t});
  EXPECT_NE(sql.find("INDEX idx_symbol symbol TYPE minmax GRANULARITY 1"), std::string::npos);
}

TEST(ClickHouseRendererTest, SkipIndexCustomType) {
  TableIR t = MakeTradeTable();
  t.columns[1].db_index = true;
  t.columns[1].ch_skip_index_type = "set(100)";
  t.columns[1].ch_skip_index_granularity = 4;
  auto sql = RenderClickHouseDDL({t});
  EXPECT_NE(sql.find("INDEX idx_symbol symbol TYPE set(100) GRANULARITY 4"), std::string::npos);
}

TEST(ClickHouseRendererTest, SkipIndexInsideTableDefinition) {
  TableIR t = MakeTradeTable();
  t.columns[1].db_index = true;
  auto sql = RenderClickHouseDDL({t});
  EXPECT_LT(sql.find("INDEX idx_symbol"), sql.find("ENGINE ="));
}

TEST(ClickHouseRendererTest, LastColumnCommaWhenIndexPresent) {
  TableIR t = MakeTradeTable();
  t.columns[1].db_index = true;
  auto sql = RenderClickHouseDDL({t});
  // The last column (price) must have a trailing comma before the INDEX line.
  const size_t price_pos = sql.find("  price ");
  const size_t newline = sql.find('\n', price_pos);
  const std::string price_line = sql.substr(price_pos, newline - price_pos);
  EXPECT_NE(price_line.find(','), std::string::npos);
}

TEST(ClickHouseRendererTest, NoSkipIndexWhenNotSet) {
  auto sql = RenderClickHouseDDL({MakeTradeTable()});
  EXPECT_EQ(sql.find("INDEX"), std::string::npos);
}

// --- Auto PK tests (ClickHouse) ---

TEST(ClickHouseRendererTest, AutoPkEmitsUInt64First) {
  TableIR t = MakeTradeTable();
  t.auto_pk_name = "id";
  auto sql = RenderClickHouseDDL({t});
  EXPECT_NE(sql.find("  id UInt64,"), std::string::npos);
  EXPECT_LT(sql.find("id UInt64"), sql.find("ts DateTime64"));
}

TEST(ClickHouseRendererTest, UuidPkEmitsUUIDWithDefault) {
  TableIR t = MakeTradeTable();
  t.uuid_pk = true;
  auto sql = RenderClickHouseDDL({t});
  EXPECT_NE(sql.find("  id UUID DEFAULT generateUUIDv4(),"), std::string::npos);
  EXPECT_LT(sql.find("id UUID"), sql.find("ts DateTime64"));
}

TEST(ClickHouseRendererTest, UuidPkOverridesAutoPk) {
  TableIR t = MakeTradeTable();
  t.auto_pk_name = "id";
  t.uuid_pk = true;
  auto sql = RenderClickHouseDDL({t});
  EXPECT_NE(sql.find("generateUUIDv4()"), std::string::npos);
  EXPECT_EQ(sql.find("UInt64"), std::string::npos);
}

TEST(ClickHouseRendererTest, TtlEmittedAfterOrderBy) {
  TableIR t = MakeTradeTable();
  t.ch_ttl = "ts + INTERVAL 3 MONTH";
  auto sql = RenderClickHouseDDL({t});
  EXPECT_NE(sql.find("TTL ts + INTERVAL 3 MONTH"), std::string::npos);
  EXPECT_LT(sql.find("ORDER BY"), sql.find("TTL"));
}

TEST(ClickHouseRendererTest, NoTtlWhenNotSet) {
  auto sql = RenderClickHouseDDL({MakeTradeTable()});
  EXPECT_EQ(sql.find("TTL"), std::string::npos);
}

TEST(ClickHouseRendererTest, NoPkWhenNotSet) {
  auto sql = RenderClickHouseDDL({MakeTradeTable()});
  EXPECT_EQ(sql.find("UInt64"), std::string::npos);
  EXPECT_EQ(sql.find("generateUUIDv4"), std::string::npos);
}

// --- Auto PK tests (TimescaleDB) ---

TEST(TimescaleRendererTest, AutoPkEmitsBigintIdentityFirst) {
  TableIR t = MakeTradeTable();
  t.auto_pk_name = "id";
  auto sql = RenderTimescaleDDL({t});
  EXPECT_NE(sql.find("  id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,"), std::string::npos);
  EXPECT_LT(sql.find("id BIGINT"), sql.find("ts TIMESTAMPTZ"));
}

TEST(TimescaleRendererTest, UuidPkEmitsUUIDWithDefault) {
  TableIR t = MakeTradeTable();
  t.uuid_pk = true;
  auto sql = RenderTimescaleDDL({t});
  EXPECT_NE(sql.find("  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,"), std::string::npos);
  EXPECT_LT(sql.find("id UUID"), sql.find("ts TIMESTAMPTZ"));
}

TEST(TimescaleRendererTest, NoPkWhenNotSet) {
  auto sql = RenderTimescaleDDL({MakeTradeTable()});
  EXPECT_EQ(sql.find("PRIMARY KEY"), std::string::npos);
  EXPECT_EQ(sql.find("gen_random_uuid"), std::string::npos);
}

// --- Column codec (ClickHouse) ---

TEST(ClickHouseRendererTest, ColumnCodecEmitted) {
  TableIR t = MakeTradeTable();
  t.columns[2].ch_codec = "Delta, LZ4";  // price
  auto sql = RenderClickHouseDDL({t});
  EXPECT_NE(sql.find("CODEC(Delta, LZ4)"), std::string::npos);
}

TEST(ClickHouseRendererTest, NoCodecWhenNotSet) {
  auto sql = RenderClickHouseDDL({MakeTradeTable()});
  EXPECT_EQ(sql.find("CODEC"), std::string::npos);
}

// --- Column default ---

TEST(ClickHouseRendererTest, ColumnDefaultEmitted) {
  TableIR t = MakeTradeTable();
  t.columns[1].db_default = "'UNKNOWN'";  // symbol
  auto sql = RenderClickHouseDDL({t});
  EXPECT_NE(sql.find("DEFAULT 'UNKNOWN'"), std::string::npos);
}

TEST(TimescaleRendererTest, ColumnDefaultEmitted) {
  TableIR t = MakeTradeTable();
  t.columns[1].db_default = "'UNKNOWN'";
  auto sql = RenderTimescaleDDL({t});
  EXPECT_NE(sql.find("DEFAULT 'UNKNOWN'"), std::string::npos);
}

TEST(TimescaleRendererTest, ColumnDefaultBeforeNotNull) {
  TableIR t = MakeTradeTable();
  t.columns[0].db_default = "now()";  // ts, NOT NULL
  auto sql = RenderTimescaleDDL({t});
  const size_t def_pos = sql.find("DEFAULT now()");
  const size_t nn_pos  = sql.find("NOT NULL");
  EXPECT_LT(def_pos, nn_pos);
}

// --- Column comment ---

TEST(ClickHouseRendererTest, ColumnCommentEmitted) {
  TableIR t = MakeTradeTable();
  t.columns[1].db_comment = "Trading symbol";
  auto sql = RenderClickHouseDDL({t});
  EXPECT_NE(sql.find("COMMENT 'Trading symbol'"), std::string::npos);
}

TEST(ClickHouseRendererTest, ColumnCommentSingleQuoteEscaped) {
  TableIR t = MakeTradeTable();
  t.columns[1].db_comment = "It's a symbol";
  auto sql = RenderClickHouseDDL({t});
  EXPECT_NE(sql.find("COMMENT 'It''s a symbol'"), std::string::npos);
}

TEST(TimescaleRendererTest, ColumnCommentEmitsCommentOn) {
  TableIR t = MakeTradeTable();
  t.columns[1].db_comment = "Trading symbol";
  auto sql = RenderTimescaleDDL({t});
  EXPECT_NE(sql.find("COMMENT ON COLUMN trades.symbol IS 'Trading symbol'"), std::string::npos);
}

TEST(TimescaleRendererTest, ColumnCommentSingleQuoteEscaped) {
  TableIR t = MakeTradeTable();
  t.columns[1].db_comment = "It's a symbol";
  auto sql = RenderTimescaleDDL({t});
  EXPECT_NE(sql.find("IS 'It''s a symbol'"), std::string::npos);
}

TEST(TimescaleRendererTest, CompressSegmentbySingleQuoteEscaped) {
  TableIR t = MakeTradeTable();
  t.ts_compress_segmentby = "it's_col";
  auto sql = RenderTimescaleDDL({t});
  EXPECT_NE(sql.find("compress_segmentby = 'it''s_col'"), std::string::npos);
}

TEST(TimescaleRendererTest, CompressOrderbySingleQuoteEscaped) {
  TableIR t = MakeTradeTable();
  t.ts_compress_orderby = "ts DESC -- it's sorted";
  auto sql = RenderTimescaleDDL({t});
  EXPECT_NE(sql.find("compress_orderby = 'ts DESC -- it''s sorted'"), std::string::npos);
}

TEST(TimescaleRendererTest, ColumnCommentBeforeHypertable) {
  TableIR t = MakeTradeTable();
  t.columns[1].db_comment = "Trading symbol";
  auto sql = RenderTimescaleDDL({t});
  EXPECT_LT(sql.find("COMMENT ON COLUMN"), sql.find("create_hypertable"));
}

// --- SAMPLE BY / SETTINGS (ClickHouse) ---

TEST(ClickHouseRendererTest, SampleByEmittedAfterOrderBy) {
  TableIR t = MakeTradeTable();
  t.ch_sample_by = "intHash32(symbol)";
  auto sql = RenderClickHouseDDL({t});
  EXPECT_NE(sql.find("SAMPLE BY intHash32(symbol)"), std::string::npos);
  EXPECT_LT(sql.find("ORDER BY"), sql.find("SAMPLE BY"));
}

TEST(ClickHouseRendererTest, SampleByBeforeTtl) {
  TableIR t = MakeTradeTable();
  t.ch_sample_by = "intHash32(symbol)";
  t.ch_ttl = "ts + INTERVAL 1 MONTH";
  auto sql = RenderClickHouseDDL({t});
  EXPECT_LT(sql.find("SAMPLE BY"), sql.find("TTL"));
}

TEST(ClickHouseRendererTest, SettingsEmittedLast) {
  TableIR t = MakeTradeTable();
  t.ch_settings = "index_granularity = 8192";
  t.ch_ttl = "ts + INTERVAL 1 MONTH";
  auto sql = RenderClickHouseDDL({t});
  EXPECT_NE(sql.find("SETTINGS index_granularity = 8192"), std::string::npos);
  EXPECT_LT(sql.find("TTL"), sql.find("SETTINGS"));
}

TEST(ClickHouseRendererTest, NoSettingsWhenNotSet) {
  auto sql = RenderClickHouseDDL({MakeTradeTable()});
  EXPECT_EQ(sql.find("SETTINGS"), std::string::npos);
}

// --- TimescaleDB compression ---

TEST(TimescaleRendererTest, CompressionAlterTableEmitted) {
  TableIR t = MakeTradeTable();
  t.ts_compress_segmentby = "symbol";
  t.ts_compress_orderby = "ts DESC";
  auto sql = RenderTimescaleDDL({t});
  EXPECT_NE(sql.find("ALTER TABLE trades SET ("), std::string::npos);
  EXPECT_NE(sql.find("timescaledb.compress"), std::string::npos);
  EXPECT_NE(sql.find("timescaledb.compress_segmentby = 'symbol'"), std::string::npos);
  EXPECT_NE(sql.find("timescaledb.compress_orderby = 'ts DESC'"), std::string::npos);
}

TEST(TimescaleRendererTest, CompressionPolicyEmittedWhenAfterSet) {
  TableIR t = MakeTradeTable();
  t.ts_compress_after = "7 days";
  auto sql = RenderTimescaleDDL({t});
  EXPECT_NE(sql.find("add_compression_policy('trades', INTERVAL '7 days')"), std::string::npos);
}

TEST(TimescaleRendererTest, CompressionPolicyNotEmittedWithoutAfter) {
  TableIR t = MakeTradeTable();
  t.ts_compress_segmentby = "symbol";
  auto sql = RenderTimescaleDDL({t});
  EXPECT_EQ(sql.find("add_compression_policy"), std::string::npos);
}

TEST(TimescaleRendererTest, NoCompressionWhenNotSet) {
  auto sql = RenderTimescaleDDL({MakeTradeTable()});
  EXPECT_EQ(sql.find("timescaledb.compress"), std::string::npos);
  EXPECT_EQ(sql.find("ALTER TABLE"), std::string::npos);
}

// --- TimescaleDB retention ---

TEST(TimescaleRendererTest, RetentionPolicyEmitted) {
  TableIR t = MakeTradeTable();
  t.ts_retention = "1 year";
  auto sql = RenderTimescaleDDL({t});
  EXPECT_NE(sql.find("add_retention_policy('trades', INTERVAL '1 year')"), std::string::npos);
}

TEST(TimescaleRendererTest, NoRetentionWhenNotSet) {
  auto sql = RenderTimescaleDDL({MakeTradeTable()});
  EXPECT_EQ(sql.find("add_retention_policy"), std::string::npos);
}
