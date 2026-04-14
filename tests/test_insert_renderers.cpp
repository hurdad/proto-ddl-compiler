#include <gtest/gtest.h>

#include "backends/clickhouse_insert_renderer.hpp"
#include "backends/timescale_insert_renderer.hpp"
#include "ir.hpp"

namespace {

ColumnIR MakeCol(const std::string& name,
                 const std::string& proto_field_name,
                 FieldKind kind,
                 const std::string& ch_type,
                 const std::string& pg_type,
                 bool nullable = false,
                 bool repeated = false,
                 const std::string& enum_cpp_type = "") {
  ColumnIR c;
  c.name = name;
  c.proto_field_name = proto_field_name;
  c.field_kind = kind;
  c.type_clickhouse = ch_type;
  c.type_postgres = pg_type;
  c.nullable = nullable;
  c.repeated = repeated;
  c.enum_cpp_type = enum_cpp_type;
  return c;
}

TableIR MakeTradeTable() {
  TableIR t;
  t.name = "trades";
  t.proto_cpp_type = "example::Trade";
  t.proto_include = "example_trade.pb.h";
  t.ch_order_by = "timestamp";
  t.ts_time_column = "timestamp";
  t.columns.push_back(MakeCol("timestamp", "timestamp", FieldKind::kTimestamp,
                               "DateTime64(3)", "TIMESTAMPTZ"));
  t.columns.push_back(MakeCol("symbol", "symbol", FieldKind::kString, "String", "TEXT"));
  t.columns.push_back(MakeCol("price",  "price",  FieldKind::kDouble, "Float64",
                               "DOUBLE PRECISION", /*nullable=*/true));
  t.columns.push_back(MakeCol("size",   "size",   FieldKind::kInt32,  "Int32", "INTEGER"));
  t.columns.push_back(MakeCol("side", "side", FieldKind::kEnum,
                               "LowCardinality(String)", "side",
                               /*nullable=*/false, /*repeated=*/false, "example::Side"));
  return t;
}

}  // namespace

// ============================================================
// ClickHouse insert renderer
// ============================================================

TEST(CHInsertTest, EmptyTablesReturnsEmpty) {
  auto f = RenderClickHouseInsert({}, "base");
  EXPECT_TRUE(f.header.empty());
  EXPECT_TRUE(f.source.empty());
}

TEST(CHInsertTest, HeaderIncludesProtoAndClickHouse) {
  auto f = RenderClickHouseInsert({MakeTradeTable()}, "example_trade");
  EXPECT_NE(f.header.find("#include <clickhouse/client.h>"), std::string::npos);
  EXPECT_NE(f.header.find("#include \"example_trade.pb.h\""), std::string::npos);
}

TEST(CHInsertTest, HeaderDeclaresFunctionSignature) {
  auto f = RenderClickHouseInsert({MakeTradeTable()}, "example_trade");
  EXPECT_NE(f.header.find("void InsertTrades("), std::string::npos);
  EXPECT_NE(f.header.find("clickhouse::Client& client"), std::string::npos);
  EXPECT_NE(f.header.find("std::vector<example::Trade>"), std::string::npos);
}

TEST(CHInsertTest, SourceIncludesHeader) {
  auto f = RenderClickHouseInsert({MakeTradeTable()}, "example_trade");
  EXPECT_NE(f.source.find("#include \"example_trade.ch_insert.h\""), std::string::npos);
}

TEST(CHInsertTest, SourceHasEarlyReturnOnEmpty) {
  auto f = RenderClickHouseInsert({MakeTradeTable()}, "example_trade");
  EXPECT_NE(f.source.find("if (rows.empty()) return;"), std::string::npos);
}

TEST(CHInsertTest, TimestampColumnUsesDateTime64WithPrecision) {
  auto f = RenderClickHouseInsert({MakeTradeTable()}, "example_trade");
  EXPECT_NE(f.source.find("ColumnDateTime64>(3)"), std::string::npos);
}

TEST(CHInsertTest, TimestampAppendConvertsSecondsAndNanos) {
  auto f = RenderClickHouseInsert({MakeTradeTable()}, "example_trade");
  EXPECT_NE(f.source.find("seconds() * 1000LL"), std::string::npos);
  EXPECT_NE(f.source.find("nanos() / 1000000LL"), std::string::npos);
}

TEST(CHInsertTest, EnumColumnUsesLowCardinality) {
  auto f = RenderClickHouseInsert({MakeTradeTable()}, "example_trade");
  EXPECT_NE(f.source.find("ColumnLowCardinalityT<clickhouse::ColumnString>"), std::string::npos);
}

TEST(CHInsertTest, EnumAppendCallsNameFunction) {
  auto f = RenderClickHouseInsert({MakeTradeTable()}, "example_trade");
  EXPECT_NE(f.source.find("example::Side_Name(row.side())"), std::string::npos);
}

TEST(CHInsertTest, NullableColumnUsesNullableT) {
  auto f = RenderClickHouseInsert({MakeTradeTable()}, "example_trade");
  EXPECT_NE(f.source.find("ColumnNullableT<clickhouse::ColumnFloat64>"), std::string::npos);
}

TEST(CHInsertTest, NullableAppendChecksHasField) {
  auto f = RenderClickHouseInsert({MakeTradeTable()}, "example_trade");
  EXPECT_NE(f.source.find("row.has_price()"), std::string::npos);
  EXPECT_NE(f.source.find("std::nullopt"), std::string::npos);
}

TEST(CHInsertTest, BlockAppendColumnForEachColumn) {
  auto f = RenderClickHouseInsert({MakeTradeTable()}, "example_trade");
  EXPECT_NE(f.source.find("block.AppendColumn(\"timestamp\""), std::string::npos);
  EXPECT_NE(f.source.find("block.AppendColumn(\"symbol\""),    std::string::npos);
  EXPECT_NE(f.source.find("block.AppendColumn(\"side\""),      std::string::npos);
}

TEST(CHInsertTest, ClientInsertCallWithTableName) {
  auto f = RenderClickHouseInsert({MakeTradeTable()}, "example_trade");
  EXPECT_NE(f.source.find("client.Insert(\"trades\""), std::string::npos);
}

TEST(CHInsertTest, UuidColumnUsesColumnUUID) {
  TableIR t = MakeTradeTable();
  t.columns.push_back(MakeCol("trade_id", "trade_id", FieldKind::kUUID, "UUID", "UUID"));
  auto f = RenderClickHouseInsert({t}, "example_trade");
  EXPECT_NE(f.source.find("ColumnUUID"), std::string::npos);
  EXPECT_NE(f.source.find("#include <clickhouse/columns/uuid.h>"), std::string::npos);
}

TEST(CHInsertTest, UuidAppendUsesMempyAndUuidStruct) {
  TableIR t = MakeTradeTable();
  t.columns.push_back(MakeCol("trade_id", "trade_id", FieldKind::kUUID, "UUID", "UUID"));
  auto f = RenderClickHouseInsert({t}, "example_trade");
  EXPECT_NE(f.source.find("memcpy("), std::string::npos);
  EXPECT_NE(f.source.find("clickhouse::UUID"), std::string::npos);
  EXPECT_NE(f.source.find("row.trade_id()"), std::string::npos);
}

TEST(CHInsertTest, UuidViaMessageAccessesValueField) {
  TableIR t = MakeTradeTable();
  ColumnIR col = MakeCol("trade_id", "trade_id", FieldKind::kUUID, "UUID", "UUID");
  col.uuid_via_message = true;
  t.columns.push_back(col);
  auto f = RenderClickHouseInsert({t}, "example_trade");
  EXPECT_NE(f.source.find("row.trade_id().value()"), std::string::npos);
}

TEST(CHInsertTest, RepeatedColumnUsesArrayT) {
  TableIR t = MakeTradeTable();
  t.columns.push_back(MakeCol("tags", "tags", FieldKind::kString,
                               "Array(String)", "TEXT[]",
                               /*nullable=*/false, /*repeated=*/true));
  auto f = RenderClickHouseInsert({t}, "example_trade");
  EXPECT_NE(f.source.find("ColumnArrayT<clickhouse::ColumnString>"), std::string::npos);
  EXPECT_NE(f.source.find("AppendAsColumn"), std::string::npos);
  EXPECT_NE(f.source.find("tags_size()"), std::string::npos);
}

// ============================================================
// TimescaleDB insert renderer
// ============================================================

TEST(PGInsertTest, EmptyTablesReturnsEmpty) {
  auto f = RenderTimescaleInsert({}, "base");
  EXPECT_TRUE(f.header.empty());
  EXPECT_TRUE(f.source.empty());
}

TEST(PGInsertTest, HeaderIncludesProtoAndPqxx) {
  auto f = RenderTimescaleInsert({MakeTradeTable()}, "example_trade");
  EXPECT_NE(f.header.find("#include <pqxx/pqxx>"), std::string::npos);
  EXPECT_NE(f.header.find("#include \"example_trade.pb.h\""), std::string::npos);
}

TEST(PGInsertTest, HeaderDeclaresFunctionSignature) {
  auto f = RenderTimescaleInsert({MakeTradeTable()}, "example_trade");
  EXPECT_NE(f.header.find("void InsertTrades("), std::string::npos);
  EXPECT_NE(f.header.find("pqxx::connection& conn"), std::string::npos);
  EXPECT_NE(f.header.find("std::vector<example::Trade>"), std::string::npos);
}

TEST(PGInsertTest, SourceIncludesHeader) {
  auto f = RenderTimescaleInsert({MakeTradeTable()}, "example_trade");
  EXPECT_NE(f.source.find("#include \"example_trade.pg_insert.h\""), std::string::npos);
}

TEST(PGInsertTest, SourceHasStreamToTable) {
  auto f = RenderTimescaleInsert({MakeTradeTable()}, "example_trade");
  EXPECT_NE(f.source.find("pqxx::stream_to::table"), std::string::npos);
  EXPECT_NE(f.source.find("\"trades\""), std::string::npos);
}

TEST(PGInsertTest, SourceListsAllColumns) {
  auto f = RenderTimescaleInsert({MakeTradeTable()}, "example_trade");
  EXPECT_NE(f.source.find("\"timestamp\""), std::string::npos);
  EXPECT_NE(f.source.find("\"symbol\""),    std::string::npos);
  EXPECT_NE(f.source.find("\"side\""),      std::string::npos);
}

TEST(PGInsertTest, TimestampFormatsAsString) {
  auto f = RenderTimescaleInsert({MakeTradeTable()}, "example_trade");
  EXPECT_NE(f.source.find("gmtime_r"), std::string::npos);
  EXPECT_NE(f.source.find("strftime"), std::string::npos);
  EXPECT_NE(f.source.find(".seconds()"), std::string::npos);
  EXPECT_NE(f.source.find(".nanos()"), std::string::npos);
}

TEST(PGInsertTest, EnumCallsNameFunction) {
  auto f = RenderTimescaleInsert({MakeTradeTable()}, "example_trade");
  EXPECT_NE(f.source.find("example::Side_Name(row.side())"), std::string::npos);
}

TEST(PGInsertTest, NullableUsesOptional) {
  auto f = RenderTimescaleInsert({MakeTradeTable()}, "example_trade");
  EXPECT_NE(f.source.find("std::optional<double>"), std::string::npos);
  EXPECT_NE(f.source.find("row.has_price()"), std::string::npos);
}

TEST(PGInsertTest, StreamCommitCalled) {
  auto f = RenderTimescaleInsert({MakeTradeTable()}, "example_trade");
  EXPECT_NE(f.source.find("stream.complete()"), std::string::npos);
  EXPECT_NE(f.source.find("txn.commit()"), std::string::npos);
}

TEST(PGInsertTest, UuidColumnBuildsHexString) {
  TableIR t = MakeTradeTable();
  t.columns.push_back(MakeCol("trade_id", "trade_id", FieldKind::kUUID, "UUID", "UUID"));
  auto f = RenderTimescaleInsert({t}, "example_trade");
  EXPECT_NE(f.source.find("snprintf"), std::string::npos);
  EXPECT_NE(f.source.find("%02x%02x%02x%02x-%02x%02x"), std::string::npos);
  EXPECT_NE(f.source.find("row.trade_id()"), std::string::npos);
}

TEST(PGInsertTest, UuidViaMessageAccessesValueField) {
  TableIR t = MakeTradeTable();
  ColumnIR col = MakeCol("trade_id", "trade_id", FieldKind::kUUID, "UUID", "UUID");
  col.uuid_via_message = true;
  t.columns.push_back(col);
  auto f = RenderTimescaleInsert({t}, "example_trade");
  EXPECT_NE(f.source.find("row.trade_id().value()"), std::string::npos);
}

TEST(PGInsertTest, RepeatedColumnBuildsArrayLiteral) {
  TableIR t = MakeTradeTable();
  t.columns.push_back(MakeCol("tags", "tags", FieldKind::kString,
                               "Array(String)", "TEXT[]",
                               /*nullable=*/false, /*repeated=*/true));
  auto f = RenderTimescaleInsert({t}, "example_trade");
  EXPECT_NE(f.source.find("tags_size()"), std::string::npos);
  EXPECT_NE(f.source.find("\"{\""), std::string::npos);
}
