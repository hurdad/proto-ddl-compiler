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
  c.has_proto_presence = nullable;  // nullable test columns have proto presence (the common case)
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

TEST(CHInsertTest, UnsupportedFieldEmitsHashError) {
  TableIR t = MakeTradeTable();
  t.columns.push_back(MakeCol("mystery", "mystery", FieldKind::kUnknown,
                               "Unknown", "Unknown"));
  auto f = RenderClickHouseInsert({t}, "example_trade");
  EXPECT_NE(f.source.find("#error"), std::string::npos);
  EXPECT_EQ(f.source.find("// TODO"), std::string::npos);
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

TEST(PGInsertTest, UnsupportedFieldEmitsHashError) {
  TableIR t = MakeTradeTable();
  t.columns.push_back(MakeCol("mystery", "mystery", FieldKind::kUnknown,
                               "Unknown", "Unknown"));
  auto f = RenderTimescaleInsert({t}, "example_trade");
  EXPECT_NE(f.source.find("#error"), std::string::npos);
}

TEST(CHInsertTest, RenamedColumnUsesProtoFieldNameInAccessor) {
  TableIR t = MakeTradeTable();
  ColumnIR col = MakeCol("stock_symbol", "symbol", FieldKind::kString, "String", "TEXT");
  t.columns.push_back(col);
  auto f = RenderClickHouseInsert({t}, "example_trade");
  // SQL column name used in AppendColumn
  EXPECT_NE(f.source.find("block.AppendColumn(\"stock_symbol\""), std::string::npos);
  // Proto accessor uses original field name
  EXPECT_NE(f.source.find("row.symbol()"), std::string::npos);
}

TEST(PGInsertTest, RenamedColumnUsesProtoFieldNameInAccessor) {
  TableIR t = MakeTradeTable();
  ColumnIR col = MakeCol("stock_symbol", "symbol", FieldKind::kString, "String", "TEXT");
  t.columns.push_back(col);
  auto f = RenderTimescaleInsert({t}, "example_trade");
  // SQL column name appears in the stream_to column list
  EXPECT_NE(f.source.find("\"stock_symbol\""), std::string::npos);
  // Proto accessor uses original field name
  EXPECT_NE(f.source.find("row.symbol()"), std::string::npos);
}

TEST(CHInsertTest, EmbedAccessorPrefixUsedInAppend) {
  TableIR t = MakeTradeTable();
  ColumnIR col = MakeCol("loc_lat", "lat", FieldKind::kDouble, "Float64", "DOUBLE PRECISION");
  col.embed_accessor_prefix = "loc().";
  t.columns.push_back(col);
  auto f = RenderClickHouseInsert({t}, "example_trade");
  EXPECT_NE(f.source.find("col_loc_lat->Append(row.loc().lat())"), std::string::npos);
  // Column declaration uses SQL name
  EXPECT_NE(f.source.find("block.AppendColumn(\"loc_lat\""), std::string::npos);
}

TEST(PGInsertTest, EmbedAccessorPrefixUsedInTuple) {
  TableIR t = MakeTradeTable();
  ColumnIR col = MakeCol("loc_lat", "lat", FieldKind::kDouble, "DOUBLE PRECISION", "DOUBLE PRECISION");
  col.embed_accessor_prefix = "loc().";
  t.columns.push_back(col);
  auto f = RenderTimescaleInsert({t}, "example_trade");
  // Column name in stream_to list
  EXPECT_NE(f.source.find("\"loc_lat\""), std::string::npos);
  // Accessor uses the prefix chain
  EXPECT_NE(f.source.find("row.loc().lat()"), std::string::npos);
}

TEST(CHInsertTest, EmbedNullableAccessorUsesHasWithPrefix) {
  TableIR t = MakeTradeTable();
  ColumnIR col = MakeCol("loc_label", "label", FieldKind::kString, "String", "TEXT",
                          /*nullable=*/true);
  col.embed_accessor_prefix = "loc().";
  t.columns.push_back(col);
  auto f = RenderClickHouseInsert({t}, "example_trade");
  EXPECT_NE(f.source.find("row.loc().has_label()"), std::string::npos);
}

TEST(PGInsertTest, EmbedNullableAccessorUsesHasWithPrefix) {
  TableIR t = MakeTradeTable();
  ColumnIR col = MakeCol("loc_label", "label", FieldKind::kString, "String", "TEXT",
                          /*nullable=*/true);
  col.embed_accessor_prefix = "loc().";
  t.columns.push_back(col);
  auto f = RenderTimescaleInsert({t}, "example_trade");
  EXPECT_NE(f.source.find("row.loc().has_label()"), std::string::npos);
}

TEST(PGInsertTest, RepeatedStringArrayDoubleQuotesAndEscapes) {
  TableIR t = MakeTradeTable();
  t.columns.push_back(MakeCol("tags", "tags", FieldKind::kString,
                               "Array(String)", "TEXT[]",
                               /*nullable=*/false, /*repeated=*/true));
  auto f = RenderTimescaleInsert({t}, "example_trade");
  // Each element must be wrapped in double-quotes.
  EXPECT_NE(f.source.find("arr += '\"'"), std::string::npos);
  // A per-character loop must handle backslash and double-quote escaping.
  EXPECT_NE(f.source.find("for (char _c : _elem)"), std::string::npos);
  // Backslash escape: the generated code emits arr += "\\\\";
  EXPECT_NE(f.source.find("if (_c == '\\\\')"), std::string::npos);
  // Double-quote escape: the generated code emits arr += "\\\";
  EXPECT_NE(f.source.find("else if (_c == '\"')"), std::string::npos);
}

// --- Nullable Timestamp in TimescaleDB insert (must use std::optional, not bare string) ---

TEST(PGInsertTest, NullableTimestampUsesOptional) {
  TableIR t = MakeTradeTable();
  ColumnIR col;
  col.name = "optional_ts";
  col.proto_field_name = "optional_ts";
  col.field_kind = FieldKind::kTimestamp;
  col.type_clickhouse = "DateTime64(3)";
  col.type_postgres = "TIMESTAMPTZ";
  col.nullable = true;
  col.has_proto_presence = true;
  t.columns.push_back(col);
  auto f = RenderTimescaleInsert({t}, "example_trade");
  // Must check presence before formatting.
  EXPECT_NE(f.source.find("has_optional_ts()"), std::string::npos);
  // Must return std::optional<std::string> — present or absent.
  EXPECT_NE(f.source.find("std::optional<std::string>{}"), std::string::npos);
  // The gmtime_r/strftime formatting path must still be present.
  EXPECT_NE(f.source.find("gmtime_r"), std::string::npos);
}

TEST(PGInsertTest, NonNullableTimestampNoOptional) {
  // Non-nullable timestamps must NOT wrap in optional.
  auto f = RenderTimescaleInsert({MakeTradeTable()}, "example_trade");
  // timestamp column is not nullable — should not generate optional code for it.
  // (There's no has_timestamp() call in the output.)
  EXPECT_EQ(f.source.find("has_timestamp()"), std::string::npos);
}

// --- ClickHouse nullable enum: LowCardinality(Nullable(String)) ---

TEST(CHInsertTest, NullableEnumUsesLowCardinalityNullable) {
  TableIR t = MakeTradeTable();
  ColumnIR col = MakeCol("opt_side", "opt_side", FieldKind::kEnum,
                          "LowCardinality(String)", "side",
                          /*nullable=*/true, /*repeated=*/false, "example::Side");
  col.has_proto_presence = true;
  t.columns.push_back(col);
  auto f = RenderClickHouseInsert({t}, "example_trade");
  // Must use LowCardinality(Nullable(String)) column type, not Nullable(LowCardinality).
  EXPECT_NE(f.source.find("ColumnLowCardinalityT<clickhouse::ColumnNullableT<clickhouse::ColumnString>>"),
            std::string::npos);
  EXPECT_EQ(f.source.find("ColumnNullableT<clickhouse::ColumnLowCardinalityT"), std::string::npos);
  // Append uses std::optional<std::string>.
  EXPECT_NE(f.source.find("std::optional<std::string>"), std::string::npos);
  EXPECT_NE(f.source.find("has_opt_side()"), std::string::npos);
}

