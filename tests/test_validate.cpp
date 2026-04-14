#include <gtest/gtest.h>

#include "ir.hpp"
#include "validate.hpp"

namespace {

TableIR MakeTable(const std::string& name,
                  const std::string& ch_order_by = "ts",
                  const std::string& ts_time_column = "ts") {
  TableIR t;
  t.name = name;
  t.ch_order_by = ch_order_by;
  t.ts_time_column = ts_time_column;
  ColumnIR col;
  col.name = "ts";
  col.type_clickhouse = "DateTime64(3)";
  col.type_postgres = "TIMESTAMPTZ";
  col.nullable = false;
  col.has_proto_presence = true;  // Timestamp is a message type — always has presence
  t.columns.push_back(col);
  return t;
}

}  // namespace

// --- ClickHouse validation ---

TEST(ValidateClickHouseTest, ValidTable) {
  auto errors = ValidateClickHouseTables({MakeTable("t1")});
  EXPECT_TRUE(errors.empty());
}

TEST(ValidateClickHouseTest, MissingOrderBy) {
  TableIR t = MakeTable("t1");
  t.ch_order_by = "";
  auto errors = ValidateClickHouseTables({t});
  ASSERT_EQ(errors.size(), 1u);
  EXPECT_NE(errors[0].find("ch_order_by"), std::string::npos);
}

TEST(ValidateClickHouseTest, MultipleTablesOneInvalid) {
  auto errors = ValidateClickHouseTables({MakeTable("t1"), MakeTable("t2", "")});
  ASSERT_EQ(errors.size(), 1u);
  EXPECT_NE(errors[0].find("t2"), std::string::npos);
}

TEST(ValidateClickHouseTest, EmptyInput) {
  EXPECT_TRUE(ValidateClickHouseTables({}).empty());
}

// --- Timescale validation ---

TEST(ValidateTimescaleTest, ValidTable) {
  auto errors = ValidateTimescaleTables({MakeTable("t1")});
  EXPECT_TRUE(errors.empty());
}

TEST(ValidateTimescaleTest, MissingTimeColumn) {
  TableIR t = MakeTable("t1");
  t.ts_time_column = "";
  auto errors = ValidateTimescaleTables({t});
  ASSERT_EQ(errors.size(), 1u);
  EXPECT_NE(errors[0].find("ts_time_column"), std::string::npos);
}

TEST(ValidateTimescaleTest, TimeColumnNotInSchema) {
  TableIR t = MakeTable("t1");
  t.ts_time_column = "nonexistent";
  auto errors = ValidateTimescaleTables({t});
  ASSERT_EQ(errors.size(), 1u);
  EXPECT_NE(errors[0].find("nonexistent"), std::string::npos);
}

TEST(ValidateTimescaleTest, NullableTimeColumn) {
  TableIR t = MakeTable("t1");
  t.columns[0].nullable = true;
  auto errors = ValidateTimescaleTables({t});
  ASSERT_EQ(errors.size(), 1u);
  EXPECT_NE(errors[0].find("NOT NULL"), std::string::npos);
}

TEST(ValidateTimescaleTest, EmptyInput) {
  EXPECT_TRUE(ValidateTimescaleTables({}).empty());
}

// --- Nullable column without proto presence ---

namespace {

ColumnIR MakeNullableNoPresenceCol(const std::string& name) {
  ColumnIR col;
  col.name = name;
  col.type_clickhouse = "Nullable(Int32)";
  col.type_postgres = "INTEGER";
  col.nullable = true;
  col.has_proto_presence = false;
  return col;
}

}  // namespace

TEST(ValidateClickHouseTest, NullableNoPresenceIsError) {
  TableIR t = MakeTable("t1");
  t.columns.push_back(MakeNullableNoPresenceCol("bad_col"));
  auto errors = ValidateClickHouseTables({t});
  ASSERT_FALSE(errors.empty());
  EXPECT_NE(errors[0].find("bad_col"), std::string::npos);
}

TEST(ValidateClickHouseTest, NullableWithPresenceIsOk) {
  TableIR t = MakeTable("t1");
  ColumnIR col = MakeNullableNoPresenceCol("good_col");
  col.has_proto_presence = true;  // 'optional' field — valid
  t.columns.push_back(col);
  EXPECT_TRUE(ValidateClickHouseTables({t}).empty());
}

TEST(ValidateClickHouseTest, RepeatedNullableNoPresenceIsOk) {
  TableIR t = MakeTable("t1");
  ColumnIR col = MakeNullableNoPresenceCol("arr_col");
  col.repeated = true;  // repeated fields are not a problem
  t.columns.push_back(col);
  EXPECT_TRUE(ValidateClickHouseTables({t}).empty());
}

TEST(ValidateTimescaleTest, NullableNoPresenceIsError) {
  TableIR t = MakeTable("t1");
  t.columns.push_back(MakeNullableNoPresenceCol("bad_col"));
  auto errors = ValidateTimescaleTables({t});
  ASSERT_FALSE(errors.empty());
  EXPECT_NE(errors[0].find("bad_col"), std::string::npos);
}

TEST(ValidateTimescaleTest, NullableWithPresenceIsOk) {
  TableIR t = MakeTable("t1");
  ColumnIR col = MakeNullableNoPresenceCol("good_col");
  col.has_proto_presence = true;
  t.columns.push_back(col);
  EXPECT_TRUE(ValidateTimescaleTables({t}).empty());
}

TEST(ValidateTimescaleTest, RepeatedNullableNoPresenceIsOk) {
  TableIR t = MakeTable("t1");
  ColumnIR col = MakeNullableNoPresenceCol("arr_col");
  col.repeated = true;
  t.columns.push_back(col);
  EXPECT_TRUE(ValidateTimescaleTables({t}).empty());
}
