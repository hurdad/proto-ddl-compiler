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
