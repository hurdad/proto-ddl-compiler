#include <gtest/gtest.h>

#include <google/protobuf/descriptor.h>

#include "mapper.hpp"
#include "mapper_test_types.pb.h"

namespace {

const google::protobuf::Descriptor* kDesc = test::AllTypes::descriptor();

const google::protobuf::FieldDescriptor* F(const char* name) {
  return kDesc->FindFieldByName(name);
}

}  // namespace

TEST(MapperTest, Int32) {
  auto r = MapFieldTypes(*F("f_int32"));
  ASSERT_TRUE(r.has_value());
  EXPECT_EQ(r->clickhouse, "Int32");
  EXPECT_EQ(r->postgres, "INTEGER");
}

TEST(MapperTest, Int64) {
  auto r = MapFieldTypes(*F("f_int64"));
  ASSERT_TRUE(r.has_value());
  EXPECT_EQ(r->clickhouse, "Int64");
  EXPECT_EQ(r->postgres, "BIGINT");
}

TEST(MapperTest, UInt32) {
  auto r = MapFieldTypes(*F("f_uint32"));
  ASSERT_TRUE(r.has_value());
  EXPECT_EQ(r->clickhouse, "UInt32");
  EXPECT_EQ(r->postgres, "INTEGER");
}

TEST(MapperTest, UInt64) {
  auto r = MapFieldTypes(*F("f_uint64"));
  ASSERT_TRUE(r.has_value());
  EXPECT_EQ(r->clickhouse, "UInt64");
  EXPECT_EQ(r->postgres, "BIGINT");
}

TEST(MapperTest, Float) {
  auto r = MapFieldTypes(*F("f_float"));
  ASSERT_TRUE(r.has_value());
  EXPECT_EQ(r->clickhouse, "Float32");
  EXPECT_EQ(r->postgres, "REAL");
}

TEST(MapperTest, Double) {
  auto r = MapFieldTypes(*F("f_double"));
  ASSERT_TRUE(r.has_value());
  EXPECT_EQ(r->clickhouse, "Float64");
  EXPECT_EQ(r->postgres, "DOUBLE PRECISION");
}

TEST(MapperTest, Bool) {
  auto r = MapFieldTypes(*F("f_bool"));
  ASSERT_TRUE(r.has_value());
  EXPECT_EQ(r->clickhouse, "Bool");
  EXPECT_EQ(r->postgres, "BOOLEAN");
}

TEST(MapperTest, String) {
  auto r = MapFieldTypes(*F("f_string"));
  ASSERT_TRUE(r.has_value());
  EXPECT_EQ(r->clickhouse, "String");
  EXPECT_EQ(r->postgres, "TEXT");
}

TEST(MapperTest, Bytes) {
  auto r = MapFieldTypes(*F("f_bytes"));
  ASSERT_TRUE(r.has_value());
  EXPECT_EQ(r->clickhouse, "String");
  EXPECT_EQ(r->postgres, "BYTEA");
}

TEST(MapperTest, Timestamp) {
  auto r = MapFieldTypes(*F("f_timestamp"));
  ASSERT_TRUE(r.has_value());
  EXPECT_EQ(r->clickhouse, "DateTime64(3)");
  EXPECT_EQ(r->postgres, "TIMESTAMPTZ");
}

TEST(MapperTest, Enum) {
  auto r = MapFieldTypes(*F("f_enum"));
  ASSERT_TRUE(r.has_value());
  EXPECT_EQ(r->clickhouse, "LowCardinality(String)");
  EXPECT_EQ(r->postgres, "side");
}

TEST(MapperTest, RepeatedInt32) {
  auto r = MapFieldTypes(*F("f_repeated_int32"));
  ASSERT_TRUE(r.has_value());
  EXPECT_EQ(r->clickhouse, "Array(Int32)");
  EXPECT_EQ(r->postgres, "INTEGER[]");
}
