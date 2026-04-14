cmake_minimum_required(VERSION 3.20)
project(proto-ddl-compiler VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

find_package(Protobuf REQUIRED)
find_package(PkgConfig REQUIRED)
pkg_check_modules(PQXX REQUIRED libpqxx)

# clickhouse-cpp (third-party submodule)
set(WITH_OPENSSL OFF CACHE BOOL "" FORCE)
add_subdirectory(third_party/clickhouse-cpp EXCLUDE_FROM_ALL)

protobuf_generate_cpp(DB_OPTIONS_SRCS DB_OPTIONS_HDRS proto/db_options.proto)

add_executable(protoc-gen-dbddl
  src/main.cpp
  src/generator.cpp
  src/mapper.cpp
  src/validate.cpp
  src/descriptor_utils.cpp
  src/backends/clickhouse_renderer.cpp
  src/backends/clickhouse_insert_renderer.cpp
  src/backends/timescale_renderer.cpp
  src/backends/timescale_insert_renderer.cpp
  ${DB_OPTIONS_SRCS}
)

target_include_directories(protoc-gen-dbddl
  PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/src
    ${CMAKE_CURRENT_BINARY_DIR}
    ${Protobuf_INCLUDE_DIRS}
)

target_link_libraries(protoc-gen-dbddl
  PRIVATE
    protobuf::libprotobuf
    protobuf::libprotoc
)

install(TARGETS protoc-gen-dbddl RUNTIME DESTINATION bin)
install(FILES proto/db_options.proto
        DESTINATION share/proto-ddl-compiler/proto)

# --- Unit tests ---
include(CTest)
find_package(GTest REQUIRED)

protobuf_generate_cpp(MAPPER_TEST_SRCS MAPPER_TEST_HDRS tests/mapper_test_types.proto)

add_executable(dbddl_tests
  tests/test_mapper.cpp
  tests/test_validate.cpp
  tests/test_renderers.cpp
  tests/test_insert_renderers.cpp
  src/mapper.cpp
  src/validate.cpp
  src/backends/clickhouse_renderer.cpp
  src/backends/clickhouse_insert_renderer.cpp
  src/backends/timescale_renderer.cpp
  src/backends/timescale_insert_renderer.cpp
  ${DB_OPTIONS_SRCS}
  ${MAPPER_TEST_SRCS}
)

target_include_directories(dbddl_tests
  PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/src
    ${CMAKE_CURRENT_BINARY_DIR}
    ${Protobuf_INCLUDE_DIRS}
    ${GTEST_INCLUDE_DIRS}
)

target_link_libraries(dbddl_tests
  PRIVATE
    protobuf::libprotobuf
    GTest::gtest
    GTest::gtest_main
)

add_test(NAME dbddl_tests COMMAND dbddl_tests)

# --- Generated insert compile test ---
# Run the plugin against each proto and compile the output to verify that the
# generated C++ insert code is valid.

set(GENERATED_INSERT_DIR ${CMAKE_CURRENT_BINARY_DIR}/generated_inserts)
set(GENERATED_INSERT_SRCS "")
set(GENERATED_PROTO_SRCS "")

# generate_insert_code(<proto_stem>)
# Registers a custom command that invokes the plugin on proto/<proto_stem>.proto
# and appends the generated .cc files to GENERATED_INSERT_SRCS.
function(generate_insert_code PROTO_STEM)
  set(PROTO_FILE ${CMAKE_CURRENT_SOURCE_DIR}/proto/${PROTO_STEM}.proto)
  set(CH_H   ${GENERATED_INSERT_DIR}/${PROTO_STEM}.ch_insert.h)
  set(CH_CC  ${GENERATED_INSERT_DIR}/${PROTO_STEM}.ch_insert.cc)
  set(PG_H   ${GENERATED_INSERT_DIR}/${PROTO_STEM}.pg_insert.h)
  set(PG_CC  ${GENERATED_INSERT_DIR}/${PROTO_STEM}.pg_insert.cc)

  add_custom_command(
    OUTPUT ${CH_H} ${CH_CC} ${PG_H} ${PG_CC}
    COMMAND ${CMAKE_COMMAND} -E make_directory ${GENERATED_INSERT_DIR}
    COMMAND protoc
      --plugin=protoc-gen-dbddl=$<TARGET_FILE:protoc-gen-dbddl>
      --dbddl_out=${GENERATED_INSERT_DIR}
      -I ${CMAKE_CURRENT_SOURCE_DIR}/proto
      ${PROTO_FILE}
    DEPENDS
      protoc-gen-dbddl
      ${PROTO_FILE}
      ${CMAKE_CURRENT_SOURCE_DIR}/proto/db_options.proto
    COMMENT "Generating insert code from ${PROTO_STEM}.proto"
  )

  protobuf_generate_cpp(PROTO_SRCS PROTO_HDRS ${PROTO_FILE})

  set(GENERATED_INSERT_SRCS ${GENERATED_INSERT_SRCS} ${CH_CC} ${PG_CC} PARENT_SCOPE)
  set(GENERATED_PROTO_SRCS  ${GENERATED_PROTO_SRCS}  ${PROTO_SRCS}     PARENT_SCOPE)
endfunction()

generate_insert_code(example_trade)
generate_insert_code(kitchen_sink)

add_library(generated_inserts STATIC
  ${GENERATED_INSERT_SRCS}
  ${GENERATED_PROTO_SRCS}
)

target_include_directories(generated_inserts
  PUBLIC
    ${GENERATED_INSERT_DIR}
    ${CMAKE_CURRENT_BINARY_DIR}
    ${Protobuf_INCLUDE_DIRS}
    ${PQXX_INCLUDE_DIRS}
)

target_link_libraries(generated_inserts
  PUBLIC
    clickhouse-cpp-lib
    ${PQXX_LIBRARIES}
    protobuf::libprotobuf
)
