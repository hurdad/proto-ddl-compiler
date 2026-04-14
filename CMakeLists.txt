cmake_minimum_required(VERSION 3.20)
project(proto-ddl-compiler VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

find_package(Protobuf REQUIRED)

protobuf_generate_cpp(DB_OPTIONS_SRCS DB_OPTIONS_HDRS proto/db_options.proto)

add_executable(protoc-gen-dbddl
  src/main.cpp
  src/generator.cpp
  src/mapper.cpp
  src/validate.cpp
  src/descriptor_utils.cpp
  src/backends/clickhouse_renderer.cpp
  src/backends/timescale_renderer.cpp
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
