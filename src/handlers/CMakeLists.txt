# Nanopb protobuf generation

set(NANOPB_DIR "${CMAKE_SOURCE_DIR}/nanopb")
set(NANOPB_PLUGIN "${NANOPB_DIR}/generator/protoc-gen-nanopb")
set(PROTOC_EXECUTABLE "${NANOPB_DIR}/generator/protoc")

set(GENERATED_DIR ${CMAKE_CURRENT_BINARY_DIR}/generated)
file(MAKE_DIRECTORY ${GENERATED_DIR})

set(WEATHER_PB_C  "${GENERATED_DIR}/weather.pb.c")
set(WEATHER_PB_H  "${GENERATED_DIR}/weather.pb.h")
set(WRAPPERS_PB_C "${GENERATED_DIR}/google/protobuf/wrappers.pb.c")
set(WRAPPERS_PB_H "${GENERATED_DIR}/google/protobuf/wrappers.pb.h")

add_custom_command(
    OUTPUT ${WEATHER_PB_C} ${WEATHER_PB_H} ${WRAPPERS_PB_C} ${WRAPPERS_PB_H}
    COMMAND ${PROTOC_EXECUTABLE}
        --plugin=protoc-gen-nanopb=${NANOPB_PLUGIN}
        --nanopb_out=${GENERATED_DIR}
        -I${CMAKE_CURRENT_SOURCE_DIR}
        -I${NANOPB_DIR}/generator/proto
        ${CMAKE_CURRENT_SOURCE_DIR}/weather.proto
        google/protobuf/wrappers.proto
    DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/weather.proto
    COMMENT "Generating nanopb sources from .proto files"
)

add_custom_target(generate_weather_pb
    DEPENDS ${WEATHER_PB_C} ${WEATHER_PB_H} ${WRAPPERS_PB_C} ${WRAPPERS_PB_H}
)

# Nanopb lib

add_library(nanopb_lib STATIC
    ${NANOPB_DIR}/pb_common.c
    ${NANOPB_DIR}/pb_encode.c
    ${NANOPB_DIR}/pb_decode.c
    ${WEATHER_PB_C}
    ${WRAPPERS_PB_C}
)

add_dependencies(nanopb_lib generate_weather_pb)

target_include_directories(nanopb_lib
    PUBLIC
        ${GENERATED_DIR}
        ${NANOPB_DIR}
)

set_target_properties(nanopb_lib PROPERTIES POSITION_INDEPENDENT_CODE ON)

# Main file

add_library(weather_handlers STATIC
    handlers.c
)

target_include_directories(weather_handlers
    PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}
    PRIVATE
    ${PostgreSQL_INCLUDE_DIRS}
)

target_link_libraries(weather_handlers
    PRIVATE
    nanopb_lib
    ${PostgreSQL_LIBRARIES}
)

# Enable position-independent code (for linking into shared libs)
set_target_properties(weather_handlers PROPERTIES POSITION_INDEPENDENT_CODE ON)
