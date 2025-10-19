# Directorio de salida para archivos generados
set(GENERATED_DIR ${CMAKE_CURRENT_BINARY_DIR}/generated)
file(MAKE_DIRECTORY ${GENERATED_DIR})

# Archivos proto
set(WEATHER_PROTO ${CMAKE_CURRENT_SOURCE_DIR}/weather.proto)
set(WRAPPERS_PROTO ${PROTOBUF_INCLUDE_DIRS}/google/protobuf/wrappers.proto)

# Archivos generados
# Output directory for generated C files
set(GENERATED_DIR ${CMAKE_CURRENT_BINARY_DIR}/generated)
file(MAKE_DIRECTORY ${GENERATED_DIR})

# Proto files
set(WEATHER_PROTO ${CMAKE_CURRENT_SOURCE_DIR}/weather.proto)
set(WRAPPERS_PROTO ${PROTOBUF_INCLUDE_DIRS}/google/protobuf/wrappers.proto)

# Generated C files
set(WEATHER_PB_C_C ${GENERATED_DIR}/weather.pb-c.c)
set(WEATHER_PB_C_H ${GENERATED_DIR}/weather.pb-c.h)
set(WRAPPERS_PB_C_C ${GENERATED_DIR}/google/protobuf/wrappers.pb-c.c)
set(WRAPPERS_PB_C_H ${GENERATED_DIR}/google/protobuf/wrappers.pb-c.h)

# Make folder for google/protobuf headers inside the generated directory
file(MAKE_DIRECTORY ${GENERATED_DIR}/google/protobuf)

# Generate weather.pb-c.c/h from weather.proto
add_custom_command(
    OUTPUT ${WEATHER_PB_C_C} ${WEATHER_PB_C_H}
    COMMAND protoc --c_out=${GENERATED_DIR} -I=${CMAKE_CURRENT_SOURCE_DIR} -I=${PROTOBUF_INCLUDE_DIRS} ${WEATHER_PROTO}
    DEPENDS ${WEATHER_PROTO}
    COMMENT "Generating weather.pb-c.c and weather.pb-c.h"
    VERBATIM
)

# Generate wrappers.pb-c.c/h from wrappers.proto (Google types)
add_custom_command(
    OUTPUT ${WRAPPERS_PB_C_C} ${WRAPPERS_PB_C_H}
    COMMAND protoc --c_out=${GENERATED_DIR} -I=${PROTOBUF_INCLUDE_DIRS} ${WRAPPERS_PROTO}
    DEPENDS ${WRAPPERS_PROTO}
    COMMENT "Generating wrappers.pb-c.c and wrappers.pb-c.h"
    VERBATIM
)

# Static library including handlers.c and generated protobuf C files
add_library(weather_handlers STATIC
    handlers.c
    ${WEATHER_PB_C_C}
    ${WRAPPERS_PB_C_C}
)

# Enable position-independent code (for linking into shared libs)
set_target_properties(weather_handlers PROPERTIES POSITION_INDEPENDENT_CODE ON)

# Include directories: source code, generated files, protobuf headers
target_include_directories(weather_handlers
    PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}    # your source code
    ${GENERATED_DIR}               # generated files
    ${PROTOBUF_INCLUDE_DIRS}       # protobuf headers (wrappers.proto)
    PRIVATE
    ${PostgreSQL_INCLUDE_DIRS}
)

# Link against protobuf-c library
target_link_libraries(weather_handlers
    PUBLIC
    ${PROTOBUF_LIBRARIES}
    PRIVATE
    ${PostgreSQL_LIBRARIES}
)
