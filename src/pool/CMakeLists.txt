add_library(weather_pool STATIC
    pool.c
)

set_target_properties(weather_pool PROPERTIES POSITION_INDEPENDENT_CODE ON)

target_include_directories(weather_pool
    PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}
)
