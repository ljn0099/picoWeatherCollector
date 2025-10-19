add_library(weather_utils
    utils.c
)

set_target_properties(weather_utils PROPERTIES POSITION_INDEPENDENT_CODE ON)

target_include_directories(weather_utils
    PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${SODIUM_INCLUDE_DIRS}
    ${PostgreSQL_INCLUDE_DIRS}
)

target_link_libraries(weather_utils
    PUBLIC
    ${SODIUM_LIBRARIES}
    ${PostgreSQL_LIBRARIES}
)
