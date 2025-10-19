add_subdirectory(utils)
add_subdirectory(database)
add_subdirectory(pool)
add_subdirectory(handlers)

add_library(picoWeatherCollector SHARED
    weather_collector.c
)

target_include_directories(picoWeatherCollector
    PRIVATE
    ${MOSQUITTO_INCLUDE_DIRS}
    ${SODIUM_INCLUDE_DIRS}
    ${PostgreSQL_INCLUDE_DIRS}
)

target_link_libraries(picoWeatherCollector
    PRIVATE
    weather_db
    weather_utils
    weather_pool
    weather_handlers
    ${MOSQUITTO_LIBRARIES}
    ${SODIUM_LIBRARIES}
    ${PostgreSQL_LIBRARIES}
)

set_target_properties(picoWeatherCollector PROPERTIES
    OUTPUT_NAME "picoWeatherCollector"
    PREFIX ""
    SUFFIX ".so"
    LIBRARY_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}"
)
