add_library(weather_db STATIC
    database.c
)

set_target_properties(weather_db PROPERTIES POSITION_INDEPENDENT_CODE ON)

target_include_directories(weather_db
    PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${PostgreSQL_INCLUDE_DIRS}
    ${MOSQUITTO_INCLUDE_DIRS}
)

target_link_libraries(weather_db
    PUBLIC
    ${PostgreSQL_LIBRARIES}
    ${MOSQUITTO_LIBRARIES}
)
