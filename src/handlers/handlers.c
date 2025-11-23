#include <inttypes.h>
#include <libpq-fe.h>
#include <stdio.h>
#include <stdlib.h>

#include "../database/database.h"
#include "../types.h"

#include "pb.h"
#include "pb_decode.h"
#include "pb_encode.h"
#include "weather.pb.h"

#define N_FLOATS 11
#define FLOAT_STR_SIZE 32
#define UINT64_STR_SIZE 21

typedef enum {
    TEMP,
    HUMIDITY,
    PRESSURE,
    LUX,
    UVI,
    WIND_SPEED,
    WIND_DIRECTION,
    GUST_SPEED,
    GUST_DIRECTION,
    RAINFALL,
    SOLAR_IRRADIANCE
} MeasurementIndex;

char *floats_to_strings(const weather_WeatherMeasurement *m, char *ptrs[N_FLOATS]) {
    const bool has[N_FLOATS] = {
        m->has_temperature,   m->has_humidity,  m->has_pressure,       m->has_lux,
        m->has_uvi,           m->has_windSpeed, m->has_windDirection,  m->has_gustSpeed,
        m->has_gustDirection, m->has_rainfall,  m->has_solarIrradiance};

    const float values[N_FLOATS] = {
        m->temperature.value,   m->humidity.value,  m->pressure.value,       m->lux.value,
        m->uvi.value,           m->windSpeed.value, m->windDirection.value,  m->gustSpeed.value,
        m->gustDirection.value, m->rainfall.value,  m->solarIrradiance.value};

    int count = 0;
    for (int i = 0; i < N_FLOATS; i++)
        if (has[i])
            count++;

    char *buffer = malloc(count * FLOAT_STR_SIZE);
    if (!buffer)
        return NULL;

    int offset = 0;
    for (int i = 0; i < N_FLOATS; i++) {
        if (has[i]) {
            ptrs[i] = buffer + offset;
            snprintf(ptrs[i], FLOAT_STR_SIZE, "%f", values[i]);
            offset += FLOAT_STR_SIZE;
        }
        else {
            ptrs[i] = NULL;
        }
    }

    return buffer;
}

void handle_insert_data(void *arg) {
    struct msgTask *task = (struct msgTask *)arg;

    weather_WeatherMeasurement meas = weather_WeatherMeasurement_init_zero;
    char *buffer = NULL;
    PGresult *res = NULL;
    PGconn *conn = NULL;

    pb_istream_t stream = pb_istream_from_buffer(task->payload, task->payloadLen);

    if (!pb_decode(&stream, weather_WeatherMeasurement_fields, &meas)) {
        fprintf(stderr, "Nanopb decode error: %s\n", PB_GET_ERROR(&stream));
        goto cleanup;
    }

    if (meas.periodStart == 0 || meas.periodEnd == 0)
        goto cleanup;

    char *ptrs[N_FLOATS];
    buffer = floats_to_strings(&meas, ptrs);
    if (!buffer)
        goto cleanup;

    char periodStartStr[UINT64_STR_SIZE];
    char periodEndStr[UINT64_STR_SIZE];

    snprintf(periodStartStr, sizeof(periodStartStr), "%" PRIu64, meas.periodStart);
    snprintf(periodEndStr, sizeof(periodEndStr), "%" PRIu64, meas.periodEnd);

    const char *paramValues[14] = {
        periodStartStr,   periodEndStr,          task->username,   ptrs[TEMP],
        ptrs[HUMIDITY],   ptrs[PRESSURE],        ptrs[LUX],        ptrs[UVI],
        ptrs[WIND_SPEED], ptrs[WIND_DIRECTION],  ptrs[GUST_SPEED], ptrs[GUST_DIRECTION],
        ptrs[RAINFALL],   ptrs[SOLAR_IRRADIANCE]};

    conn = get_conn();

    res = PQexecParams(conn,
                       "INSERT INTO weather.weather_data (station_id, time_range, temperature, "
                       "humidity, pressure, lux, uvi, wind_speed, wind_direction, gust_speed, "
                       "gust_direction, rainfall, solar_irradiance) "
                       "VALUES ("
                       "  (SELECT station_id FROM stations.stations WHERE uuid = $3), "
                       "  tstzrange(to_timestamp($1) AT TIME ZONE 'UTC', "
                       "            to_timestamp($2) AT TIME ZONE 'UTC', '[)'), "
                       "  $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14"
                       ")",
                       14, NULL, paramValues, NULL, NULL, 0);

    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        fprintf(stderr, "Postgres error: %s\n", PQerrorMessage(conn));
    }

cleanup:
    if (conn)
        release_conn(conn);
    if (res)
        PQclear(res);
    if (buffer)
        free(buffer);

    if (task) {
        free(task->username);
        free(task->topic);
        free(task->payload);
        free(task);
    }
}
