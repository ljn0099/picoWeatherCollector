#include <inttypes.h>
#include <libpq-fe.h>
#include <stdio.h>
#include <stdlib.h>

#include "../database/database.h"
#include "../types.h"
#include "google/protobuf/wrappers.pb-c.h"
#include "weather.pb-c.h"

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

char *floats_to_strings(const Google__Protobuf__FloatValue *vals[N_FLOATS], char *ptrs[N_FLOATS]) {
    int count = 0;
    for (int i = 0; i < N_FLOATS; i++) {
        if (vals[i])
            count++;
    }

    char *buffer = malloc(count * FLOAT_STR_SIZE);
    if (!buffer)
        return NULL;

    int offset = 0;
    for (int i = 0; i < N_FLOATS; i++) {
        if (vals[i]) {
            ptrs[i] = buffer + offset;
            snprintf(ptrs[i], FLOAT_STR_SIZE, "%f", vals[i]->value);
            offset += FLOAT_STR_SIZE;
        }
        else {
            ptrs[i] = NULL;
        }
    }

    // Return buffer for free
    return buffer;
}

void handle_insert_data(void *arg) {
    struct msgTask *task = (struct msgTask *)arg;

    Weather__WeatherMeasurement *meas = NULL;
    char *buffer = NULL;
    PGresult *res = NULL;
    PGconn *conn = NULL;

    meas = weather__weather_measurement__unpack(NULL, task->payloadLen, task->payload);
    if (!meas) {
        fprintf(stderr, "Error: cannot unpack the message\n");
        goto cleanup;
    }

    if (meas->periodstart == 0 || meas->periodend == 0) {
        goto cleanup;
    }

    const Google__Protobuf__FloatValue *vals[N_FLOATS] = {
        meas->temperature,   meas->humidity,  meas->pressure,       meas->lux,
        meas->uvi,           meas->windspeed, meas->winddirection,  meas->gustspeed,
        meas->gustdirection, meas->rainfall,  meas->solarirradiance};
    char *ptrs[N_FLOATS];
    buffer = floats_to_strings(vals, ptrs);
    if (!buffer) {
        goto cleanup;
    }

    char periodStart[UINT64_STR_SIZE];
    char periodEnd[UINT64_STR_SIZE];
    snprintf(periodStart, sizeof(periodStart), "%" PRIu64, meas->periodstart);
    snprintf(periodEnd, sizeof(periodEnd), "%" PRIu64, meas->periodend);

    const char *paramValues[14] = {periodStart,      periodEnd,
                                   task->username,   ptrs[TEMP],
                                   ptrs[HUMIDITY],   ptrs[PRESSURE],
                                   ptrs[LUX],        ptrs[UVI],
                                   ptrs[WIND_SPEED], ptrs[WIND_DIRECTION],
                                   ptrs[GUST_SPEED], ptrs[GUST_DIRECTION],
                                   ptrs[RAINFALL],   ptrs[SOLAR_IRRADIANCE]};

    conn = get_conn();
    res = PQexecParams(conn,
                       "INSERT INTO weather.weather_data (station_id, time_range, temperature, "
                       "humidity, pressure, lux, uvi, wind_speed, wind_direction, gust_speed, "
                       "gust_direction, rainfall, solar_irradiance) "
                       "VALUES ("
                       "  (SELECT station_id FROM stations.stations WHERE uuid = $3), "
                       "  tstzrange(to_timestamp($1) AT TIME ZONE 'UTC', to_timestamp($2) AT TIME "
                       "ZONE 'UTC', '[)'), "
                       "  $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14"
                       ")",
                       14, NULL, paramValues, NULL, NULL, 0);

    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        fprintf(stderr, "Error executing the query: %s", PQerrorMessage(conn));
        goto cleanup;
    }

    goto cleanup;

cleanup:
    if (conn)
        release_conn(conn);
    if (res)
        PQclear(res);
    if (buffer)
        free(buffer);
    if (meas)
        weather__weather_measurement__free_unpacked(meas, NULL);
    if (task) {
        free(task->username);
        free(task->topic);
        free(task->payload);
        free(task);
    }
    return;
}
