#ifndef UTILS_H
#define UTILS_H

#include <stdbool.h>

bool validate_api_key(PGconn *conn, const char *stationUUID, const char *apiKey);

#endif
