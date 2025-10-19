#include <libpq-fe.h>
#include <sodium/crypto_generichash.h>
#include <sodium/utils.h>
#include <stdbool.h>
#include <stdio.h>
#include <string.h>

#define KEY_ENTROPY 32
#define BASE64_VARIANT sodium_base64_VARIANT_URLSAFE_NO_PADDING

bool validate_api_key(PGconn *conn, const char *stationUUID, const char *apiKey) {
    if (!conn || !stationUUID || !apiKey)
        return false;

    unsigned char recievedKey[KEY_ENTROPY];
    if (sodium_base642bin(recievedKey, sizeof(recievedKey), apiKey, strlen(apiKey), NULL, NULL,
                          NULL, BASE64_VARIANT) != 0) {
        return false;
    }

    unsigned char recievedKeyHash[crypto_generichash_BYTES];
    crypto_generichash(recievedKeyHash, sizeof(recievedKeyHash), recievedKey, sizeof(recievedKey),
                       NULL, 0);

    // Convert the hash into base64 for the query
    char recievedKeyHashB64[sodium_base64_ENCODED_LEN((sizeof(recievedKeyHash)), BASE64_VARIANT)];
    sodium_bin2base64(recievedKeyHashB64, sizeof(recievedKeyHashB64), recievedKeyHash,
                      (sizeof(recievedKeyHash)), BASE64_VARIANT);

    PGresult *res;

    const char *paramValues[2] = {recievedKeyHashB64, stationUUID};

    res = PQexecParams(conn,
                       "SELECT 1 "
                       "FROM auth.api_keys k "
                       "JOIN stations.stations s ON s.uuid = $2 "
                       "WHERE k.api_key = $1 "
                       "  AND k.revoked_at IS NULL "
                       "  AND (k.expires_at IS NULL OR k.expires_at > NOW()) "
                       "  AND (k.station_id = s.station_id)",
                       2, NULL, paramValues, NULL, NULL, 0);

    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        fprintf(stderr, "Error executing the query: %s", PQerrorMessage(conn));
        PQclear(res);
        return false;
    }

    if (PQntuples(res) <= 0) {
        PQclear(res);
        return false;
    }

    PQclear(res);
    return true;
}
