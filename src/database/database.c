#include <libpq-fe.h>
#include <mosquitto_plugin.h>
#include <pthread.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

typedef struct {
    PGconn *conn;
    int busy;
} ConnWrapper;

ConnWrapper *dbPool;
int maxConn;

pthread_mutex_t dbPoolMutex = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t dbPoolCond = PTHREAD_COND_INITIALIZER;

const char *DB_HOST;
const char *DB_USER;
const char *DB_PASS;
const char *DB_NAME;
const char *DB_PORT;

bool init_db_vars(struct mosquitto_opt *options, int optionsCount) {
    for (int i = 0; i < optionsCount; i++) {
        if (strcmp(options[i].key, "db_host") == 0)
            DB_HOST = options[i].value;
        else if (strcmp(options[i].key, "db_user") == 0)
            DB_USER = options[i].value;
        else if (strcmp(options[i].key, "db_pass") == 0)
            DB_PASS = options[i].value;
        else if (strcmp(options[i].key, "db_name") == 0)
            DB_NAME = options[i].value;
        else if (strcmp(options[i].key, "db_port") == 0)
            DB_PORT = options[i].value;
        else if (strcmp(options[i].key, "max_db_conn") == 0)
            maxConn = atoi(options[i].value);
    }

    if (!DB_HOST || !DB_USER || !DB_PASS || !DB_NAME || DB_PORT == 0) {
        fprintf(stderr, "[WEATHER_COLLECTOR] Missing obligatory config:\n");
        if (!DB_HOST)
            fprintf(stderr, "db_host\n");
        if (!DB_USER)
            fprintf(stderr, "db_user\n");
        if (!DB_PASS)
            fprintf(stderr, "db_pass\n");
        if (!DB_NAME)
            fprintf(stderr, "db_name\n");
        if (!DB_PORT)
            fprintf(stderr, "db_port\n");
        return false;
    }

    if (maxConn <= 0) {
        maxConn = (int)sysconf(_SC_NPROCESSORS_ONLN);
        if (maxConn <= 0)
            maxConn = 1;
    }

    return true;
}

PGconn *init_db_conn(void) {

    PGconn *conn;
    conn = PQsetdbLogin(DB_HOST, DB_PORT,
                        NULL, // options
                        NULL, // tty
                        DB_NAME, DB_USER, DB_PASS);

    if (PQstatus(conn) != CONNECTION_OK) {
        fprintf(stderr, "Connection error: %s\n", PQerrorMessage(conn));
        PQfinish(conn);
        return NULL;
    }

    return conn;
}

bool init_db_pool(void) {
    // Reserve memory for the dbPool
    dbPool = malloc(sizeof(ConnWrapper) * maxConn);
    if (!dbPool) {
        perror("malloc");
        return false;
    }

    for (int i = 0; i < maxConn; i++) {
        dbPool[i].conn = init_db_conn();
        dbPool[i].busy = 0;
        if (!dbPool[i].conn) {
            // Clean all initialized connections
            for (int j = 0; j < i; j++)
                PQfinish(dbPool[j].conn);
            free(dbPool);
            dbPool = NULL;
            return false;
        }
    }
    return true;
}

void free_db_pool(void) {
    if (!dbPool)
        return;

    for (int i = 0; i < maxConn; i++) {
        PQfinish(dbPool[i].conn);
    }

    free(dbPool);
    dbPool = NULL;

    pthread_mutex_destroy(&dbPoolMutex);
    pthread_cond_destroy(&dbPoolCond);
}

PGconn *get_conn(void) {
    PGconn *ret = NULL;

    pthread_mutex_lock(&dbPoolMutex);

    // Wait for a free connection
    while (1) {
        for (int i = 0; i < maxConn; i++) {
            if (dbPool[i].busy == 0) {
                dbPool[i].busy = 1; // Mark connection as busy
                ret = dbPool[i].conn;
                pthread_mutex_unlock(&dbPoolMutex);
                return ret;
            }
        }
        // There aren't any free connection, wait for release_conn to make a signal
        pthread_cond_wait(&dbPoolCond, &dbPoolMutex);
    }
}

void release_conn(PGconn *conn) {
    pthread_mutex_lock(&dbPoolMutex);
    for (int i = 0; i < maxConn; i++) {
        if (dbPool[i].conn == conn) {
            dbPool[i].busy = 0;               // Mark connection as free
            pthread_cond_signal(&dbPoolCond); // Awake a waiting thread
            break;
        }
    }
    pthread_mutex_unlock(&dbPoolMutex);
}
