#include <libpq-fe.h>
#include <mosquitto.h>
#include <mosquitto_broker.h>
#include <mosquitto_plugin.h>
#include <sodium/core.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "database/database.h"
#include "handlers/handlers.h"
#include "pool/pool.h"
#include "types.h"
#include "utils/utils.h"

#define PLUGIN_API_VERSION 5

#define UUID_LEN 36
#define PREFIX_LEN (9 + UUID_LEN + 1) // "stations/" + uuid + '/'

#define MAX_PAYLOAD 4096 // 4KB

static mosquitto_plugin_id_t *pluginId = NULL;

// Authentication callback
static int auth_callback(int event, void *eventData, void *userData) {
    (void)event;
    (void)userData;

    struct mosquitto_evt_basic_auth *auth = eventData;
    const char *username = auth->username;
    const char *password = auth->password;

    mosquitto_log_printf(MOSQ_LOG_INFO,
                         "[WEATHER_COLLECTOR] Auth callback: username=%s password=%s", username,
                         password);

    PGconn *conn = get_conn();
    if (!conn)
        return MOSQ_ERR_AUTH;

    if (!validate_api_key(conn, username, password)) {
        release_conn(conn);
        return MOSQ_ERR_AUTH;
    }

    release_conn(conn);

    return MOSQ_ERR_SUCCESS;
}

// Access control callback
static int acl_callback(int event, void *eventData, void *userData) {
    (void)event;
    (void)userData;

    struct mosquitto_evt_acl_check *acldata = eventData;
    const char *username = mosquitto_client_username(acldata->client);
    const char *topic = acldata->topic;

    if (!username || !topic)
        return MOSQ_ERR_ACL_DENIED;

    if (strncmp(topic, "stations/", 9) != 0)
        return MOSQ_ERR_ACL_DENIED;

    // Verify the uuid
    if (strncmp(topic + 9, username, UUID_LEN) != 0)
        return MOSQ_ERR_ACL_DENIED;

    // Verify the '/'
    if (topic[9 + UUID_LEN] != '/')
        return MOSQ_ERR_ACL_DENIED;

    mosquitto_log_printf(MOSQ_LOG_INFO, "[WEATHER_COLLECTOR] Message allowed");
    return MOSQ_ERR_SUCCESS;
}

int message_callback(int event, void *eventData, void *userData) {
    (void)event;
    (void)userData;

    struct mosquitto_evt_message *msg = eventData;

    const char *username = mosquitto_client_username(msg->client);
    const char *topic = msg->topic;
    const char *payload = msg->payload;
    size_t payloadLen = msg->payloadlen;
    msgType_t msgType = MSG_NULL;

    if (payloadLen > MAX_PAYLOAD)
        return MOSQ_ERR_UNKNOWN;

    const size_t expectedLen = 9 + UUID_LEN + 5; // stations/ + uuid + /data
    size_t len = strlen(topic);

    // Check exact length and suffix "data"
    if (len == expectedLen && topic[len - 4] == 'd' && topic[len - 3] == 'a' &&
        topic[len - 2] == 't' && topic[len - 1] == 'a') {
        msgType = MSG_DATA;
    }

    if (msgType == MSG_NULL)
        return MOSQ_ERR_UNKNOWN;

    // Create the task
    struct msgTask *task = malloc(sizeof(struct msgTask));
    if (!task)
        return MOSQ_ERR_NOMEM;

    // Copy the data
    task->username = strdup(username);
    task->topic = strdup(topic);
    task->payload = malloc(payloadLen);
    task->msgType = msgType;

    if (!task->username || !task->topic || !task->payload) {
        free(task->username);
        free(task->topic);
        free(task->payload);
        free(task);
        return MOSQ_ERR_NOMEM;
    }

    memcpy(task->payload, payload, payloadLen);
    task->payloadLen = payloadLen;

    bool ret;

    switch (msgType) {
        case MSG_DATA:
            ret = add_task(handle_insert_data, task);
            break;
        default:
            ret = false;
    }

    if (!ret) {
        free(task->username);
        free(task->topic);
        free(task->payload);
        free(task);
        return MOSQ_ERR_UNKNOWN;
    }

    return MOSQ_ERR_SUCCESS;
}

int mosquitto_plugin_version(int supportedVersionCount, const int *supportedVersions) {
    for (int i = 0; i < supportedVersionCount; i++) {
        if (supportedVersions[i] == PLUGIN_API_VERSION) {
            return PLUGIN_API_VERSION;
        }
    }
    return 0;
}

int mosquitto_plugin_init(mosquitto_plugin_id_t *identifier, void **userData,
                          struct mosquitto_opt *options, int optionsCount) {
    (void)userData;
    (void)options;
    (void)optionsCount;

    pluginId = identifier;

    mosquitto_log_printf(MOSQ_LOG_INFO, "[WEATHER_COLLECTOR] Plugin initilization");

    if (sodium_init() < 0) {
        mosquitto_log_printf(MOSQ_LOG_INFO, "[WEATHER_COLLECTOR] Failed to initialize sodium");
        return MOSQ_ERR_UNKNOWN;
    }

    if (!init_db_vars(options, optionsCount)) {
        mosquitto_log_printf(MOSQ_LOG_ERR, "[WEATHER_COLLECTOR] Invalid db config");
        return MOSQ_ERR_UNKNOWN;
    }

    if (!init_db_pool()) {
        mosquitto_log_printf(MOSQ_LOG_ERR, "[WEATHER_COLLECTOR] Error opening db connection");
        return MOSQ_ERR_UNKNOWN;
    }

    if (!init_thread_pool(options, optionsCount)) {
        mosquitto_log_printf(MOSQ_LOG_ERR, "[WEATHER_COLLECTOR] Error creating thread pool");
        return MOSQ_ERR_UNKNOWN;
    }

    mosquitto_callback_register(pluginId, MOSQ_EVT_BASIC_AUTH, auth_callback, NULL, NULL);
    mosquitto_callback_register(pluginId, MOSQ_EVT_ACL_CHECK, acl_callback, NULL, NULL);
    mosquitto_callback_register(pluginId, MOSQ_EVT_MESSAGE, message_callback, NULL, NULL);

    mosquitto_log_printf(MOSQ_LOG_INFO, "[WEATHER_COLLECTOR] Plugin correctly initialized");

    return MOSQ_ERR_SUCCESS;
}

int mosquitto_plugin_cleanup(void *userData, struct mosquitto_opt *options, int optionsCount) {
    (void)userData;
    (void)options;
    (void)optionsCount;

    mosquitto_callback_unregister(pluginId, MOSQ_EVT_BASIC_AUTH, auth_callback, NULL);
    mosquitto_callback_unregister(pluginId, MOSQ_EVT_ACL_CHECK, acl_callback, NULL);
    mosquitto_callback_unregister(pluginId, MOSQ_EVT_MESSAGE, message_callback, NULL);

    free_db_pool();
    free_thread_pool();

    mosquitto_log_printf(MOSQ_LOG_INFO, "[WEATHER_COLLECTOR] Plugin cleanup");
    return MOSQ_ERR_SUCCESS;
}
