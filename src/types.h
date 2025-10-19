#ifndef TYPES_H
#define TYPES_H

typedef enum {
    MSG_NULL = 0,
    MSG_DATA
} msgType_t;

struct msgTask {
    char *username;
    char *topic;
    uint8_t *payload;
    size_t payloadLen;
    msgType_t msgType;
};

#endif
