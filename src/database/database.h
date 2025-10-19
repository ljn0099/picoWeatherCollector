#ifndef DATABASE_H
#define DATABASE_H

#include <libpq-fe.h>
#include <stdbool.h>

struct mosquitto_opt;

bool init_db_vars(struct mosquitto_opt *options, int option_count);

bool init_db_pool(void);
void free_db_pool(void);

PGconn *get_conn(void);

void release_conn(PGconn *conn);

#endif
