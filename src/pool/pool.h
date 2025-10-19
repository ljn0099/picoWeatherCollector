#ifndef POOL_H
#define POOL_H

#include <stdbool.h>

bool init_thread_pool(struct mosquitto_opt *options, int optionsCount);

void free_thread_pool(void);

bool add_task(void (*function)(void *), void *arg);

#endif
