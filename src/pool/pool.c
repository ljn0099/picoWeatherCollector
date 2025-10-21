#include <mosquitto_plugin.h>
#include <pthread.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

typedef struct Task {
    void (*function)(void *);
    void *arg;
    struct Task *next;
} Task;

typedef struct {
    Task *front;
    Task *rear;
    pthread_mutex_t mutex;
    pthread_cond_t cond;
    pthread_cond_t done;
    int shutdown;
    int taskCount;
} TaskQueue;

typedef struct {
    pthread_t *threads;
    TaskQueue queue;
    int numThreads;
} ThreadPool;

// Global pool
ThreadPool pool;

void *worker(void *arg) {
    (void)arg;

    TaskQueue *q = &pool.queue;

    while (1) {
        pthread_mutex_lock(&q->mutex);
        while (q->front == NULL && !q->shutdown) {
            pthread_cond_wait(&q->cond, &q->mutex);
        }

        if (q->shutdown && q->front == NULL) {
            pthread_mutex_unlock(&q->mutex);
            break;
        }

        Task *task = q->front;
        q->front = q->front->next;
        if (q->front == NULL)
            q->rear = NULL;
        pthread_mutex_unlock(&q->mutex);

        task->function(task->arg);
        free(task);

        pthread_mutex_lock(&q->mutex);
        q->taskCount--;
        if (q->taskCount == 0) {
            pthread_cond_signal(&q->done);
        }
        pthread_mutex_unlock(&q->mutex);
    }
    return NULL;
}

bool init_thread_pool(struct mosquitto_opt *options, int optionsCount) {
    int numThreads = 0;
    for (int i = 0; i < optionsCount; i++) {
        if (strcmp(options[i].key, "num_threads") == 0)
            numThreads = atoi(options[i].value);
    }

    if (numThreads <= 0) {
        numThreads = (int)sysconf(_SC_NPROCESSORS_ONLN);
        if (numThreads <= 0)
            numThreads = 1;
    }

    pool.numThreads = numThreads;
    pool.threads = malloc(sizeof(pthread_t) * numThreads);
    if (!pool.threads)
        return false;

    TaskQueue *q = &pool.queue;
    q->front = q->rear = NULL;
    q->shutdown = 0;
    q->taskCount = 0;

    if (pthread_mutex_init(&q->mutex, NULL) != 0)
        return false;
    if (pthread_cond_init(&q->cond, NULL) != 0)
        return false;
    if (pthread_cond_init(&q->done, NULL) != 0)
        return false;

    for (int i = 0; i < pool.numThreads; i++) {
        if (pthread_create(&pool.threads[i], NULL, worker, NULL) != 0) {
            return false;
        }
    }

    return true;
}

void free_thread_pool(void) {
    TaskQueue *q = &pool.queue;

    pthread_mutex_lock(&q->mutex);
    q->shutdown = 1;
    pthread_cond_broadcast(&q->cond);
    while (q->taskCount > 0) {
        pthread_cond_wait(&q->done, &q->mutex);
    }
    pthread_mutex_unlock(&q->mutex);

    for (int i = 0; i < pool.numThreads; i++) {
        pthread_join(pool.threads[i], NULL);
    }

    free(pool.threads);
    pthread_mutex_destroy(&q->mutex);
    pthread_cond_destroy(&q->cond);
    pthread_cond_destroy(&q->done);
}

bool add_task(void (*function)(void *), void *arg) {
    TaskQueue *q = &pool.queue;
    Task *task = malloc(sizeof(Task));
    if (!task)
        return false;

    task->function = function;
    task->arg = arg;
    task->next = NULL;

    pthread_mutex_lock(&q->mutex);
    if (q->shutdown) {
        pthread_mutex_unlock(&q->mutex);
        free(task);
        return false; // pool closing/closed
    }

    if (q->rear == NULL) {
        q->front = q->rear = task;
    }
    else {
        q->rear->next = task;
        q->rear = task;
    }
    q->taskCount++;
    pthread_cond_signal(&q->cond);
    pthread_mutex_unlock(&q->mutex);

    return true;
}
