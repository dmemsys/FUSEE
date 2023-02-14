#ifndef DDCKV_MICRO_TEST_H_
#define DDCKV_MICRO_TEST_H_

#include <stdint.h>
#include <pthread.h>

#include "client.h"

typedef struct TagMicroRunClientArgs {
    int thread_id;
    int main_core_id;
    int poll_core_id;
    char * workload_name;
    char * config_file;
    pthread_barrier_t * insert_start_barrier;
    pthread_barrier_t * insert_finish_barrier;
    pthread_barrier_t * update_start_barrier;
    pthread_barrier_t * update_finish_barrier;
    pthread_barrier_t * search_start_barrier;
    pthread_barrier_t * search_finish_barrier;
    pthread_barrier_t * delete_start_barrier;
    pthread_barrier_t * delete_finish_barrier;
    volatile bool * should_stop;
    // bool * timer_is_ready;
    pthread_barrier_t * timer_barrier;

    uint32_t ret_num_insert_ops;
    uint32_t ret_num_update_ops;
    uint32_t ret_num_search_ops;
    uint32_t ret_num_delete_ops;
    uint32_t ret_fail_insert_num;
    uint32_t ret_fail_update_num;
    uint32_t ret_fail_search_num;
    uint32_t ret_fail_delete_num;

    uint32_t client_id;
    uint32_t num_threads;
    char * op_type;
    Client * client;
} MicroRunClientArgs;

void * run_client(void * _args);
void * run_client_cr(void * _args);

#endif