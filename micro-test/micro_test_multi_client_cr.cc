#include <stdio.h>
#include <stdlib.h>

#include <stdio.h>
#include <stdlib.h>

#include <atomic>

#include "client.h"
#include "micro_test.h"

static void start_client_threads(char * op_type, int num_clients, GlobalConfig * config, 
        char * config_fname) {
    MicroRunClientArgs * client_args_list = (MicroRunClientArgs *)malloc(sizeof(MicroRunClientArgs) * num_clients);
    pthread_barrier_t insert_start_barrier;
    pthread_barrier_t insert_finish_barrier;
    pthread_barrier_t update_start_barrier;
    pthread_barrier_t update_finish_barrier;
    pthread_barrier_t search_start_barrier;
    pthread_barrier_t search_finish_barrier;
    pthread_barrier_t delete_start_barrier;
    pthread_barrier_t delete_finish_barrier;
    pthread_barrier_t global_timer_barrier;
    pthread_barrier_init(&insert_start_barrier, NULL, num_clients);
    pthread_barrier_init(&insert_finish_barrier, NULL, num_clients);
    pthread_barrier_init(&update_start_barrier, NULL, num_clients);
    pthread_barrier_init(&update_finish_barrier, NULL, num_clients);
    pthread_barrier_init(&search_start_barrier, NULL, num_clients);
    pthread_barrier_init(&search_finish_barrier, NULL, num_clients);
    pthread_barrier_init(&delete_start_barrier, NULL, num_clients);
    pthread_barrier_init(&delete_finish_barrier, NULL, num_clients);
    pthread_barrier_init(&global_timer_barrier, NULL, num_clients);
    volatile bool should_stop = false;

    pthread_t tid_list[num_clients];
    for (int i = 0; i < num_clients; i ++) {
        client_args_list[i].client_id    = config->server_id - config->memory_num;
        client_args_list[i].thread_id    = i;
        client_args_list[i].num_threads  = num_clients;
        client_args_list[i].main_core_id = config->main_core_id + i * 2;
        client_args_list[i].poll_core_id = config->poll_core_id + i * 2;
        client_args_list[i].config_file   = config_fname;
        client_args_list[i].insert_start_barrier= &insert_start_barrier;
        client_args_list[i].insert_finish_barrier= &insert_finish_barrier;
        client_args_list[i].update_start_barrier= &update_start_barrier;
        client_args_list[i].update_finish_barrier= &update_finish_barrier;
        client_args_list[i].search_start_barrier= &search_start_barrier;
        client_args_list[i].search_finish_barrier= &search_finish_barrier;
        client_args_list[i].delete_start_barrier= &delete_start_barrier;
        client_args_list[i].delete_finish_barrier= &delete_finish_barrier;
        client_args_list[i].timer_barrier = &global_timer_barrier;
        client_args_list[i].should_stop   = &should_stop;
        client_args_list[i].ret_num_insert_ops = 0;
        client_args_list[i].ret_num_update_ops = 0;
        client_args_list[i].ret_num_search_ops = 0;
        client_args_list[i].ret_num_delete_ops = 0;
        client_args_list[i].ret_fail_insert_num = 0;
        client_args_list[i].ret_fail_update_num = 0;
        client_args_list[i].ret_fail_search_num = 0;
        client_args_list[i].ret_fail_delete_num = 0;
        client_args_list[i].op_type = op_type;
        pthread_t tid;
        pthread_create(&tid, NULL, run_client_cr, &client_args_list[i]);
        tid_list[i] = tid;
    }

    uint32_t total_insert_tpt = 0;
    uint32_t total_insert_failed = 0;
    uint32_t total_update_tpt = 0;
    uint32_t total_update_failed = 0;
    uint32_t total_search_tpt = 0;
    uint32_t total_search_failed = 0;
    uint32_t total_delete_tpt = 0;
    uint32_t total_delete_failed = 0;
    for (int i = 0; i < num_clients; i ++) {
        pthread_join(tid_list[i], NULL);
        total_insert_tpt += client_args_list[i].ret_num_insert_ops;
        total_update_tpt += client_args_list[i].ret_num_update_ops;
        total_search_tpt += client_args_list[i].ret_num_search_ops;
        total_delete_tpt += client_args_list[i].ret_num_delete_ops;
        total_insert_failed += client_args_list[i].ret_fail_insert_num;
        total_update_failed += client_args_list[i].ret_fail_update_num;
        total_search_failed += client_args_list[i].ret_fail_search_num;
        total_delete_failed += client_args_list[i].ret_fail_delete_num;
    }
    printf("insert total: %d ops\n", total_insert_tpt);
    printf("insert failed: %d ops\n", total_insert_failed);
    printf("insert tpt: %d ops/s\n", (total_insert_tpt - total_insert_failed) / config->workload_run_time);
    printf("update total: %d ops\n", total_update_tpt);
    printf("update failed: %d ops\n", total_update_failed);
    printf("update tpt: %d ops/s\n", (total_update_tpt - total_update_failed) / config->workload_run_time);
    printf("search total: %d ops\n", total_search_tpt);
    printf("search failed: %d ops\n", total_search_failed);
    printf("search tpt: %d ops/s\n", (total_search_tpt - total_search_failed) / config->workload_run_time);
    printf("delete total: %d ops\n", total_delete_tpt);
    printf("delete failed: %d ops\n", total_delete_failed);
    printf("delete tpt: %d ops/s\n", (total_delete_tpt - total_delete_failed) / config->workload_run_time);
    free(client_args_list);
}

int main(int argc, char ** argv) {
    if (argc != 3) {
        printf("Usage: %s path-to-config-file num-clients\n", argv[0]);
        return 1;
    }

    int num_clients = atoi(argv[2]);

    GlobalConfig config;
    int ret = load_config(argv[1], &config);
    assert(ret == 0);

    start_client_threads("INSERT", num_clients, &config, argv[1]);   
}