#include <stdio.h>
#include <stdlib.h>

#include <atomic>

#include "client.h"
#include "ycsb_test.h"

int main(int argc, char ** argv) {
    if (argc != 4) {
        printf("Usage: %s path-to-config-file workload-name num-clients\n", argv[0]);
        return 1;
    }

    WorkloadFileName * workload_fnames = get_workload_fname(argv[2]);
    int num_clients = atoi(argv[3]);

    GlobalConfig config;
    int ret = load_config(argv[1], &config);
    assert(ret == 0);

    // bind this process to main core
    // run client args
    RunClientArgs * client_args_list = (RunClientArgs *)malloc(sizeof(RunClientArgs) * num_clients);
    pthread_barrier_t global_load_barrier;
    pthread_barrier_init(&global_load_barrier, NULL, num_clients);
    pthread_barrier_t global_timer_barrier;
    pthread_barrier_init(&global_timer_barrier, NULL, num_clients);
    volatile bool should_stop = false;

    pthread_t tid_list[num_clients];
    for (int i = 0; i < num_clients; i ++) {
        client_args_list[i].client_id    = config.server_id - config.memory_num;
        client_args_list[i].thread_id    = i;
        client_args_list[i].main_core_id = config.main_core_id + i * 2;
        client_args_list[i].poll_core_id = config.poll_core_id + i * 2;
        client_args_list[i].workload_name = argv[2];
        client_args_list[i].config_file   = argv[1];
        client_args_list[i].load_barrier  = &global_load_barrier;
        client_args_list[i].should_stop   = &should_stop;
        client_args_list[i].timer_barrier = &global_timer_barrier;
        client_args_list[i].ret_num_ops = 0;
        client_args_list[i].ret_faile_num = 0;
        client_args_list[i].num_threads = num_clients;
        pthread_t tid;
        pthread_create(&tid, NULL, run_client_cont_tpt, &client_args_list[i]);
        tid_list[i] = tid;
    }

    for (int i = 0; i < num_clients; i ++) {
        pthread_join(tid_list[i], NULL);
        printf("thread %d finished\n", i);
    }
}