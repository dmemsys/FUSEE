#include <stdio.h>
#include <stdlib.h>

#include "client.h"
#include "ycsb_test.h"

int main(int argc, char ** argv) {
    if (argc != 4) {
        printf("Usage: %s client-id config-file workload-name\n", argv[0]);
        return 1;
    }

    WorkloadFileName * workload_fnames = get_workload_fname(argv[3]);

    int ret = 0;
    GlobalConfig config;
    ret = load_config(argv[2], &config);
    assert(ret == 0);

    // assign client id and core id
    int client_id = atoi(argv[1]);
    assert(client_id > config.num_replication);
    config.server_id = client_id;
    conf_reassign_cores(&config, client_id);

    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(config.main_core_id, &cpuset);
    ret = sched_setaffinity(0, sizeof(cpuset), &cpuset);
    assert(ret == 0);

    // check if affinity is successfully set
    CPU_ZERO(&cpuset);
    ret = sched_getaffinity(0, sizeof(cpuset), &cpuset);
    for (int i = 0; i < sysconf(_SC_NPROCESSORS_CONF); i ++) {
        if (CPU_ISSET(i, &cpuset)) {
            printf("main process running on core: %d\n", i);
        }
    }
    
    Client client(&config);
    client.load_cache();
    
    pthread_t polling_tid = client.start_polling_thread();

    ret = load_test_cnt_time(client, workload_fnames);
    assert(ret == 0);
    client.stop_polling_thread();
    pthread_join(polling_tid, NULL);
    return 0;
}