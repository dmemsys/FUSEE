#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <pthread.h>

#include "server.h"

int main(int argc, char ** argv) {
    if (argc != 2) {
        printf("Usage: %s [server_id]\n", argv[0]);
        return -1;
    }

    int32_t server_id = atoi(argv[1]);
    int32_t ret = 0;
    struct GlobalConfig server_conf;
    ret = load_config("./server_config.json", &server_conf);
    assert(ret == 0);
    server_conf.server_id = server_id;

    printf("===== Starting Server %d =====\n", server_conf.server_id);
    Server * server = new Server(&server_conf);
    ServerMainArgs server_main_args;
    server_main_args.server = server;
    server_main_args.core_id = server_conf.main_core_id;

    pthread_t server_tid;
    pthread_create(&server_tid, NULL, server_main, (void *)&server_main_args);

    printf("press to exit\n");
    // getchar();
    printf("===== Ending Server %d =====\n", server_conf.server_id);
    sleep(100000000ll);

    server->stop();
    return 0;
}