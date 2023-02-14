#ifndef DDCKV_YCSB_TEST_H_
#define DDCKV_YCSB_TEST_H_

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <sys/time.h>

#include "client.h"
#include "client_cr.h"

typedef struct TagWorkloadFileName {
    char load_fname[64];
    char trans_fname[64];
} WorkloadFileName;

typedef struct TagRunClientArgs {
    int thread_id;
    int main_core_id;
    int poll_core_id;
    char * workload_name;
    char * config_file;
    pthread_barrier_t * load_barrier;
    volatile bool * should_stop;
    // bool * timer_is_ready;
    pthread_barrier_t * timer_barrier;

    uint32_t ret_num_ops;
    uint32_t ret_faile_num;

    uint32_t client_id;
    uint32_t num_threads;
} RunClientArgs;

int is_valid_workload(char * workload_name);
WorkloadFileName * get_workload_fname(char * workload_name);
WorkloadFileName * get_workload_fname(char * workload_name, int thread_id);
int get_num_failed(ClientFiberArgs * fb_args_list, int num_coro);
uint64_t get_time_spent(ClientFiberArgs * fb_args_list, int num_coro);
bool time_is_less_than(struct timeval * t1, struct timeval * t2);

int load_workload(Client & client, WorkloadFileName * workload_fnames);
int load_workload(ClientCR & client, WorkloadFileName * workload_fnames);
int load_workload_sync(Client & client, WorkloadFileName * workload_fnames);
int load_workload_sync(ClientCR & client, WorkloadFileName * workload_fnames);
int load_workload_1coro(Client & client, WorkloadFileName * workload_fnames);
int load_workload_1coro(Client & client, WorkloadFileName * workload_fnames, int st, int ed);

int load_test_cnt_time(Client & client, WorkloadFileName * workload_fnames);
int load_test_cnt_ops(Client & client, WorkloadFileName * workload_fnames);
int load_test_cnt_ops(ClientCR & client, WorkloadFileName * workloadfnames);
int load_test_cnt_ops_mt(Client & client, WorkloadFileName * workload_fnames, RunClientArgs * args);
int load_test_cnt_ops_mt(ClientCR & client, WorkloadFileName * workload_fnames, RunClientArgs * arg);
int load_test_cnt_ops_on_crash(Client & client, WorkloadFileName * workload_fnames);
int load_test_cnt_ops_mt_on_crash_cont_sample(Client & client, WorkloadFileName * workload_fnames);

int load_test_lat_mt(Client & client, WorkloadFileName * workload_fnames, RunClientArgs * args, const char * out_fname);
int load_test_lat_mt(ClientCR & client, WorkloadFileName * workload_fnames, RunClientArgs * args, const char * out_fname);
int load_test_lat(Client & client, WorkloadFileName * get_workload_fname, const char * out_fname);
int load_test_lat(ClientCR & client, WorkloadFileName * get_workload_fname, const char * out_fname);

void conf_reassign_cores(GlobalConfig * conf, int new_client_id);

void timer_fb_func(volatile bool * should_stop, int seconds);
void timer_fb_func_ms(volatile bool * should_stop, int milliseconds);

void * run_client(void * _args);
void * run_client_cr(void * _args);
void * run_client_lat(void *_args);
void * run_client_cr_lat(void * _args);
void * run_client_cont_tpt(void * _args);
void * run_client_on_crash_cont_tpt(void * _args);

#endif