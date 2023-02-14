#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>

#include "ycsb_test.h"

#include "client.h"
#include "client_cr.h"

int is_valid_workload(char * workload_name) {
    return strcmp(workload_name, "workloada") == 0 || strcmp(workload_name, "workloadb") == 0 ||
           strcmp(workload_name, "workloadc") == 0 || strcmp(workload_name, "workloadd") == 0 ||
           strcmp(workload_name, "workloade") == 0 || strcmp(workload_name, "workloads") == 0 ||
           strcmp(workload_name, "workloadss") == 0 || strcmp(workload_name, "workloadupd0") == 0 ||
           strcmp(workload_name, "workloadupd10") == 0 || strcmp(workload_name, "workloadupd20") == 0 ||
           strcmp(workload_name, "workloadupd30") == 0 || strcmp(workload_name, "workloadupd40") == 0 ||
           strcmp(workload_name, "workloadupd50") == 0 || strcmp(workload_name, "workloadupd60") == 0 ||
           strcmp(workload_name, "workloadupd70") == 0 || strcmp(workload_name, "workloadupd80") == 0 || 
           strcmp(workload_name, "workloadupd90") == 0 || strcmp(workload_name, "workloadupd100") == 0;
}

WorkloadFileName * get_workload_fname(char * workload_name) {
    assert(is_valid_workload(workload_name));
    WorkloadFileName * workload_fname = (WorkloadFileName *)malloc(sizeof(WorkloadFileName));
    sprintf(workload_fname->load_fname, "workloads/%s.spec_load", workload_name);
    sprintf(workload_fname->trans_fname, "workloads/%s.spec_trans", workload_name);
    // sprintf(workload_fname->load_fname, "upd-workloads/%s.spec_load", workload_name);
    // sprintf(workload_fname->trans_fname, "upd-workloads/%s.spec_trans", workload_name);
    return workload_fname;
}

WorkloadFileName * get_workload_fname(char * workload_name, int thread_id) {
    assert(is_valid_workload(workload_name));
    WorkloadFileName * workload_fname = (WorkloadFileName *)malloc(sizeof(WorkloadFileName));
#ifdef YCSB_10M
    sprintf(workload_fname->load_fname, "ycsb-small/%s.load", workload_name);
    sprintf(workload_fname->trans_fname, "ycsb-small/%s.trans%d", workload_name, thread_id);
#else
    sprintf(workload_fname->load_fname, "workloads/%s.spec_load", workload_name);
    sprintf(workload_fname->trans_fname, "workloads/%s.spec_trans%d", workload_name, thread_id);
    // sprintf(workload_fname->load_fname, "upd-workloads/%s.spec_load", workload_name);
    // sprintf(workload_fname->trans_fname, "upd-workloads/%s.spec_trans%d", workload_name, thread_id);
#endif
    return workload_fname;
}

int get_num_failed(ClientFiberArgs * fb_args_list, int num_coro) {
    int sum = 0;
    for (int i = 0; i < num_coro; i ++) {
        sum += fb_args_list[i].num_failed;
    }
    return sum;
}

int load_large_workload_2_coro(Client & client, WorkloadFileName * workload_fnames) {
    int ret = 0;
    for (int i = 0; i < 10; i ++) {
        ret = client.load_kv_requests(workload_fnames->load_fname, i * 10000, 10000);
        assert(ret == 0);
        int num_coro = 1;
        bool should_stop = false;

        printf("Load phase %d start\n", i);
        ClientFiberArgs * fb_args_list = (ClientFiberArgs *)malloc(sizeof(ClientFiberArgs) * num_coro);
        uint32_t coro_num_ops = client.num_local_operations_ / num_coro;
        for (int i = 0; i < num_coro; i ++) {
            fb_args_list[i].client = &client;
            fb_args_list[i].coro_id = i;
            fb_args_list[i].ops_num = coro_num_ops;
            fb_args_list[i].ops_st_idx = coro_num_ops * i;
            fb_args_list[i].num_failed = 0;
            fb_args_list[i].st = (struct timeval *)malloc(sizeof(struct timeval));
            fb_args_list[i].et = (struct timeval *)malloc(sizeof(struct timeval));
            fb_args_list[i].should_stop = &should_stop;
        }
        boost::fibers::fiber fb_list[num_coro];
        for (int i = 0; i < num_coro; i ++) {
            boost::fibers::fiber fb(client_ops_fb_cnt_time, &fb_args_list[i]);
            fb_list[i] = std::move(fb);
        }

        for (int i = 0; i < num_coro; i ++) {
            fb_list[i].join();
        }

        int num_failed = get_num_failed(fb_args_list, num_coro);
        printf("Load phase %d ends (Failed %d)\n", i, num_failed);
        uint64_t time_spent_us = get_time_spent(fb_args_list, num_coro);
        printf("time spent: %lf ms\n", (float)time_spent_us / 1000);
        free(fb_args_list);
    }
    printf("All load phase ends\n");
    return 0;
}

int load_workload_1coro(Client & client, WorkloadFileName * workload_fnames, int st, int ed) {
    int ret = 0;
    ret = client.load_kv_requests(workload_fnames->load_fname, st, ed);
    assert(ret == 0);
    int num_coro = 1;
    bool should_stop = false;
    
    printf("Load phase start\n");
    ClientFiberArgs * fb_args_list = (ClientFiberArgs *)malloc(sizeof(ClientFiberArgs) * num_coro);
    uint32_t coro_num_ops = client.num_local_operations_ / num_coro;
    for (int i = 0; i < num_coro; i ++) {
        fb_args_list[i].client = &client;
        fb_args_list[i].coro_id = i;
        fb_args_list[i].ops_num = coro_num_ops;
        fb_args_list[i].ops_st_idx = coro_num_ops * i;
        fb_args_list[i].num_failed = 0;
        fb_args_list[i].st = (struct timeval *)malloc(sizeof(struct timeval));
        fb_args_list[i].et = (struct timeval *)malloc(sizeof(struct timeval));
        fb_args_list[i].should_stop = &should_stop;
    }
    fb_args_list[num_coro - 1].ops_num += client.num_local_operations_ % num_coro;
    fb_args_list->should_stop = &should_stop;

    boost::fibers::fiber fb_list[num_coro];
    for (int i = 0; i < num_coro; i ++) {
        boost::fibers::fiber fb(client_ops_fb_cnt_time, &fb_args_list[i]);
        fb_list[i] = std::move(fb);
    }

    for (int i = 0; i < num_coro; i ++) {
        fb_list[i].join();
    }

    int num_failed = get_num_failed(fb_args_list, num_coro);
    printf("Load phase ends (Failed %d)\n", num_failed);

    uint64_t time_spent_us = get_time_spent(fb_args_list, num_coro);
    printf("time spent: %lf ms\n", (float)time_spent_us / 1000);
    free(fb_args_list);
    return 0;
}

int load_workload_1coro(Client & client, WorkloadFileName * workload_fnames) {
    int ret = 0;
    ret = client.load_kv_requests(workload_fnames->load_fname, 0, 100000);
    assert(ret == 0);
    int num_coro = 1;
    bool should_stop = false;
    
    printf("Load phase start\n");
    ClientFiberArgs * fb_args_list = (ClientFiberArgs *)malloc(sizeof(ClientFiberArgs) * num_coro);
    uint32_t coro_num_ops = client.num_local_operations_ / num_coro;
    for (int i = 0; i < num_coro; i ++) {
        fb_args_list[i].client = &client;
        fb_args_list[i].coro_id = i;
        fb_args_list[i].ops_num = coro_num_ops;
        fb_args_list[i].ops_st_idx = coro_num_ops * i;
        fb_args_list[i].num_failed = 0;
        fb_args_list[i].st = (struct timeval *)malloc(sizeof(struct timeval));
        fb_args_list[i].et = (struct timeval *)malloc(sizeof(struct timeval));
        fb_args_list[i].should_stop = &should_stop;
    }
    fb_args_list[num_coro - 1].ops_num += client.num_local_operations_ % num_coro;
    fb_args_list->should_stop = &should_stop;

    boost::fibers::fiber fb_list[num_coro];
    for (int i = 0; i < num_coro; i ++) {
        boost::fibers::fiber fb(client_ops_fb_cnt_time, &fb_args_list[i]);
        fb_list[i] = std::move(fb);
    }

    for (int i = 0; i < num_coro; i ++) {
        fb_list[i].join();
    }

    int num_failed = get_num_failed(fb_args_list, num_coro);
    printf("Load phase ends (Failed %d)\n", num_failed);

    uint64_t time_spent_us = get_time_spent(fb_args_list, num_coro);
    printf("time spent: %lf ms\n", (float)time_spent_us / 1000);
    free(fb_args_list);
    return 0;
}

int load_workload_1coro(ClientCR & client, WorkloadFileName * workload_fnames) {
    int ret = 0;
    ret = client.load_kv_requests(workload_fnames->load_fname, 0, 100000);
    assert(ret == 0);
    int num_coro = 1;
    
    printf("Load phase start\n");
    ClientFiberArgs * fb_args_list = (ClientFiberArgs *)malloc(sizeof(ClientFiberArgs) * num_coro);
    uint32_t coro_num_ops = client.num_local_operations_ / num_coro;
    for (int i = 0; i < num_coro; i ++) {
        fb_args_list[i].client_cr = &client;
        fb_args_list[i].coro_id = i;
        fb_args_list[i].ops_num = coro_num_ops;
        fb_args_list[i].ops_st_idx = coro_num_ops * i;
        fb_args_list[i].num_failed = 0;
        fb_args_list[i].st = (struct timeval *)malloc(sizeof(struct timeval));
        fb_args_list[i].et = (struct timeval *)malloc(sizeof(struct timeval));
    }
    fb_args_list[num_coro - 1].ops_num += client.num_local_operations_ % num_coro;

    boost::fibers::fiber fb_list[num_coro];
    for (int i = 0; i < num_coro; i ++) {
        boost::fibers::fiber fb(client_cr_ops_fb_cnt_time, &fb_args_list[i]);
        fb_list[i] = std::move(fb);
    }

    for (int i = 0; i < num_coro; i ++) {
        fb_list[i].join();
    }

    int num_failed = get_num_failed(fb_args_list, num_coro);
    printf("Load phase ends (Failed %d)\n", num_failed);

    uint64_t time_spent_us = get_time_spent(fb_args_list, num_coro);
    printf("time spent: %lf ms\n", (float)time_spent_us / 1000);
    free(fb_args_list);
    return 0;
}

int load_workload(ClientCR & client, WorkloadFileName * workload_fnames) {
    int ret = 0;
    ret = client.load_kv_requests(workload_fnames->load_fname, 0, -1);
    assert(ret == 0);
    
    printf("Load phase start\n");
    ClientFiberArgs * fb_args_list = (ClientFiberArgs *)malloc(sizeof(ClientFiberArgs) * client.num_coroutines_);
    uint32_t coro_num_ops = client.num_local_operations_ / client.num_coroutines_;
    for (int i = 0; i < client.num_coroutines_; i ++) {
        fb_args_list[i].client_cr = &client;
        fb_args_list[i].coro_id = i;
        fb_args_list[i].ops_num = coro_num_ops;
        fb_args_list[i].ops_st_idx = coro_num_ops * i;
        fb_args_list[i].num_failed = 0;
        fb_args_list[i].st = (struct timeval *)malloc(sizeof(struct timeval));
        fb_args_list[i].et = (struct timeval *)malloc(sizeof(struct timeval));
    }
    fb_args_list[client.num_coroutines_ - 1].ops_num += client.num_local_operations_ % client.num_coroutines_;

    boost::fibers::fiber fb_list[client.num_coroutines_];
    for (int i = 0; i < client.num_coroutines_; i ++) {
        boost::fibers::fiber fb(client_cr_ops_fb_cnt_time, &fb_args_list[i]);
        fb_list[i] = std::move(fb);
    }

    for (int i = 0; i < client.num_coroutines_; i ++) {
        fb_list[i].join();
    }

    int num_failed = get_num_failed(fb_args_list, client.num_coroutines_);
    printf("Load phase ends (Failed %d)\n", num_failed);

    uint64_t time_spent_us = get_time_spent(fb_args_list, client.num_coroutines_);
    printf("time spent: %lf ms\n", (float)time_spent_us / 1000);
    free(fb_args_list);
    return 0;
}

int load_workload(Client & client, WorkloadFileName * workload_fnames) {
    int ret = 0;
    ret = client.load_kv_requests(workload_fnames->load_fname, 0, -1);
    assert(ret == 0);
    
    printf("Load phase start\n");
    ClientFiberArgs * fb_args_list = (ClientFiberArgs *)malloc(sizeof(ClientFiberArgs) * client.num_coroutines_);
    uint32_t coro_num_ops = client.num_local_operations_ / client.num_coroutines_;
    for (int i = 0; i < client.num_coroutines_; i ++) {
        fb_args_list[i].client = &client;
        fb_args_list[i].coro_id = i;
        fb_args_list[i].ops_num = coro_num_ops;
        fb_args_list[i].ops_st_idx = coro_num_ops * i;
        fb_args_list[i].num_failed = 0;
        fb_args_list[i].st = (struct timeval *)malloc(sizeof(struct timeval));
        fb_args_list[i].et = (struct timeval *)malloc(sizeof(struct timeval));
    }
    fb_args_list[client.num_coroutines_ - 1].ops_num += client.num_local_operations_ % client.num_coroutines_;

    boost::fibers::fiber fb_list[client.num_coroutines_];
    for (int i = 0; i < client.num_coroutines_; i ++) {
        boost::fibers::fiber fb(client_ops_fb_cnt_time, &fb_args_list[i]);
        fb_list[i] = std::move(fb);
    }

    for (int i = 0; i < client.num_coroutines_; i ++) {
        fb_list[i].join();
    }

    int num_failed = get_num_failed(fb_args_list, client.num_coroutines_);
    printf("Load phase ends (Failed %d)\n", num_failed);

    uint64_t time_spent_us = get_time_spent(fb_args_list, client.num_coroutines_);
    printf("time spent: %lf ms\n", (float)time_spent_us / 1000);
    free(fb_args_list);
    return 0;
}

int load_workload_sync(Client & client, WorkloadFileName * workload_fnames) {
    int ret = 0;
    ret = client.load_kv_requests(workload_fnames->load_fname, 0, -1);
    assert(ret == 0);
    assert(client.num_local_operations_ == 100000);

    printf("load %s\n", workload_fnames->load_fname);
    uint64_t * lat_list = (uint64_t *)malloc(sizeof(uint64_t) * client.num_local_operations_);
    memset(lat_list, 0, sizeof(uint64_t) * client.num_local_operations_);

    uint32_t num_failed = 0;
    void * search_addr;
    struct timeval st, et;
    client.init_kvreq_space(0, 0, client.num_local_operations_);

    for (int i = 0; i < client.num_local_operations_; i ++) {
        KVReqCtx * ctx = &client.kv_req_ctx_list_[i];
        ctx->coro_id = 0;

        switch (ctx->req_type) {
        case KV_REQ_SEARCH:
            gettimeofday(&st, NULL);
            search_addr = client.kv_search_sync(ctx);
            gettimeofday(&et, NULL);
            if (search_addr == NULL) {
                num_failed ++;
            }
            break;
        case KV_REQ_INSERT:
            gettimeofday(&st, NULL);
            ret = client.kv_insert_sync(ctx);
            gettimeofday(&et, NULL);
            if (ret == KV_OPS_FAIL_REDO || ret == KV_OPS_FAIL_RETURN) {
                num_failed ++;
            }
            break;
        case KV_REQ_UPDATE:
            gettimeofday(&st, NULL);
            ret = client.kv_update_sync(ctx);
            gettimeofday(&et, NULL);
            break;
        case KV_REQ_DELETE:
            gettimeofday(&st, NULL);
            ret = client.kv_delete_sync(ctx);
            gettimeofday(&et, NULL);
            break;
        default:
            assert(0);
            break;
        }

        lat_list[i] = (et.tv_sec - st.tv_sec) * 1000000 + (et.tv_usec - st.tv_usec);
    }
    printf("Failed: %d\n", num_failed);
    return 0;
}

int load_workload_sync(ClientCR & client, WorkloadFileName * workload_fnames) {
    int ret = 0;
    ret = client.load_kv_requests(workload_fnames->load_fname, 0, -1);
    assert(ret == 0);
    assert(client.num_local_operations_ == 100000);

    printf("load %s\n", workload_fnames->load_fname);
    uint64_t * lat_list = (uint64_t *)malloc(sizeof(uint64_t) * client.num_local_operations_);
    memset(lat_list, 0, sizeof(uint64_t) * client.num_local_operations_);

    uint32_t num_failed = 0;
    void * search_addr;
    struct timeval st, et;
    client.init_kvreq_space(0, 0, client.num_local_operations_);

    for (int i = 0; i < client.num_local_operations_; i ++) {
        KVReqCtx * ctx = &client.kv_req_ctx_list_[i];
        ctx->coro_id = 0;

        switch (ctx->req_type) {
        case KV_REQ_SEARCH:
            gettimeofday(&st, NULL);
            search_addr = client.kv_search_sync(ctx);
            gettimeofday(&et, NULL);
            if (search_addr == NULL) {
                num_failed ++;
            }
            break;
        case KV_REQ_INSERT:
            gettimeofday(&st, NULL);
            ret = client.kv_insert_sync(ctx);
            gettimeofday(&et, NULL);
            if (ret == KV_OPS_FAIL_REDO || ret == KV_OPS_FAIL_RETURN) {
                num_failed ++;
            }
            break;
        case KV_REQ_UPDATE:
            gettimeofday(&st, NULL);
            ret = client.kv_update_sync(ctx);
            gettimeofday(&et, NULL);
            break;
        case KV_REQ_DELETE:
            gettimeofday(&st, NULL);
            ret = client.kv_delete_sync(ctx);
            gettimeofday(&et, NULL);
            break;
        default:
            assert(0);
            break;
        }

        lat_list[i] = (et.tv_sec - st.tv_sec) * 1000000 + (et.tv_usec - st.tv_usec);
    }
    printf("Failed: %d\n", num_failed);
    return 0;
}

int load_test_cnt_ops_mt(ClientCR & client, WorkloadFileName * workload_fnames, RunClientArgs * args) {
    int ret = 0;
    ret = client.load_kv_requests(workload_fnames->trans_fname, 0, 10000);
    assert(ret == 0);

    printf("Test phase start\n");
    boost::fibers::barrier global_barrier(client.num_coroutines_ + 1);
    ClientFiberArgs * fb_args_list = (ClientFiberArgs *)malloc(sizeof(ClientFiberArgs) * client.num_coroutines_);
    uint32_t coro_num_ops = client.num_local_operations_ / client.num_coroutines_;
    for (int i = 0; i < client.num_coroutines_; i ++) {
        fb_args_list[i].client_cr = &client;
        fb_args_list[i].coro_id = i;
        fb_args_list[i].ops_num = coro_num_ops;
        fb_args_list[i].ops_st_idx = coro_num_ops * i;
        fb_args_list[i].num_failed = 0;
        fb_args_list[i].b = &global_barrier;
        fb_args_list[i].should_stop = args->should_stop;
    }
    fb_args_list[client.num_coroutines_ - 1].ops_num += client.num_local_operations_ % client.num_coroutines_;

    boost::fibers::fiber fb_list[client.num_coroutines_];
    for (int i = 0; i < client.num_coroutines_; i ++) {
        boost::fibers::fiber fb(client_cr_ops_fb_cnt_ops, &fb_args_list[i]);
        fb_list[i] = std::move(fb);
    }

    global_barrier.wait();
    boost::fibers::fiber timer_fb;
    if (args->thread_id == 0) {
        printf("%d initializes timer\n", args->thread_id);
        pthread_barrier_wait(args->timer_barrier);
        boost::fibers::fiber fb(timer_fb_func, args->should_stop, 10);
        timer_fb = std::move(fb);
    } else {
        printf("%d wait for timer\n", args->thread_id);
        pthread_barrier_wait(args->timer_barrier);
    }
    
    printf("%d passed barrier\n", args->thread_id);
    if (args->thread_id == 0) {
        timer_fb.join();
    }
    uint32_t ops_cnt = 0;
    for (int i = 0; i < client.num_coroutines_; i ++) {
        fb_list[i].join();
        ops_cnt += fb_args_list[i].ops_cnt;
    }
    printf("%d ops/s\n", ops_cnt / 10);
    args->ret_num_ops = ops_cnt;
    free(fb_args_list);
    return 0;
}

int load_test_cnt_ops_mt(Client & client, WorkloadFileName * workload_fnames, RunClientArgs * args) {
    int ret = 0;
    ret = client.load_kv_requests(workload_fnames->trans_fname, 0, 100000);
    assert(ret == 0);

    client.start_gc_fiber();
    printf("Test phase start\n");
    boost::fibers::barrier global_barrier(client.num_coroutines_ + 1);
    ClientFiberArgs * fb_args_list = (ClientFiberArgs *)malloc(sizeof(ClientFiberArgs) * client.num_coroutines_);
    uint32_t coro_num_ops = client.num_local_operations_ / client.num_coroutines_;
    for (int i = 0; i < client.num_coroutines_; i ++) {
        fb_args_list[i].client = &client;
        fb_args_list[i].coro_id = i;
        fb_args_list[i].ops_num = coro_num_ops;
        fb_args_list[i].ops_st_idx = coro_num_ops * i;
        fb_args_list[i].num_failed = 0;
        fb_args_list[i].b = &global_barrier;
        fb_args_list[i].should_stop = args->should_stop;
    }
    fb_args_list[client.num_coroutines_ - 1].ops_num += client.num_local_operations_ % client.num_coroutines_;

    boost::fibers::fiber fb_list[client.num_coroutines_];
    for (int i = 0; i < client.num_coroutines_; i ++) {
        boost::fibers::fiber fb(client_ops_fb_cnt_ops, &fb_args_list[i]);
        fb_list[i] = std::move(fb);
    }

    global_barrier.wait();
    boost::fibers::fiber timer_fb;
    if (args->thread_id == 0) {
        printf("%d initializes timer\n", args->thread_id);
        pthread_barrier_wait(args->timer_barrier);
        boost::fibers::fiber fb(timer_fb_func, args->should_stop, client.workload_run_time_);
        timer_fb = std::move(fb);
    } else {
        printf("%d wait for timer\n", args->thread_id);
        pthread_barrier_wait(args->timer_barrier);
    }
    
    printf("%d passed barrier\n", args->thread_id);
    if (args->thread_id == 0) {
        timer_fb.join();
    }
    uint32_t ops_cnt = 0;
    uint32_t num_failed = 0;
    for (int i = 0; i < client.num_coroutines_; i ++) {
        fb_list[i].join();
        ops_cnt += fb_args_list[i].ops_cnt;
        num_failed += fb_args_list[i].num_failed;
        printf("fb%d finished\n", fb_args_list[i].coro_id);
    }
    client.stop_gc_fiber();
    printf("thread: %d %d ops/s\n", args->thread_id, ops_cnt / client.workload_run_time_);
    printf("%d failed\n", num_failed);
    args->ret_num_ops = ops_cnt;
    args->ret_faile_num = num_failed;
    free(fb_args_list);
    return 0;
}

int load_test_cnt_ops(ClientCR & client, WorkloadFileName * workload_fnames) {
    int ret = 0;
    ret = client.load_kv_requests(workload_fnames->trans_fname, 0, 10000);
    assert(ret == 0);

    printf("Test phase start\n");
    boost::fibers::barrier global_barrier(client.num_coroutines_ + 1);
    volatile bool should_stop = false;
    ClientFiberArgs * fb_args_list = (ClientFiberArgs *)malloc(sizeof(ClientFiberArgs) * client.num_coroutines_);
    uint32_t coro_num_ops = client.num_local_operations_ / client.num_coroutines_;
    for (int i = 0; i < client.num_coroutines_; i ++) {
        fb_args_list[i].client_cr = &client;
        fb_args_list[i].coro_id = i;
        fb_args_list[i].ops_num = coro_num_ops;
        fb_args_list[i].ops_st_idx = coro_num_ops * i;
        fb_args_list[i].num_failed = 0;
        fb_args_list[i].b = &global_barrier;
        fb_args_list[i].should_stop = &should_stop;
    }
    fb_args_list[client.num_coroutines_ - 1].ops_num += client.num_local_operations_ % client.num_coroutines_;

    boost::fibers::fiber fb_list[client.num_coroutines_];
    for (int i = 0; i < client.num_coroutines_; i ++) {
        boost::fibers::fiber fb(client_cr_ops_fb_cnt_ops, &fb_args_list[i]);
        fb_list[i] = std::move(fb);
    }

    global_barrier.wait();
    boost::fibers::fiber timer_fb(timer_fb_func, &should_stop, 10);

    timer_fb.join();
    uint32_t ops_cnt = 0;
    for (int i = 0; i < client.num_coroutines_; i ++) {
        fb_list[i].join();
        ops_cnt += fb_args_list[i].ops_cnt;
    }
    printf("%d ops/s\n", ops_cnt / 10);
    return 0;
}

int load_test_cnt_ops(Client & client, WorkloadFileName * workload_fnames) {
    int ret = 0;
    ret = client.load_kv_requests(workload_fnames->trans_fname, 0, 100000);
    assert(ret == 0);

    printf("Test phase start\n");
    boost::fibers::barrier global_barrier(client.num_coroutines_ + 1);
    volatile bool should_stop = false;
    ClientFiberArgs * fb_args_list = (ClientFiberArgs *)malloc(sizeof(ClientFiberArgs) * client.num_coroutines_);
    uint32_t coro_num_ops = client.num_local_operations_ / client.num_coroutines_;
    for (int i = 0; i < client.num_coroutines_; i ++) {
        fb_args_list[i].client = &client;
        fb_args_list[i].coro_id = i;
        fb_args_list[i].ops_num = coro_num_ops;
        fb_args_list[i].ops_st_idx = coro_num_ops * i;
        fb_args_list[i].num_failed = 0;
        fb_args_list[i].b = &global_barrier;
        fb_args_list[i].should_stop = &should_stop;
        fb_args_list[i].ops_cnt = 0;
    }
    fb_args_list[client.num_coroutines_ - 1].ops_num += client.num_local_operations_ % client.num_coroutines_;

    boost::fibers::fiber fb_list[client.num_coroutines_];
    for (int i = 0; i < client.num_coroutines_; i ++) {
        boost::fibers::fiber fb(client_ops_fb_cnt_ops, &fb_args_list[i]);
        fb_list[i] = std::move(fb);
    }

    global_barrier.wait();
    boost::fibers::fiber timer_fb(timer_fb_func, &should_stop, 10);

    timer_fb.join();
    uint32_t ops_cnt = 0;
    for (int i = 0; i < client.num_coroutines_; i ++) {
        fb_list[i].join();
        ops_cnt += fb_args_list[i].ops_cnt;
    }
    printf("%d ops/s\n", ops_cnt / 10);
    return 0;
}

int load_test_cnt_ops_on_crash(Client & client, WorkloadFileName * workload_fnames) {
    int ret = 0;
    ret = client.load_kv_requests(workload_fnames->trans_fname, 0, 100000);
    assert(ret == 0);

    printf("Test phase start\n");
    boost::fibers::barrier global_barrier(client.num_coroutines_ + 1);
    volatile bool should_stop = false;
    ClientFiberArgs * fb_args_list = (ClientFiberArgs *)malloc(sizeof(ClientFiberArgs) * client.num_coroutines_);
    uint32_t coro_num_ops = client.num_local_operations_ / client.num_coroutines_;
    for (int i = 0; i < client.num_coroutines_; i ++) {
        fb_args_list[i].client = &client;
        fb_args_list[i].coro_id = i;
        fb_args_list[i].ops_num = coro_num_ops;
        fb_args_list[i].ops_st_idx = coro_num_ops * i;
        fb_args_list[i].num_failed = 0;
        fb_args_list[i].b = &global_barrier;
        fb_args_list[i].should_stop = &should_stop;
        fb_args_list[i].ops_cnt = 0;
        fb_args_list[i].num_failed = 0;
    }
    fb_args_list[client.num_coroutines_ - 1].ops_num += client.num_local_operations_ % client.num_coroutines_;

    boost::fibers::fiber fb_list[client.num_coroutines_];
    for (int i = 0; i < client.num_coroutines_; i ++) {
        boost::fibers::fiber fb(client_ops_fb_cnt_ops_on_crash, &fb_args_list[i]);
        fb_list[i] = std::move(fb);
    }

    global_barrier.wait();
    boost::fibers::fiber timer_fb(timer_fb_func, &should_stop, 10);

    timer_fb.join();
    uint32_t ops_cnt = 0;
    uint32_t num_failed = 0;
    for (int i = 0; i < client.num_coroutines_; i ++) {
        fb_list[i].join();
        ops_cnt += fb_args_list[i].ops_cnt;
        num_failed += fb_args_list[i].num_failed;
    }
    printf("%d ops/s\n", ops_cnt / 10);
    printf("failed: %d\n", num_failed);
    return 0;
}

int load_test_cnt_ops_cont_sample(Client & client, WorkloadFileName * workload_fnames) {
    int ret = 0;
    ret = client.load_kv_requests(workload_fnames->trans_fname, 0, 100000);
    assert(ret == 0);

    printf("Test phase start\n");
    boost::fibers::barrier global_barrier(client.num_coroutines_ + 1);
    volatile bool should_stop = false;
    ClientFiberArgs * fb_args_list = (ClientFiberArgs *)malloc(sizeof(ClientFiberArgs) * client.num_coroutines_);
    uint32_t coro_num_ops = client.num_local_operations_ / client.num_coroutines_;
    for (int i = 0; i < client.num_coroutines_; i ++) {
        fb_args_list[i].client = &client;
        fb_args_list[i].coro_id = i;
        fb_args_list[i].ops_num = coro_num_ops;
        fb_args_list[i].ops_st_idx = coro_num_ops * i;
        fb_args_list[i].num_failed = 0;
        fb_args_list[i].b = &global_barrier;
        fb_args_list[i].should_stop = &should_stop;
        fb_args_list[i].ops_cnt = 0;
    }
    fb_args_list[client.num_coroutines_ - 1].ops_num += client.num_local_operations_ % client.num_coroutines_;

    uint32_t ops_num_vec[20];
    uint32_t failed_num_vec[20];
    int sleep_ms = (int)((float)client.workload_run_time_ / 20. * 1000);
    for (int i = 0; i < 20; i ++) {
        should_stop = false;
        boost::fibers::fiber fb_list[client.num_coroutines_];
        for (int i = 0; i < client.num_coroutines_; i ++) {
            boost::fibers::fiber fb(client_ops_fb_cnt_ops_cont, &fb_args_list[i]);
            fb_list[i] = std::move(fb);
        }

        global_barrier.wait();
        boost::fibers::fiber timer_fb(timer_fb_func_ms, &should_stop, sleep_ms);

        timer_fb.join();
        uint32_t ops_cnt = 0;
        uint32_t num_failed = 0;
        for (int i = 0; i < client.num_coroutines_; i ++) {
            fb_list[i].join();
            ops_cnt += fb_args_list[i].ops_cnt;
            num_failed += fb_args_list[i].num_failed;
        }
        ops_num_vec[i] = ops_cnt;
        failed_num_vec[i] = num_failed;
    }

    char out_fname[128];
    int my_server_id = client.get_my_server_id();
    sprintf(out_fname, "results/%d-tpt-vec.txt", my_server_id);
    FILE * of = fopen(out_fname, "w");
    for (int i = 0; i < 20; i ++) {
        fprintf(of, "%d %d\n", ops_num_vec[i], failed_num_vec[i]);
    }
    fclose(of);
    return 0;
}

int load_test_cnt_ops_mt_on_crash_cont_sample(Client & client, WorkloadFileName * workload_fnames) {
    int ret = 0;
    ret = client.load_kv_requests(workload_fnames->trans_fname, 0, 100000);
    assert(ret == 0);

    client.start_gc_fiber();
    printf("Test phase start\n");
    boost::fibers::barrier global_barrier(client.num_coroutines_ + 1);
    volatile bool should_stop = false;
    ClientFiberArgs * fb_args_list = (ClientFiberArgs *)malloc(sizeof(ClientFiberArgs) * client.num_coroutines_);
    uint32_t coro_num_ops = client.num_local_operations_ / client.num_coroutines_;
    for (int i = 0; i < client.num_coroutines_; i ++) {
        fb_args_list[i].client = &client;
        fb_args_list[i].coro_id = i;
        fb_args_list[i].ops_num = coro_num_ops;
        fb_args_list[i].ops_st_idx = coro_num_ops * i;
        fb_args_list[i].num_failed = 0;
        fb_args_list[i].b = &global_barrier;
        fb_args_list[i].should_stop = &should_stop;
        fb_args_list[i].ops_cnt = 0;
        fb_args_list[i].num_failed = 0;
    }
    fb_args_list[client.num_coroutines_ - 1].ops_num += client.num_local_operations_ % client.num_coroutines_;

    uint32_t ops_num_vec[20];
    uint32_t failed_num_vec[20];
    for (int k = 0; k < 20; k ++) {
        int sleep_ms = (int)((float)client.workload_run_time_ / 20. * 1000);
        struct timeval st, et;
        should_stop = false;
        if (k == 10) {
            // crash after 5 seconds
            int num_memory = client.get_num_memory();
            std::vector<uint8_t> crash_server_list;
            crash_server_list.push_back(1);
            client.crash_server(crash_server_list);
        }
        boost::fibers::fiber fb_list[client.num_coroutines_];
        for (int i = 0; i < client.num_coroutines_; i ++) {
            if (k < 10) {
                boost::fibers::fiber fb(client_ops_fb_cnt_ops_cont, &fb_args_list[i]);
                fb_list[i] = std::move(fb);
            } else {
                boost::fibers::fiber fb(client_ops_fb_cnt_ops_on_crash, &fb_args_list[i]);
                fb_list[i] = std::move(fb);
            }
        }

        // printf("sleep %d\n", sleep_ms);
        global_barrier.wait();
        boost::fibers::fiber timer_fb(timer_fb_func_ms, &should_stop, sleep_ms);

        timer_fb.join();
        uint32_t ops_cnt = 0;
        uint32_t num_failed = 0;
        for (int i = 0; i < client.num_coroutines_; i ++) {
            fb_list[i].join();
            ops_cnt += fb_args_list[i].ops_cnt;
            num_failed += fb_args_list[i].num_failed;
        }
        ops_num_vec[k] = ops_cnt;
        failed_num_vec[k] = num_failed;
    }

    client.stop_gc_fiber();

    char out_fname[128];
    int my_server_id = client.get_my_server_id();
    sprintf(out_fname, "results/%d-tpt-vec-on-crash.txt", my_server_id);
    FILE * of = fopen(out_fname, "w");
    for (int i = 0; i < 20; i ++) {
        fprintf(of, "%d %d\n", ops_num_vec[i], failed_num_vec[i]);
    }
    fclose(of);
    return 0;
}

int load_test_cnt_time(Client & client, WorkloadFileName * workload_fnames) {
    int ret = 0;
    ret = client.load_kv_requests(workload_fnames->trans_fname, 0, 100000);
    assert(ret == 0);

    printf("Test phase start\n");
    ClientFiberArgs * fb_args_list = (ClientFiberArgs *)malloc(sizeof(ClientFiberArgs) * client.num_coroutines_);
    uint32_t coro_num_ops = client.num_local_operations_ / client.num_coroutines_;
    for (int i = 0; i < client.num_coroutines_; i ++) {
        fb_args_list[i].client = &client;
        fb_args_list[i].coro_id = i;
        fb_args_list[i].ops_num = coro_num_ops;
        fb_args_list[i].ops_st_idx = coro_num_ops * i;
        fb_args_list[i].num_failed = 0;
        fb_args_list[i].st = (struct timeval *)malloc(sizeof(struct timeval));
        fb_args_list[i].et = (struct timeval *)malloc(sizeof(struct timeval));
    }
    fb_args_list[client.num_coroutines_ - 1].ops_num += client.num_local_operations_ % client.num_coroutines_;

    boost::fibers::fiber fb_list[client.num_coroutines_];
    for (int i = 0; i < client.num_coroutines_; i ++) {
        boost::fibers::fiber fb(client_ops_fb_cnt_time, &fb_args_list[i]);
        fb_list[i] = std::move(fb);
    }

    for (int i = 0; i < client.num_coroutines_; i ++) {
        fb_list[i].join();
    }

    int num_failed = get_num_failed(fb_args_list, client.num_coroutines_);
    printf("Test phase ends (Failed %d)\n", num_failed);

    uint64_t time_spent_us = get_time_spent(fb_args_list, client.num_coroutines_);
    printf("time spent: %lf ms\n", (float)time_spent_us / 1000);
    free(fb_args_list);
    return 0;
}

bool time_is_less_than(struct timeval * t1, struct timeval * t2) {
    // check seconds
    if (t1->tv_sec < t2->tv_sec) {
        return true;
    } else if (t1->tv_sec > t2->tv_sec) {
        return false;
    }

    // equal second check usec
    if (t1->tv_usec < t2->tv_usec) {
        return true;
    } else if (t1->tv_usec > t2->tv_usec) {
        return false;
    }

    return true;
}

uint64_t get_time_spent(ClientFiberArgs * fb_args_list, int num_coro) {
    struct timeval * st = fb_args_list[0].st;
    struct timeval * et = fb_args_list[0].et;

    for (int i = 0; i < num_coro; i ++) {
        if (time_is_less_than(st, fb_args_list[i].st) == false) {
            st = fb_args_list[i].st;
        }
        if (time_is_less_than(et, fb_args_list[i].et)) {
            et = fb_args_list[i].et;
        }
    }

    return (et->tv_sec - st->tv_sec) * 1000000 + (et->tv_usec - st->tv_usec);
}

void conf_reassign_cores(GlobalConfig * conf, int new_client_id) {
    int core_off = new_client_id - conf->num_replication - 1;
    conf->main_core_id += core_off * 2;
    conf->poll_core_id += core_off * 2;
    printf("plan to run main thread on core: %d\npoll thread on core: %d\n", conf->main_core_id, conf->poll_core_id);
}

void timer_fb_func(volatile bool * should_stop, int seconds) {
    boost::this_fiber::sleep_for(std::chrono::seconds(seconds));
    *should_stop = true;
    // printf("stopped!\n");
}

void timer_fb_func_ms(volatile bool * should_stop, int milliseconds) {
    boost::this_fiber::sleep_for(std::chrono::milliseconds(milliseconds));
    *should_stop = true;
    // printf("stopped!\n");
}

void * run_client_lat(void * _args) {
    RunClientArgs * args = (RunClientArgs *)_args;
    WorkloadFileName * workload_fnames = get_workload_fname(args->workload_name, args->thread_id);

    int ret = 0;
    GlobalConfig config;
    ret = load_config(args->config_file, &config);
    assert(ret == 0);

    // modify config to config
    config.main_core_id = args->main_core_id;
    config.poll_core_id = args->poll_core_id;
    // config.main_core_id = 0;
    // config.poll_core_id = 1;
    config.server_id    += args->thread_id;

    // bind this process to main core
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(config.main_core_id, &cpuset);
    pthread_t this_tid = pthread_self();
    ret = pthread_setaffinity_np(this_tid, sizeof(cpuset), &cpuset);
    assert(ret == 0);
    ret = pthread_getaffinity_np(this_tid, sizeof(cpuset), &cpuset);
    for (int i = 0; i < sysconf(_SC_NPROCESSORS_CONF); i ++) {
        if (CPU_ISSET(i, &cpuset)) {
            printf("client %d main process running on core: %d\n", args->thread_id, i);
        }
    }

    Client client(&config);

    pthread_t polling_tid = client.start_polling_thread();

    if (args->thread_id == 0 && config.server_id - config.memory_num == 0) {
        printf("%d loading workload\n", args->thread_id);
        ret = load_workload_1coro(client, workload_fnames);
        assert(ret == 0);
        getchar();  // use an input to synchronize
        pthread_barrier_wait(args->load_barrier);
    } else if (args->thread_id == 0) {
        assert(config.server_id - config.memory_num != 0);
        getchar();  // use an input to synchronize
        pthread_barrier_wait(args->load_barrier);
    } else {
        printf("%d waiting workload\n", args->thread_id);
        pthread_barrier_wait(args->load_barrier);
    }

    char name_buf[256];
    sprintf(name_buf, "results/lat_%d.txt", args->thread_id);
    printf("%d passed load barrier\n", args->thread_id);
    ret = load_test_lat_mt(client, workload_fnames, args, name_buf);
    assert(ret == 0);

    client.stop_polling_thread();
    pthread_join(polling_tid, NULL);
    return 0;
}

void * run_client_cr_lat(void * _args) {
    RunClientArgs * args = (RunClientArgs *)_args;
    WorkloadFileName * workload_fnames = get_workload_fname(args->workload_name, args->thread_id);

    int ret = 0;
    GlobalConfig config;
    ret = load_config(args->config_file, &config);
    assert(ret == 0);

    // modify config to config
    // config.main_core_id = args->main_core_id;
    // config.poll_core_id = args->poll_core_id;
    config.main_core_id = 0;
    config.poll_core_id = 1;
    config.server_id    += args->thread_id;

    // bind this process to main core
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(config.main_core_id, &cpuset);
    pthread_t this_tid = pthread_self();
    ret = pthread_setaffinity_np(this_tid, sizeof(cpuset), &cpuset);
    assert(ret == 0);
    ret = pthread_getaffinity_np(this_tid, sizeof(cpuset), &cpuset);
    for (int i = 0; i < sysconf(_SC_NPROCESSORS_CONF); i ++) {
        if (CPU_ISSET(i, &cpuset)) {
            printf("client %d main process running on core: %d\n", args->thread_id, i);
        }
    }

    ClientCR client(&config);

    if (args->thread_id == 0 && config.server_id - config.memory_num == 0) {
        printf("%d loading workload\n", args->thread_id);
        pthread_t polling_tid = client.start_polling_thread();
        ret = load_workload_1coro(client, workload_fnames);
        assert(ret == 0);
        client.stop_polling_thread();
        pthread_join(polling_tid, NULL);
        getchar();  // use an input to synchronize
        pthread_barrier_wait(args->load_barrier);
    } else if (args->thread_id == 0) {
        assert(config.server_id - config.memory_num != 0);
        getchar();  // use an input to synchronize
        pthread_barrier_wait(args->load_barrier);
    } else {
        printf("%d waiting workload\n", args->thread_id);
        pthread_barrier_wait(args->load_barrier);
    }

    char name_buf[256];
    sprintf(name_buf, "results/lat_cr_%d.txt", args->thread_id);
    printf("%d passed load barrier\n", args->thread_id);
    ret = load_test_lat_mt(client, workload_fnames, args, name_buf);
    assert(ret == 0);

    return 0;
}

void * run_client(void * _args) {
    RunClientArgs * args = (RunClientArgs *)_args;
    WorkloadFileName * workload_fnames = get_workload_fname(args->workload_name, args->thread_id + args->client_id);

    int ret = 0;
    GlobalConfig config;
    ret = load_config(args->config_file, &config);
    assert(ret == 0);

    // modify config to config
    config.main_core_id = args->main_core_id;
    config.poll_core_id = args->poll_core_id;
    config.server_id    += args->thread_id;

    // bind this process to main core
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(config.main_core_id, &cpuset);
    pthread_t this_tid = pthread_self();
    ret = pthread_setaffinity_np(this_tid, sizeof(cpuset), &cpuset);
    assert(ret == 0);
    ret = pthread_getaffinity_np(this_tid, sizeof(cpuset), &cpuset);
    for (int i = 0; i < sysconf(_SC_NPROCESSORS_CONF); i ++) {
        if (CPU_ISSET(i, &cpuset)) {
            printf("client %d main process running on core: %d\n", args->thread_id, i);
        }
    }

    Client client(&config);
    printf("Client %d start\n", args->thread_id);

    pthread_t polling_tid = client.start_polling_thread();

    // if (args->thread_id < 2 && config.server_id - config.memory_num < 80) {
#ifdef YCSB_10M
    if (args->thread_id < 2 && config.server_id - config.memory_num < 2) {
#else
    if (args->thread_id == 0 && config.server_id - config.memory_num == 0) {
#endif
        // for (int i = 0 ; i < 20; i ++) {
        //     printf("%d\n", i);
        //     ret = load_workload_1coro(client, workload_fnames, i * 500000, 500000);
        // }
        int i = config.server_id - config.memory_num;
#ifdef YCSB_10M
        int load_id = i / 8 + args->thread_id;
        printf("%d %d loading workload\n", args->thread_id, i);
        ret = load_workload_1coro(client, workload_fnames, load_id * 500000, 500000);
#else
        ret = load_workload_1coro(client, workload_fnames, 0, -1);
#endif
        // ret = load_large_workload_2_coro(client, workload_fnames);
        // ret = load_large_workload(client, workload_fnames);
        assert(ret == 0);
        if (i == 0)
            getchar();  // use an input to synchronize
        pthread_barrier_wait(args->load_barrier);
    } else if (args->thread_id == 0) {
        assert(config.server_id - config.memory_num != 0);
        printf("%d press to start!\n");
        getchar();  // use an input to synchronize
        pthread_barrier_wait(args->load_barrier);
    } else {
        printf("%d waiting workload\n", args->thread_id);
        pthread_barrier_wait(args->load_barrier);
    }

    printf("%d passed load barrier\n", args->thread_id);
    // ret = load_test_cnt_time(client, workload_fnames);
    ret = load_test_cnt_ops_mt(client, workload_fnames, args);
    assert(ret == 0);

    client.stop_polling_thread();
    pthread_join(polling_tid, NULL);
    return 0;
}

void * run_client_cont_tpt(void * _args) {
    RunClientArgs * args = (RunClientArgs *)_args;
    WorkloadFileName * workload_fnames = get_workload_fname(args->workload_name, args->thread_id + args->client_id);

    int ret = 0;
    GlobalConfig config;
    ret = load_config(args->config_file, &config);
    assert(ret == 0);

    // modify config to config
    config.main_core_id = args->main_core_id;
    config.poll_core_id = args->poll_core_id;
    // config.main_core_id = 0;
    // config.poll_core_id = 1;
    config.server_id    += args->thread_id;

    // bind this process to main core
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(config.main_core_id, &cpuset);
    pthread_t this_tid = pthread_self();
    ret = pthread_setaffinity_np(this_tid, sizeof(cpuset), &cpuset);
    assert(ret == 0);
    ret = pthread_getaffinity_np(this_tid, sizeof(cpuset), &cpuset);
    for (int i = 0; i < sysconf(_SC_NPROCESSORS_CONF); i ++) {
        if (CPU_ISSET(i, &cpuset)) {
            printf("client %d main process running on core: %d\n", args->thread_id, i);
        }
    }

    Client client(&config);

    pthread_t polling_tid = client.start_polling_thread();

    if (args->thread_id == 0 && config.server_id - config.memory_num == 0) {
        printf("%d loading workload\n", args->thread_id);
        ret = load_workload_1coro(client, workload_fnames);
        assert(ret == 0);
        getchar();  // use an input to synchronize
        pthread_barrier_wait(args->load_barrier);
    } else if (args->thread_id == 0) {
        assert(config.server_id - config.memory_num != 0);
        getchar();  // use an input to synchronize
        if (client.get_my_server_id() >= 18) {
            // sleep for 5 seconds if the client id is greater than 18
            sleep(5);
        }
        pthread_barrier_wait(args->load_barrier);
    } else {
        printf("%d waiting workload\n", args->thread_id);
        pthread_barrier_wait(args->load_barrier);
    }

    printf("%d passed load barrier\n", args->thread_id);
    // ret = load_test_cnt_time(client, workload_fnames);
    ret = load_test_cnt_ops_cont_sample(client, workload_fnames);
    assert(ret == 0);

    client.stop_polling_thread();
    pthread_join(polling_tid, NULL);
    return 0;
}

void * run_client_on_crash_cont_tpt(void * _args) {
    RunClientArgs * args = (RunClientArgs *)_args;
    WorkloadFileName * workload_fnames = get_workload_fname(args->workload_name, args->thread_id + args->client_id);

    int ret = 0;
    GlobalConfig config;
    ret = load_config(args->config_file, &config);
    assert(ret == 0);

    // modify config to config
    config.main_core_id = args->main_core_id;
    config.poll_core_id = args->poll_core_id;
    // config.main_core_id = 0;
    // config.poll_core_id = 1;
    config.server_id    += args->thread_id;

    // bind this process to main core
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(config.main_core_id, &cpuset);
    pthread_t this_tid = pthread_self();
    ret = pthread_setaffinity_np(this_tid, sizeof(cpuset), &cpuset);
    assert(ret == 0);
    ret = pthread_getaffinity_np(this_tid, sizeof(cpuset), &cpuset);
    for (int i = 0; i < sysconf(_SC_NPROCESSORS_CONF); i ++) {
        if (CPU_ISSET(i, &cpuset)) {
            printf("client %d main process running on core: %d\n", args->thread_id, i);
        }
    }

    Client client(&config);

    pthread_t polling_tid = client.start_polling_thread();

    if (args->thread_id == 0 && config.server_id - config.memory_num == 0) {
        printf("%d loading workload\n", args->thread_id);
        ret = load_workload_1coro(client, workload_fnames);
        assert(ret == 0);
        getchar();  // use an input to synchronize
        pthread_barrier_wait(args->load_barrier);
    } else if (args->thread_id == 0) {
        assert(config.server_id - config.memory_num != 0);
        getchar();  // use an input to synchronize
        pthread_barrier_wait(args->load_barrier);
    } else {
        printf("%d waiting workload\n", args->thread_id);
        pthread_barrier_wait(args->load_barrier);
    }

    printf("%d passed load barrier\n", args->thread_id);
    // ret = load_test_cnt_time(client, workload_fnames);
    ret = load_test_cnt_ops_mt_on_crash_cont_sample(client, workload_fnames);
    assert(ret == 0);

    client.stop_polling_thread();
    pthread_join(polling_tid, NULL);
    return 0;
}

void * run_client_cr(void * _args) {
    RunClientArgs * args = (RunClientArgs *)_args;
    WorkloadFileName * workload_fnames = get_workload_fname(args->workload_name, args->thread_id);

    int ret = 0;
    GlobalConfig config;
    ret = load_config(args->config_file, &config);
    assert(ret == 0);

    // modify config to config
    // config.main_core_id = args->main_core_id;
    // config.poll_core_id = args->poll_core_id;
    config.main_core_id = 0;
    config.poll_core_id = 1;
    config.server_id    += args->thread_id;

    // bind this process to main core
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(config.main_core_id, &cpuset);
    pthread_t this_tid = pthread_self();
    ret = pthread_setaffinity_np(this_tid, sizeof(cpuset), &cpuset);
    assert(ret == 0);
    ret = pthread_getaffinity_np(this_tid, sizeof(cpuset), &cpuset);
    for (int i = 0; i < sysconf(_SC_NPROCESSORS_CONF); i ++) {
        if (CPU_ISSET(i, &cpuset)) {
            printf("client %d main process running on core: %d\n", args->thread_id, i);
        }
    }

    ClientCR client(&config);

    pthread_t polling_tid = client.start_polling_thread();

    if (args->thread_id == 0 && config.server_id - config.memory_num == 0) {
        printf("%d loading workload\n", args->thread_id);
        ret = load_workload_1coro(client, workload_fnames);
        assert(ret == 0);
        getchar();  // use an input to synchronize
        pthread_barrier_wait(args->load_barrier);
    } else if (args->thread_id == 0) {
        assert(config.server_id - config.memory_num != 0);
        getchar();  // use an input to synchronize
        pthread_barrier_wait(args->load_barrier);
    } else {
        printf("%d waiting workload\n", args->thread_id);
        pthread_barrier_wait(args->load_barrier);
    }

    printf("%d passed load barrier\n", args->thread_id);
    // ret = load_test_cnt_time(client, workload_fnames);
    ret = load_test_cnt_ops_mt(client, workload_fnames, args);
    assert(ret == 0);

    client.stop_polling_thread();
    pthread_join(polling_tid, NULL);
    return 0;
}

int load_test_lat_mt(Client & client, WorkloadFileName * workload_fname, 
        RunClientArgs * args, const char * out_fname) {
    int ret = 0;
    ret = client.load_kv_requests(workload_fname->trans_fname, 0, 100000);
    assert(ret == 0);

    printf("Test phase start\n");
    boost::fibers::barrier global_barrier(client.num_coroutines_ + 1);
    ClientFiberArgs * fb_args_list = (ClientFiberArgs *)malloc(sizeof(ClientFiberArgs) * client.num_coroutines_);
    uint32_t coro_num_ops = client.num_local_operations_ / client.num_coroutines_;
    for (int i = 0; i < client.num_coroutines_; i ++) {
        fb_args_list[i].client = &client;
        fb_args_list[i].coro_id = i;
        fb_args_list[i].ops_num = coro_num_ops;
        fb_args_list[i].ops_st_idx = coro_num_ops * i;
        fb_args_list[i].num_failed = 0;
        fb_args_list[i].b = &global_barrier;
        fb_args_list[i].should_stop = args->should_stop;
        fb_args_list[i].thread_id = args->thread_id;
    }
    fb_args_list[client.num_coroutines_ - 1].ops_num += client.num_local_operations_ % client.num_coroutines_;

    boost::fibers::fiber fb_list[client.num_coroutines_];
    for (int i = 0; i < client.num_coroutines_; i ++) {
        boost::fibers::fiber fb(client_ops_fb_lat, &fb_args_list[i]);
        fb_list[i] = std::move(fb);
    }

    global_barrier.wait();
    boost::fibers::fiber timer_fb;
    if (args->thread_id == 0) {
        printf("%d initializes timer\n", args->thread_id);
        pthread_barrier_wait(args->timer_barrier);
        boost::fibers::fiber fb(timer_fb_func, args->should_stop, client.workload_run_time_);
        timer_fb = std::move(fb);
    } else {
        printf("%d wait for timer\n", args->thread_id);
        pthread_barrier_wait(args->timer_barrier);
    }
    
    printf("%d passed barrier\n", args->thread_id);
    if (args->thread_id == 0) {
        timer_fb.join();
    }
    uint32_t ops_cnt = 0;
    uint32_t num_failed = 0;
    for (int i = 0; i < client.num_coroutines_; i ++) {
        fb_list[i].join();
        ops_cnt += fb_args_list[i].ops_cnt;
        num_failed += fb_args_list[i].num_failed;
    }
    printf("%d failed\n", num_failed);
    args->ret_num_ops = ops_cnt;
    args->ret_faile_num = num_failed;
    free(fb_args_list);
    return 0;
}

int load_test_lat_mt(ClientCR & client, WorkloadFileName * workload_fname, 
        RunClientArgs * args, const char * out_fname) {
    int ret = 0;
    ret = client.load_kv_requests(workload_fname->trans_fname, 0, 1000);
    assert(ret == 0);
    // assert(client.num_local_operations_ == 1000);

    printf("lat test %s\n", workload_fname->trans_fname);
    uint64_t * lat_list = (uint64_t *)malloc(sizeof(uint64_t) * client.num_local_operations_);
    memset(lat_list, 0, sizeof(uint64_t) * client.num_local_operations_);

    uint32_t num_failed = 0;
    void * search_addr;
    struct timeval st, et;
    client.init_kvreq_space(0, 0, client.num_local_operations_);
    pthread_barrier_wait(args->timer_barrier);

    printf("%d passed timer barrier\n", args->thread_id);
    for (int i = 0; i < client.num_local_operations_; i ++) {
        KVReqCtx * ctx = &client.kv_req_ctx_list_[i];
        ctx->coro_id = 0;

        switch (ctx->req_type) {
        case KV_REQ_SEARCH:
            gettimeofday(&st, NULL);
            search_addr = client.kv_search_sync(ctx);
            gettimeofday(&et, NULL);
            if (search_addr == NULL) {
                num_failed ++;
            }
            break;
        case KV_REQ_INSERT:
            gettimeofday(&st, NULL);
            ret = client.kv_insert_sync(ctx);
            gettimeofday(&et, NULL);
            if (ret == KV_OPS_FAIL_REDO || ret == KV_OPS_FAIL_RETURN) {
                num_failed ++;
            }
            break;
        case KV_REQ_UPDATE:
            gettimeofday(&st, NULL);
            ret = client.kv_update_sync(ctx);
            gettimeofday(&et, NULL);
            break;
        case KV_REQ_DELETE:
            gettimeofday(&st, NULL);
            ret = client.kv_delete_sync(ctx);
            gettimeofday(&et, NULL);
            break;
        default:
            assert(0);
            break;
        }

        lat_list[i] = (et.tv_sec - st.tv_sec) * 1000000 + (et.tv_usec - st.tv_usec);
    }
    printf("%d finished Failed: %d\n", args->thread_id, num_failed);
    
    FILE * lat_fp = fopen(out_fname, "w");
    assert(lat_fp != NULL);
    for (int i = 0; i < client.num_local_operations_; i ++) {
        fprintf(lat_fp, "%ld\n", lat_list[i]);
    }
    fclose(lat_fp);
    return 0;
}

int load_test_lat(ClientCR & client, WorkloadFileName * workload_fname, const char * out_fname) {
    int ret = 0;
    ret = client.load_kv_requests(workload_fname->trans_fname, 0, 1000);
    assert(ret == 0);
    // assert(client.num_local_operations_ == 1000);

    printf("lat test %s\n", workload_fname->trans_fname);
    uint64_t * lat_list = (uint64_t *)malloc(sizeof(uint64_t) * client.num_local_operations_);
    memset(lat_list, 0, sizeof(uint64_t) * client.num_local_operations_);

    uint32_t num_failed = 0;
    void * search_addr;
    struct timeval st, et;
    client.init_kvreq_space(0, 0, client.num_local_operations_);
    for (int i = 0; i < client.num_local_operations_; i ++) {
        KVReqCtx * ctx = &client.kv_req_ctx_list_[i];
        ctx->coro_id = 0;

        switch (ctx->req_type) {
        case KV_REQ_SEARCH:
            gettimeofday(&st, NULL);
            search_addr = client.kv_search_sync(ctx);
            gettimeofday(&et, NULL);
            if (search_addr == NULL) {
                num_failed ++;
            }
            break;
        case KV_REQ_INSERT:
            gettimeofday(&st, NULL);
            ret = client.kv_insert_sync(ctx);
            gettimeofday(&et, NULL);
            if (ret == KV_OPS_FAIL_REDO || ret == KV_OPS_FAIL_RETURN) {
                num_failed ++;
            }
            break;
        case KV_REQ_UPDATE:
            gettimeofday(&st, NULL);
            ret = client.kv_update_sync(ctx);
            gettimeofday(&et, NULL);
            break;
        case KV_REQ_DELETE:
            gettimeofday(&st, NULL);
            ret = client.kv_delete_sync(ctx);
            gettimeofday(&et, NULL);
            break;
        default:
            assert(0);
            break;
        }

        lat_list[i] = (et.tv_sec - st.tv_sec) * 1000000 + (et.tv_usec - st.tv_usec);
    }
    printf("Failed: %d\n", num_failed);
    
    FILE * lat_fp = fopen(out_fname, "w");
    assert(lat_fp != NULL);
    for (int i = 0; i < client.num_local_operations_; i ++) {
        fprintf(lat_fp, "%ld\n", lat_list[i]);
    }
    fclose(lat_fp);
    return 0;
}

int load_test_lat(Client & client, WorkloadFileName * workload_fname, const char * out_fname) {
    int ret = 0;
    ret = client.load_kv_requests(workload_fname->trans_fname, 0, 1000);
    assert(ret == 0);
    // assert(client.num_local_operations_ == 1000);

    printf("lat test %s\n", workload_fname->trans_fname);
    uint64_t * lat_list = (uint64_t *)malloc(sizeof(uint64_t) * client.num_local_operations_);
    memset(lat_list, 0, sizeof(uint64_t) * client.num_local_operations_);

    uint32_t num_failed = 0;
    void * search_addr;
    struct timeval st, et;
    client.init_kvreq_space(0, 0, client.num_local_operations_);
    for (int i = 0; i < client.num_local_operations_; i ++) {
        KVReqCtx * ctx = &client.kv_req_ctx_list_[i];
        ctx->coro_id = 0;

        switch (ctx->req_type) {
        case KV_REQ_SEARCH:
            gettimeofday(&st, NULL);
            search_addr = client.kv_search_sync(ctx);
            gettimeofday(&et, NULL);
            if (search_addr == NULL) {
                num_failed ++;
            }
            break;
        case KV_REQ_INSERT:
            gettimeofday(&st, NULL);
            ret = client.kv_insert_sync(ctx);
            gettimeofday(&et, NULL);
            if (ret == KV_OPS_FAIL_REDO || ret == KV_OPS_FAIL_RETURN) {
                num_failed ++;
            }
            break;
        case KV_REQ_UPDATE:
            gettimeofday(&st, NULL);
            ret = client.kv_update_sync(ctx);
            gettimeofday(&et, NULL);
            break;
        case KV_REQ_DELETE:
            gettimeofday(&st, NULL);
            ret = client.kv_delete_sync(ctx);
            gettimeofday(&et, NULL);
            break;
        default:
            assert(0);
            break;
        }

        lat_list[i] = (et.tv_sec - st.tv_sec) * 1000000 + (et.tv_usec - st.tv_usec);
    }
    printf("Failed: %d\n", num_failed);
    
    FILE * lat_fp = fopen(out_fname, "w");
    assert(lat_fp != NULL);
    for (int i = 0; i < client.num_local_operations_; i ++) {
        fprintf(lat_fp, "%ld\n", lat_list[i]);
    }
    fclose(lat_fp);
    return 0;
}