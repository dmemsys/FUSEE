#include <stdio.h>
#include <sys/time.h>

#include "client.h"

#include "latency_test.h"

#define WORKLOAD_ALL (-1)
// #define WORKLOAD_NUM WORKLOAD_ALL
#define WORKLOAD_NUM 100000


static int test_lat(Client & client, char * op_type, const char * out_fname) {
    int ret = 0;
    ret = client.load_seq_kv_requests(WORKLOAD_NUM, op_type);
    assert(ret == 0);

    printf("lat test %s\n", op_type);
    uint64_t * lat_list = (uint64_t *)malloc(sizeof(uint64_t) * client.num_local_operations_);
    memset(lat_list, 0, sizeof(uint64_t) * client.num_local_operations_);

    uint32_t num_failed = 0;
    void * search_addr;
    struct timeval st, et;
    bool should_stop = false;
    client.init_kvreq_space(0, 0, client.num_local_operations_);
    for (int i = 0; i < client.num_local_operations_; i ++) {
        KVReqCtx * ctx = &client.kv_req_ctx_list_[i];
        ctx->coro_id = 0;
        ctx->should_stop = &should_stop;
        // ctx->use_cache = false;

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
            if (ret == KV_OPS_FAIL_RETURN) {
                num_failed ++;
            }
            break;
        case KV_REQ_UPDATE:
            gettimeofday(&st, NULL);
            ret = client.kv_update_sync(ctx);
            if (ret == KV_OPS_FAIL_RETURN) {
                num_failed ++;
            }
            gettimeofday(&et, NULL);
            break;
        case KV_REQ_DELETE:
            gettimeofday(&st, NULL);
            ret = client.kv_delete_sync(ctx);
            if (ret == KV_OPS_FAIL_RETURN) {
                num_failed ++;
            }
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

static int test_lat(ClientCR & client, char * op_type, const char * out_fname) {
    int ret = 0;
    ret = client.load_seq_kv_requests(WORKLOAD_NUM, op_type);
    assert(ret == 0);

    printf("lat test %s\n", op_type);
    uint64_t * lat_list = (uint64_t *)malloc(sizeof(uint64_t) * client.num_local_operations_);
    memset(lat_list, 0, sizeof(uint64_t) * client.num_local_operations_);

    uint32_t num_failed = 0;
    void * search_addr;
    struct timeval st, et;
    bool should_stop = false;
    client.init_kvreq_space(0, 0, client.num_local_operations_);
    for (int i = 0; i < client.num_local_operations_; i ++) {
        KVReqCtx * ctx = &client.kv_req_ctx_list_[i];
        ctx->coro_id = 0;
        ctx->should_stop = &should_stop;

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

int test_insert_lat(Client & client) {
    char out_fname[128];
    int num_rep = client.get_num_rep();
    sprintf(out_fname, "results/insert_lat-%drp.txt", num_rep);
    return test_lat(client, "INSERT", out_fname);
}

int test_search_lat(Client & client) {
    char out_fname[128];
    int num_rep = client.get_num_rep();
    sprintf(out_fname, "results/search_lat-%drp.txt", num_rep);
    return test_lat(client, "READ", out_fname);
}

int test_update_lat(Client & client) {
    char out_fname[128];
    int num_rep = client.get_num_rep();
    sprintf(out_fname, "results/update_lat-%drp.txt", num_rep);
    return test_lat(client, "UPDATE", out_fname);
}

int test_delete_lat(Client & client) {
    char out_fname[128];
    int num_rep = client.get_num_rep();
    sprintf(out_fname, "results/delete_lat-%drp.txt", num_rep);
    return test_lat(client, "DELETE", out_fname);
}

int test_insert_lat(ClientCR & client) {
    char out_fname[128];
    int num_rep = client.get_num_rep();
    sprintf(out_fname, "results/insert_cr_lat-%drp.txt", num_rep);
    return test_lat(client, "INSERT", out_fname);
}

int test_search_lat(ClientCR & client) {
    char out_fname[128];
    int num_rep = client.get_num_rep();
    sprintf(out_fname, "results/search_cr_lat-%drp.txt", num_rep);
    return test_lat(client, "READ", out_fname);
}

int test_update_lat(ClientCR & client) {
    char out_fname[128];
    int num_rep = client.get_num_rep();
    sprintf(out_fname, "results/update_cr_lat-%drp.txt", num_rep);
    return test_lat(client, "UPDATE", out_fname);
}

int test_delete_lat(ClientCR & client) {
    char out_fname[128];
    int num_rep = client.get_num_rep();
    sprintf(out_fname, "results/delete_cr_lat-%drp.txt", num_rep);
    return test_lat(client, "DELETE", out_fname);
}