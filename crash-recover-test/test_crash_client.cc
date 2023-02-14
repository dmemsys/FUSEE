#include "client.h"

#define INSERT_NUM 1000
#define UPDATE_NUM 1000

const char * key = "test-12345-k";
const char * key_template = "test-%d-k";
const char * value_insert_template = "test-%d-v-insert";
const char * value_update_template = "test-12345-v-update-%d";
const char * value_template = "test-12345-v-%s";

KVReqCtx * prepare_ctx(Client & client, const char * key, const char * value, int req_type) {
    uint64_t client_input_buf = (uint64_t)client.get_input_buf();
    uint32_t client_input_buf_lkey = client.get_input_buf_lkey();

    memcpy((void *)(client_input_buf + sizeof(KVLogHeader)), key, strlen(key));
    memcpy((void *)(client_input_buf + sizeof(KVLogHeader) + strlen(key)), value, strlen(value));
    KVLogHeader * header = (KVLogHeader *)client_input_buf;
    header->is_valid = true;
    header->key_length = strlen(key);
    header->value_length = strlen(value);

    KVInfo * kv_info = (KVInfo *)malloc(sizeof(KVInfo));
    kv_info->l_addr = (void *)client_input_buf;
    kv_info->key_len = strlen(key);
    kv_info->value_len = strlen(value);
    kv_info->lkey = client_input_buf_lkey;

    KVReqCtx * ctx = new KVReqCtx;
    ctx->req_type = req_type;
    ctx->use_cache = true;
    ctx->kv_info = kv_info;
    ctx->lkey = client.get_local_buf_mr()->lkey;

    int num_idx_rep = client.get_num_idx_rep();
    int num_replication = client.get_num_rep();
    ctx->kv_modify_pr_cas_list.resize(1);
    ctx->kv_modify_bk_0_cas_list.resize(num_idx_rep - 1);
    ctx->kv_modify_bk_1_cas_list.resize(num_idx_rep - 1);
    ctx->log_commit_addr_list.resize(num_replication);
    char key_buf[128] = {0};
    memcpy(key_buf, (void *)((uint64_t)ctx->kv_info->l_addr + sizeof(KVLogHeader)), ctx->kv_info->key_len);
    ctx->key_str = std::string(key_buf);
    return ctx;
}

void init_insert_ctx(Client & client, KVReqCtx * ctx) {
    uint64_t client_local_buf = (uint64_t)client.get_local_buf_mr()->addr;
    uint32_t client_local_buf_lkey = client.get_local_buf_mr()->lkey;

    ctx->local_bucket_addr = (RaceHashBucket *)client_local_buf;
    ctx->local_cas_target_value_addr = (void *)((uint64_t)client_local_buf + 4 * sizeof(RaceHashBucket));
    ctx->local_cas_return_value_addr = (void *)((uint64_t)ctx->local_cas_target_value_addr + sizeof(uint64_t));
    ctx->op_laddr = (void *)((uint64_t)ctx->local_cas_return_value_addr + sizeof(uint64_t) * MAX_REP_NUM);
    ctx->lkey = client_local_buf_lkey;

    KVLogHeader * header = (KVLogHeader *)ctx->kv_info->l_addr;
    KVLogTail * tail = (KVLogTail *)((uint64_t)ctx->kv_info->l_addr
        + sizeof(KVLogHeader) + header->key_length + header->value_length);
    tail->op = KV_OP_INSERT;
}

void init_update_ctx(Client & client, KVReqCtx * ctx) {
    uint64_t client_local_buf = (uint64_t)client.get_local_buf_mr()->addr;
    uint32_t client_local_buf_lkey = (uint64_t)client.get_local_buf_mr()->rkey;

    ctx->local_bucket_addr = (RaceHashBucket *)client_local_buf;
    ctx->local_kv_addr = (void *)((uint64_t)client_local_buf + 4 * sizeof(RaceHashBucket));
    ctx->local_cas_target_value_addr = (void *)((uint64_t)client_local_buf + 4 * sizeof(RaceHashBucket));
    ctx->local_cas_return_value_addr = (void *)((uint64_t)ctx->local_cas_target_value_addr + sizeof(uint64_t));
    ctx->op_laddr = (void *)((uint64_t)ctx->local_cas_target_value_addr + sizeof(uint64_t) * MAX_REP_NUM);
    ctx->local_cache_addr = (void *)((uint64_t)ctx->op_laddr + 2048);
    ctx->lkey = client_local_buf_lkey;

    KVLogHeader * header = (KVLogHeader *)ctx->kv_info->l_addr;
    KVLogTail * tail = (KVLogTail *)((uint64_t)ctx->kv_info->l_addr
        + sizeof(KVLogHeader) + header->key_length + header->value_length);
    tail->op = KV_OP_UPDATE;
}

void init_search_ctx(Client & client, KVReqCtx * ctx) {
    uint64_t client_local_buf = (uint64_t)client.get_local_buf_mr()->addr;
    uint32_t client_local_buf_lkey = (uint64_t)client.get_local_buf_mr()->rkey;

    ctx->local_bucket_addr = (RaceHashBucket *)client_local_buf;
    ctx->local_cache_addr  = (void *)((uint64_t)client_local_buf + 4 * sizeof(RaceHashBucket));
    ctx->local_kv_addr     = (void *)((uint64_t)client_local_buf + 4 * sizeof(RaceHashBucket));
    ctx->lkey = client_local_buf_lkey;
}

void test_crash_update_prepare(Client & client, int crash_point) {
    int ret = 0;
    // insert a kv
    char value_buf[128];
    bool should_stop = false;

    sprintf(value_buf, value_insert_template, INSERT_NUM);
    KVReqCtx * insert_ctx = prepare_ctx(client, key, value_buf, KV_REQ_INSERT);
    init_insert_ctx(client, insert_ctx);
    insert_ctx->should_stop = &should_stop;
    insert_ctx->coro_id = 100;
    ret = client.kv_insert_sync(insert_ctx);
    assert(ret == 0);
    
    // update the kv and crash
    for (int i = 0; i < UPDATE_NUM - 1; i ++) {
        sprintf(value_buf, value_update_template, i);
        KVReqCtx * update_ctx = prepare_ctx(client, key, value_buf, KV_REQ_UPDATE);
        init_update_ctx(client, update_ctx);
        ret = client.kv_update_sync(update_ctx);
    }
    sprintf(value_buf, value_update_template, UPDATE_NUM);
    KVReqCtx * update_ctx = prepare_ctx(client, key, value_buf, KV_REQ_UPDATE);
    init_update_ctx(client, update_ctx);
    ret = client.kv_update_w_crash(update_ctx, crash_point);
    assert(ret == -1);
}

void test_crash_insert_prepare(Client & client, int crash_point) {
    int ret = 0;
    char key_buf[128];
    char value_buf[128];
    // insert 1000 kv
    for (int i = 0; i < INSERT_NUM; i ++) {
        sprintf(key_buf, key_template, i);
        sprintf(value_buf, value_insert_template, i);
        KVReqCtx * insert_ctx = prepare_ctx(client, key_buf, value_buf, KV_REQ_INSERT);
        init_insert_ctx(client, insert_ctx);
        bool should_stop = false;
        insert_ctx->should_stop = &should_stop;
        insert_ctx->coro_id = 100;
        ret = client.kv_insert_sync(insert_ctx);
        if (ret != 0) {
            printf("[%s] insert error\n", __FUNCTION__);
            exit(1);
        }
    }

    sprintf(value_buf, value_insert_template, INSERT_NUM);
    KVReqCtx * insert_ctx = prepare_ctx(client, key, value_buf, KV_REQ_INSERT);
    init_insert_ctx(client, insert_ctx);
    bool should_stop = false;
    insert_ctx->should_stop = &should_stop;
    insert_ctx->coro_id = 100;
    ret = client.kv_insert_w_crash(insert_ctx, crash_point);
    if (ret != -1) {
        printf("[%s] failed to crash\n", __FUNCTION__);
        exit(1);
    }
}

void test_crash_recover(Client & client) {
    int ret = 0;
    void * search_ret;
    char new_value_buf[256];
    sprintf(new_value_buf, value_template, "update-after-crash");
    // recover
    KVReqCtx * update_ctx = prepare_ctx(client, key, new_value_buf, KV_REQ_UPDATE);
    init_update_ctx(client, update_ctx);
    ret = client.kv_update_sync(update_ctx);
    if (ret != 0) {
        printf("[%s] error update %d\n", __FUNCTION__, ret);
    }
    // assert(ret == 0);

    KVReqCtx * search_ctx = prepare_ctx(client, key, new_value_buf, KV_REQ_SEARCH);
    init_search_ctx(client, search_ctx);
    search_ret = client.kv_search_sync(search_ctx);
    if (memcmp(search_ret, new_value_buf, strlen(new_value_buf)) != 0) {
        printf("recover failed\n");
    } else {
        printf("recover success!\n");
    }
}

int main(int argc, char ** argv) {
    if (argc != 2) {
        printf("Usage: %s path-to-config-file\n", argv[0]);
    }
    int ret = 0;
    GlobalConfig config;
    ret = load_config(argv[1], &config);
    assert(ret == 0);

    config.num_coroutines = 1;
    config.is_recovery = false;
    Client client(&config);
    // pthread_t pollint_tid = client.start_polling_thread();
    client.start_gc_fiber();
    
    if (config.is_recovery == false) {
        test_crash_insert_prepare(client, KV_CRASH_UNCOMMITTED_BK_CONSENSUS_0);
        printf("crashed\n");
    }
    client.stop_gc_fiber();

    config.is_recovery = true;
    std::vector<struct timeval> recover_time_bd;
    struct timeval st, et;
    gettimeofday(&st, NULL);
    Client clientr(&config);
    clientr.start_gc_fiber();
    gettimeofday(&et, NULL);
    clientr.get_recover_time(recover_time_bd);
    test_crash_recover(clientr);

    clientr.stop_gc_fiber();

    uint64_t connection_recover_time_us = time_spent_us(&recover_time_bd[0], &recover_time_bd[1]);
    uint64_t local_recover_space_reg_time_us = time_spent_us(&recover_time_bd[1], &recover_time_bd[2]);
    uint64_t get_meta_addr_time_us = time_spent_us(&recover_time_bd[2], &recover_time_bd[3]);
    uint64_t traverse_log_time_us = time_spent_us(&recover_time_bd[3], &recover_time_bd[4]);
    uint64_t mm_recover_time_us = time_spent_us(&recover_time_bd[4], &recover_time_bd[5]);
    uint64_t local_mr_reg_time_us = time_spent_us(&recover_time_bd[5], &recover_time_bd[6]);
    uint64_t kv_ops_recover_time_us = time_spent_us(&recover_time_bd[6], &recover_time_bd[7]);

    printf("0. conn rec: %ld us\n", connection_recover_time_us);
    printf("1. rec space reg: %ld us\n", local_recover_space_reg_time_us);
    printf("2. get meta addr: %ld us\n", get_meta_addr_time_us);
    printf("3. taverse log: %ld us\n", traverse_log_time_us);
    printf("4. mm rec: %ld us\n", mm_recover_time_us);
    printf("5. local mr reg time: %ld us\n", local_mr_reg_time_us);
    printf("6. ops rec: %ld us\n", kv_ops_recover_time_us);
    printf("total:%ld us\n", time_spent_us(&st, &et));
}