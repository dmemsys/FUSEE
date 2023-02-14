#ifndef DDCKV_LATENCY_TEST_H_
#define DDCKV_LATENCY_TEST_H_

#include "client.h"
#include "client_cr.h"

int test_insert_lat(Client & client);
int test_search_lat(Client & client);
int test_update_lat(Client & client);
int test_delete_lat(Client & client);

int test_insert_lat(ClientCR & client);
int test_search_lat(ClientCR & client);
int test_update_lat(ClientCR & client);
int test_delete_lat(ClientCR & client);

#endif