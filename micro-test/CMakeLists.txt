add_library(latency_test latency_test.cc)
add_library(micro_test micro_test.cc)
add_executable(latency_test_client latency_test_client.cc)
add_executable(latency_test_client_cr latency_test_client_cr.cc)
add_executable(micro_test_multi_client micro_test_multi_client.cc)
add_executable(micro_test_multi_client_cr micro_test_multi_client_cr.cc)

target_link_libraries(latency_test
    libddckv
    ycsb_test
    pthread
    ibverbs
)

target_link_libraries(latency_test_client
    latency_test
    libddckv
    pthread
    ibverbs
)

target_link_libraries(latency_test_client_cr
    latency_test
    libddckv
    pthread
    ibverbs
)

target_link_libraries(micro_test
    libddckv
    pthread
    ibverbs
)

target_link_libraries(micro_test_multi_client
    micro_test
    libddckv
    pthread
    ibverbs
)

target_link_libraries(micro_test_multi_client_cr
    micro_test
    libddckv
    pthread
    ibverbs
)