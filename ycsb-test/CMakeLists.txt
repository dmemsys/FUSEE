add_library(ycsb_test ycsb_test.cc)
add_executable(ycsb_test_client ycsb_test_client.cc)
add_executable(ycsb_test_server ycsb_test_server.cc)
add_executable(ycsb_wl_loader ycsb_wl_loader.cc)
add_executable(ycsb_wl_worker ycsb_wl_worker.cc)
add_executable(ycsb_test_multi_client ycsb_test_multi_client.cc)
add_executable(ycsb_multi_client_cont_tpt ycsb_multi_client_cont_tpt.cc)
add_executable(ycsb_server_crash_multi_client ycsb_server_crash_multi_client.cc)

target_compile_options(ycsb_test_client
    PRIVATE
    ${CMAKE_CXX_FLAGS_DEBUG}
    "-g"
)

target_compile_options(ycsb_test_multi_client
    PRIVATE
    "-g"
)

target_link_libraries(ycsb_test
    libddckv
    pthread
    ibverbs
)

target_link_libraries(ycsb_test_multi_client
    libddckv
    ycsb_test
    ibverbs
)

target_link_libraries(ycsb_test_client
    libddckv
    ycsb_test
    pthread
    ibverbs
)

target_link_libraries(ycsb_test_server
    libddckv
    ycsb_test
    pthread
    ibverbs
)


target_link_libraries(ycsb_wl_worker
libddckv
ycsb_test
pthread
ibverbs
)

target_link_libraries(ycsb_wl_loader
    libddckv
    ycsb_test
    pthread
    ibverbs
)
    
target_link_libraries(ycsb_multi_client_cont_tpt
    ycsb_test
    libddckv
    pthread
    ibverbs
)
    
target_link_libraries(ycsb_server_crash_multi_client
    ycsb_test
    libddckv
    pthread
    ibverbs
)