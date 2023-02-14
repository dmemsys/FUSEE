add_executable(test_crash_client test_crash_client.cc)

target_link_libraries(test_crash_client
    libddckv
    pthread
    ibverbs
)