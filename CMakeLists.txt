cmake_minimum_required(VERSION 3.16 FATAL_ERROR)

project(DDCKV LANGUAGES CXX)
set(CMAKE_CXX_STANDARD 11)

find_package(Boost REQUIRED)

include_directories(src)
set(CMAKE_BUILD_TYPE Release)

add_subdirectory(src)

add_subdirectory(ycsb-test)
add_subdirectory(crash-recover-test)
add_subdirectory(micro-test)

enable_testing()
add_subdirectory(tests)