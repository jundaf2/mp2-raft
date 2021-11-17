cmake_minimum_required(VERSION 2.28)
project(mp2_raft)

set(CMAKE_CXX_STANDARD 14)

# CMakeLists.txt
add_subdirectory(common)

add_executable(raft main.cpp)

find_package(Threads REQUIRED)
if(THREADS_HAVE_PTHREAD_ARG)
    target_compile_options(raft PUBLIC "-pthread")
endif()
if(CMAKE_THREAD_LIBS_INIT)
    target_link_libraries(raft "${CMAKE_THREAD_LIBS_INIT}")
endif()
