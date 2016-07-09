cmake_minimum_required(VERSION 3.5)
project(V8FileEval)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -pthread")

link_libraries(icuuc icui18n v8 /home/ilya/Project/V8EVal/v8/out/x64.release/obj.target/tools/gyp/libv8_libplatform.a)

find_package(RapidJSON)


cmake_minimum_required(VERSION 2.8)

file(GLOB_RECURSE V8FileEval_SRC
        "*.h"
        "*.cpp"
        )

add_executable(V8FileEval ${V8FileEval_SRC} src/main.cpp)