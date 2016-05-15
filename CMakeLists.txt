cmake_minimum_required(VERSION 3.5)
project(V8FileEval)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -pthread")

link_libraries(icuuc icui18n v8 /home/ilya/Project/V8EVal/v8/out/x64.release/obj.target/tools/gyp/libv8_libplatform.a)

set(SOURCE_FILES main.cpp MapWriter.cpp MapWriter.h MergeMap.cpp MergeMap.h)
add_executable(V8FileEval ${SOURCE_FILES})