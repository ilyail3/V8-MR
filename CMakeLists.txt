cmake_minimum_required(VERSION 3.5)
project(V8FileEval)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -pthread")

set(V8DIR "/home/ilya/Downloads/v8")

include_directories(BEFORE "${V8DIR}/include")

set(V8TARGET "${V8DIR}/out/native/obj.target")

link_libraries(
        "icuuc"
        "icui18n"
        "v8"
        "v8_libplatform"
        "z"
        "boost_system"
        "boost_filesystem"
        "cassandra"
)

find_package(RapidJSON)

file(GLOB_RECURSE V8FileEval_SRC
        "*.h"
        "*.cpp"
        )

add_executable(V8FileEval ${V8FileEval_SRC})

#configure_file(
#        "${V8DIR}/out/native/natives_blob.bin"
#        "${V8DIR}/out/native/snapshot_blob.bin"
#        COPYONLY
#)

file(COPY "${V8DIR}/out/native/natives_blob.bin" DESTINATION "./")
file(COPY "${V8DIR}/out/native/snapshot_blob.bin" DESTINATION "./")
