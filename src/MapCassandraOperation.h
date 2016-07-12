//
// Created by ilya on 7/11/16.
//

#pragma once


#include <v8.h>
#include "writer/KVWriter.h"
using namespace v8;

class MapCassandraOperation {
public:
    MapCassandraOperation(Isolate::CreateParams create_params, KVWriter* writer, const char* cassandra_nodes);

    void map(const char* account, int year, int month, int bucket, const char* javascript);
private:
    Isolate::CreateParams create_params;
    KVWriter* writer;
    const char* cassandra_nodes;

    void call(char* data, int length, Local<Function> callback, Local<Function>function);
};
