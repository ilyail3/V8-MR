//
// Created by ilya on 7/10/16.
//

#pragma once


#include <v8/v8.h>
#include "writer/KVWriter.h"
#include "reader/KVStreamReader.h"

using namespace v8;

class ReduceOperation {
public:
    ReduceOperation(Isolate::CreateParams create_params, KVWriter* writer);
    ~ReduceOperation();

    void reduce(KVStreamReader *reader, const char* javascript);
private:
    Isolate::CreateParams create_params;
    KVWriter* writer;

    bool values_iterated;
    KVStreamReader* reader;

    static void values_callback(const FunctionCallbackInfo<Value>&args);
    void reduce_key(char *key, int size, Local<Function> function, Local<Function> yield_cb, Local<Function> values_cb);

    char value[MAX_VAL_SIZE];
};


