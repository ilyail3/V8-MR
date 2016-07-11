//
// Created by ilya on 7/9/16.
//

#pragma once
#include "libplatform/libplatform.h"
#include "v8.h"
#include "writer/KVWriter.h"
#include "reader/RecordsReader.h"

using namespace v8;

class MapOperation {
public:
    MapOperation(Isolate::CreateParams create_params, KVWriter* writer);

    void map(RecordsReader *reader, const char* javascript);
private:
    Isolate::CreateParams create_params;
    KVWriter* writer;

    static void map_record_cb(void* ref, const char* record, int size);

    // Variables required for callback to work as expected
    Local<Function> callback;
    Local<Function> function;
};