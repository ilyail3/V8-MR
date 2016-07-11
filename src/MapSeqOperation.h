//
// Created by ilya on 7/11/16.
//

#pragma once


#include <v8.h>
#include "writer/KVWriter.h"
using namespace v8;

class MapSeqOperation {
public:
    MapSeqOperation(Isolate::CreateParams create_params, KVWriter* writer);

    void map(const char* filename, const char* javascript);
private:
    Isolate::CreateParams create_params;
    KVWriter* writer;

    void call(char* data, int length, Local<Function> callback, Local<Function>function);
};
