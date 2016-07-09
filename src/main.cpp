//
// Created by ilya on 7/9/16.
//

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <map>


#include "v8/libplatform/libplatform.h"
#include "v8/v8.h"
#include "writer/SeqFileWriter.h"
#include "reader/CloudTrailReader.h"
#include "MapOperation.h"

using namespace v8;
using namespace std;

class ArrayBufferAllocator : public v8::ArrayBuffer::Allocator {
public:
    virtual void *Allocate(size_t length) {
        void *data = AllocateUninitialized(length);
        return data == NULL ? data : memset(data, 0, length);
    }

    virtual void *AllocateUninitialized(size_t length) { return malloc(length); }

    virtual void Free(void *data, size_t) { free(data); }
};

void map_function(Isolate::CreateParams create_params, const char* filename, const char* dir_output, const char* js_source){
    SeqFileWriter writer(dir_output);
    CloudTrailReader reader(filename);

    MapOperation mapOperation(create_params, &writer);

    mapOperation.map(&reader, js_source);
}

int main(int argc, char *argv[]) {



    // Initialize V8.
    V8::InitializeICU();
    V8::InitializeExternalStartupData(argv[0]);
    Platform *platform = platform::CreateDefaultPlatform();
    V8::InitializePlatform(platform);
    V8::Initialize();

    // Create a new Isolate and make it the current one.
    ArrayBufferAllocator allocator;
    Isolate::CreateParams create_params;
    create_params.array_buffer_allocator = &allocator;

    /*const char *map_output = "/tmp/map_result";
    const char *reduce_output = "/tmp/reduce_result";
    const char *pretty_output = "/tmp/pretty";

    char map_file[300];
    char reduce_file[300];

    map(create_params, "/home/ilya/Downloads/cloudtrails.json", map_output, map_file);
    //strcpy(map_file,"/tmp/map_result/00005_00000.map");

    reduce(create_params, map_file, reduce_output, reduce_file);

    //printf("Result written to:%s\n", reduce_file);
    pretty_print(reduce_file, pretty_output);*/


    map_function(
            create_params,
            argv[1],
            "/tmp/map_results",
            "function map(obj,yield){ yield(obj.userAgent); }"
    );



    // Reduce block


    //printf("Total keys:%ld\n", projContext.mapData->size());


    V8::Dispose();
    V8::ShutdownPlatform();
    //delete platform;
    return 0;
}