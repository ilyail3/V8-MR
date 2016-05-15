// Copyright 2015 the V8 project authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <fcntl.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <byteswap.h>
#include <vector>
#include <map>
#include <mutex>


#include "v8/libplatform/libplatform.h"
#include "v8/v8.h"
#include "MapWriter.h"
#include "MergeMap.h"

#define MAP_WRITER 1001

using namespace v8;

class ArrayBufferAllocator : public v8::ArrayBuffer::Allocator {
public:
    virtual void *Allocate(size_t length) {
        void *data = AllocateUninitialized(length);
        return data == NULL ? data : memset(data, 0, length);
    }

    virtual void *AllocateUninitialized(size_t length) { return malloc(length); }

    virtual void Free(void *data, size_t) { free(data); }
};

struct ReduceKeyContext {
    bool iterated;
    FILE *file_handler;
    char* member;
};


void yield_callback(const FunctionCallbackInfo<Value> &args) {

    Isolate *isolate = args.GetIsolate();
    HandleScope scope(isolate);

    if (args.Length() < 1) {
        isolate->ThrowException(Exception::TypeError(
                String::NewFromUtf8(isolate, "Wrong number of arguments")));
        return;
    }


    Handle<Object> JSON = isolate->GetEnteredContext()->Global()->Get(String::NewFromUtf8(isolate, "JSON"))->ToObject();

    Local<Function> stringify = Handle<Function>::Cast(JSON->Get(String::NewFromUtf8(isolate, "stringify")));

    MapWriter *mapWriter = (MapWriter *) Local<External>::Cast(args.Data())->Value();


    Local<Value> argv[] = {
            args[0]
    };

    if(args[0]->IsUndefined())
        argv[0] = Null(isolate);

    v8::String::Utf8Value key(stringify->Call(stringify, 1, argv));

    if (args.Length() == 1) {
        mapWriter->write_key(*key);

    } else {
        argv[0] = args[1];

        if(args[1]->IsUndefined())
            argv[0] = Null(isolate);

        v8::String::Utf8Value value(stringify->Call(stringify, 1, argv));

        mapWriter->write_key_value(*key, *value);
    }
}

void values_callback(const FunctionCallbackInfo<Value> &args) {
    Isolate *isolate = args.GetIsolate();
    HandleScope scope(isolate);

    if (args.Length() < 1) {
        isolate->ThrowException(Exception::TypeError(
                String::NewFromUtf8(isolate, "Wrong number of arguments")));
        return;
    }

    ReduceKeyContext *reduceKeyContext = (ReduceKeyContext *) Local<External>::Cast(args.Data())->Value();

    reduceKeyContext->iterated = true;

    Local<Function> v_function = Local<Function>::Cast(args[0]);



    read_string(reduceKeyContext->file_handler, reduceKeyContext->member);

    Local<Value> argv[1];

    while(strlen(reduceKeyContext->member) != 0){

        argv[0] = JSON::Parse(String::NewFromUtf8(isolate, reduceKeyContext->member, NewStringType::kNormal, strlen(reduceKeyContext->member)).ToLocalChecked());

        v_function->Call(v_function, 1, argv);
        read_string(reduceKeyContext->file_handler, reduceKeyContext->member);
    }
}

void map_record(Local<Value> global, Local<Function> function, Local<Function> callback, const char *data,
               int data_len) {
    Isolate *isolate = Isolate::GetCurrent();

    // This is important, without it V8 won't GC the variables used by the match
    HandleScope handle_scope(isolate);

    const unsigned argc = 2;
    Local<Value> argv[argc] = {
            JSON::Parse(String::NewFromUtf8(isolate, data, NewStringType::kNormal, data_len).ToLocalChecked()),
            callback
    };

    function->Call(function, argc, argv);
}



void map(Isolate::CreateParams create_params, const char* input_file, const char* dir_name, char* map_filename){
    int totalOffset = 0, off = 0;
    int size;



    Isolate *isolate = Isolate::New(create_params);
    // Map block
    {

        FILE *fd = fopen(input_file, "r");
        if (fd == NULL) {
            perror("Cannot open file");
            exit(1);
        }
        struct stat st;
        stat(input_file, &st);


        const int page_size = getpagesize() * 10;
        const int at_least = 2000;

        char data[page_size];
        char tmp[at_least];

        char *record_data;
        const size_t file_size = st.st_size;

        int total = 0;

        fread(data, 1, page_size, fd);

        MapWriter mapWriter(dir_name);

        Isolate::Scope isolate_scope(isolate);

        HandleScope handle_scope(isolate);

        // Create a new context.
        Local<Context> context = Context::New(isolate);

        // Enter the context for compiling and running the hello world script.
        Context::Scope context_scope(context);

        // Create a string containing the JavaScript source code.
        Local<String> source =
                String::NewFromUtf8(isolate,
                                    "function map(obj,yield){ yield(obj.userAgent); }",
                                    NewStringType::kNormal).ToLocalChecked();

        // Compile the source code.
        Local<Script> script = Script::Compile(context, source).ToLocalChecked();

        // Run the script to get the result.
        script->Run(context);

        Local<Object> global = context->Global();

        Local<Value> v_unkown = global->Get(String::NewFromUtf8(isolate, "map"));

        //isolate->SetData(MAP_WRITER, &map_writer);

        Local<Function> callback = FunctionTemplate::New(isolate, yield_callback,
                                                         External::New(isolate, &mapWriter))->GetFunction();


        if (v_unkown->IsFunction()) {
            Local<Function> v_function = Local<Function>::Cast(v_unkown);

            while (totalOffset < file_size - 1) {

                size = (unsigned char) data[off] << 8;
                size += (unsigned char) data[off + 1];

                record_data = (data + off + 2);

                total ++;
                map_record(global, v_function, callback, record_data, size);


                if (off + 2 + size > page_size - at_least) {
                    int last_position = off + 2 + size;
                    int leftover = page_size - last_position;
                    // Copy the unprocessed bits to the start of the buffer,
                    // since sequential read isn't going to touch them again

                    // The easiest way to do this would be through a tmp buffer, although, if at_least < page_size / 2
                    // The read & write zones will never overlap
                    memcpy(tmp, data + last_position, leftover);
                    // Copy the same amount of data to the start of the data buffer
                    memcpy(data, tmp, leftover);

                    // Read the next block, from the part I'm not preserving
                    fread(data + leftover, 1, page_size - leftover, fd);

                    off = 0;
                } else
                    off += 2 + size;
                totalOffset += 2 + size;

            }


        }

        printf("done, total:%d\n", total);

        fclose(fd);
    }

    // Dispose the isolate and tear down V8.
    isolate->Dispose();

    char last_file[200];
    merge_inputs(dir_name, last_file);

    sprintf(map_filename, "%s/%s", dir_name, last_file);
}

void reduce_key(char* key, Local<Function> function, Local<Function> yield_cb, Local<Function> values_cb){
    Isolate *isolate = Isolate::GetCurrent();

    // This is important, without it V8 won't GC the variables used by the match
    HandleScope handle_scope(isolate);

    const unsigned argc = 3;
    Local<Value> argv[argc] = {
            JSON::Parse(String::NewFromUtf8(isolate, key, NewStringType::kNormal, strlen(key)).ToLocalChecked()),
            values_cb,
            yield_cb
    };

    function->Call(function, argc, argv);
}

void reduce(Isolate::CreateParams create_params, const char* input_file, const char* dir_name, char* reduce_filename){


    {
        MapWriter mapWriter(dir_name);

        FILE *fd = fopen(input_file, "r");
        if (fd == NULL) {
            perror("Cannot open reduce file");
            exit(1);
        }

        ReduceKeyContext reduceKeyContext;
        reduceKeyContext.file_handler = fd;

        //char key[MAX_ITEM_SIZE];
        char* key = (char*)malloc(MAX_ITEM_SIZE);
        char* member = (char*)malloc(MAX_ITEM_SIZE);

        reduceKeyContext.member = member;

        Isolate *isolate = Isolate::New(create_params);
        // Map block
        {

            Isolate::Scope isolate_scope(isolate);

            HandleScope handle_scope(isolate);

            // Create a new context.
            Local<Context> context = Context::New(isolate);

            // Enter the context for compiling and running the hello world script.
            Context::Scope context_scope(context);

            // Create a string containing the JavaScript source code.
            Local<String> source =
                    String::NewFromUtf8(isolate,
                                        "function reduce(key, values_cb, yield){ var i = 0 ; values_cb(function(v){ i += 1; }); yield(key, i); }",
                                        NewStringType::kNormal).ToLocalChecked();

            // Compile the source code.
            Local<Script> script = Script::Compile(context, source).ToLocalChecked();

            // Run the script to get the result.
            script->Run(context);

            Local<Object> global = context->Global();

            Local<Value> v_unkown = global->Get(String::NewFromUtf8(isolate, "reduce"));

            //isolate->SetData(MAP_WRITER, &map_writer);



            if (v_unkown->IsFunction()) {
                Local<Function> v_function = Local<Function>::Cast(v_unkown);


                Local<Function> yield_cb = FunctionTemplate::New(isolate, yield_callback,
                                                                 External::New(isolate, &mapWriter))->GetFunction();

                Local<Function> values_cb = FunctionTemplate::New(isolate, values_callback,
                                                                  External::New(isolate,
                                                                                &reduceKeyContext))->GetFunction();

                read_string(fd, key);
                while (strlen(key) != 0) {
                    reduceKeyContext.iterated = false;

                    reduce_key((char *) key, v_function, yield_cb, values_cb);


                    if (!reduceKeyContext.iterated) {
                        // Values are ignored, iterate over them any way since it's required to reach next key
                        read_string(fd, member);

                        while (strlen(key) != 0)
                            read_string(fd, member);
                    }

                    read_string(fd, key);
                }

            }

            // Convert the result to an UTF8 string and print it.
            //String::Utf8Value utf8(result);
            //printf("%s\n", *utf8);
        }

        // Dispose the isolate and tear down V8.
        isolate->Dispose();

        fclose(fd);
        free(key);
        free(member);
    }



    char last_file[200];
    merge_inputs(dir_name, last_file);

    sprintf(reduce_filename, "%s/%s", dir_name, last_file);
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

    const char *map_output = "/tmp/map_result";
    const char *reduce_output = "/tmp/reduce_result";

    char map_file[300];
    char reduce_file[300];

    map(create_params, "/home/ilya/Project/V8FileEval/records", map_output, map_file);
    //strcpy(map_file,"/tmp/map_result/00005_00000.map");

    reduce(create_params, map_file, reduce_output, reduce_file);

    printf("Result written to:%s\n", reduce_file);



    // Reduce block


    //printf("Total keys:%ld\n", projContext.mapData->size());


    V8::Dispose();
    V8::ShutdownPlatform();
    //delete platform;
    return 0;
}