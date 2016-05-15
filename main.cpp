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


#include "v8/libplatform/libplatform.h"
#include "v8/v8.h"

using namespace v8;

class ArrayBufferAllocator : public v8::ArrayBuffer::Allocator {
public:
    virtual void* Allocate(size_t length) {
        void* data = AllocateUninitialized(length);
        return data == NULL ? data : memset(data, 0, length);
    }
    virtual void* AllocateUninitialized(size_t length) { return malloc(length); }
    virtual void Free(void* data, size_t) { free(data); }
};

bool matchData(Local<Value> global, Local<Function> function, const char* data, int data_len){
    Isolate *isolate = Isolate::GetCurrent();

    // This is important, without it V8 won't GC the variables used by the match
    HandleScope handle_scope(isolate);

    const unsigned argc = 1;
    Local<Value> argv[argc] = {
            JSON::Parse(String::NewFromUtf8(isolate, data, NewStringType::kNormal, data_len).ToLocalChecked())
    };

    Local<Value> result = function->Call(function, argc, argv);

    bool bResult = false;

    if(result ->IsBoolean()){
        bResult = result->BooleanValue();
    }



    return bResult;
}


int main(int argc, char* argv[]) {
    // Initialize V8.
    V8::InitializeICU();
    V8::InitializeExternalStartupData(argv[0]);
    Platform* platform = platform::CreateDefaultPlatform();
    V8::InitializePlatform(platform);
    V8::Initialize();

    // Create a new Isolate and make it the current one.
    ArrayBufferAllocator allocator;
    Isolate::CreateParams create_params;
    create_params.array_buffer_allocator = &allocator;

    const char* filename = "/tmp/records";

    FILE *fd = fopen(filename, "r");
    struct stat st;
    stat(filename, &st);


    const int page_size=getpagesize() * 10;
    const int at_least = 2000;

    char data[page_size];
    char tmp[at_least];

    char* record_data;
    const size_t file_size=st.st_size;

    int totalOffset = 0, off = 0;
    int size;

    fread(data, 1, page_size, fd);


    Isolate* isolate = Isolate::New(create_params);
    {
        Isolate::Scope isolate_scope(isolate);

        HandleScope handle_scope(isolate);

        // Create a new context.
        Local<Context> context = Context::New(isolate);

        // Enter the context for compiling and running the hello world script.
        Context::Scope context_scope(context);

        // Create a string containing the JavaScript source code.
        Local<String> source =
                String::NewFromUtf8(isolate, "function v8_eval(obj){ return obj.userAgent == 'my.RightScale.com'; }",
                                    NewStringType::kNormal).ToLocalChecked();

        // Compile the source code.
        Local<Script> script = Script::Compile(context, source).ToLocalChecked();

        // Run the script to get the result.
        script->Run(context);

        Local<Object> global = context->Global();

        Local<Value> v_unkown = global->Get(String::NewFromUtf8(isolate, "v8_eval"));


        int total = 0;
        int totalMatch = 0;

        if(v_unkown->IsFunction()){
            Local<Function> v_function = Local<Function>::Cast(v_unkown);

            while(totalOffset < file_size - 1){

                size = (unsigned char)data[off] << 8;
                size += (unsigned char)data[off+1];

                record_data = (data + off + 2);

                total += 1;


                if(matchData(global, v_function, record_data, size))
                    totalMatch += 1;

                if(off + 2 + size > page_size - at_least){
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

                if(total % 10000 == 0)
                    printf("total:%d totalMatch:%d\n", total, totalMatch);
            }


        }

        printf("done, total:%d totalMatch:%d\n", total, totalMatch);
        // Convert the result to an UTF8 string and print it.
        //String::Utf8Value utf8(result);
        //printf("%s\n", *utf8);
    }

    // Dispose the isolate and tear down V8.
    isolate->Dispose();
    V8::Dispose();
    V8::ShutdownPlatform();
    //delete platform;
    return 0;
}