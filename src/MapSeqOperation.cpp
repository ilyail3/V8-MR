//
// Created by ilya on 7/11/16.
//

#include <zlib.h>
#include "MapSeqOperation.h"
#include "consts.h"
#include "YieldCallback.h"

MapSeqOperation::MapSeqOperation(Isolate::CreateParams create_params, KVWriter *writer) {
    this->create_params = create_params;
    this->writer = writer;
}

void MapSeqOperation::map(const char *filename, const char *javascript) {
    char record[MAX_VAL_SIZE];
    uint16_t size = 0;

    auto gz = gzopen(filename, "rb");

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
                                    javascript,
                                    NewStringType::kNormal).ToLocalChecked();

        // Compile the source code.
        Local<Script> script = Script::Compile(context, source).ToLocalChecked();

        // Run the script to get the result.
        script->Run(context);

        Local<Object> global = context->Global();

        Local<Value> v_unkown = global->Get(String::NewFromUtf8(isolate, "map"));

        //isolate->SetData(MAP_WRITER, &map_writer);

        auto callback = yield_callback_function(isolate, writer);

        if (v_unkown->IsFunction()) {
            auto function = Local<Function>::Cast(v_unkown);

            while(!gzeof(gz)){
                gzread(gz, &size, 2);
                gzread(gz, record, size);

                call(record, size, callback, function);
                // call
            }

        }


    }

    // Dispose the isolate and tear down V8.
    isolate->Dispose();

    gzclose(gz);
}

void MapSeqOperation::call(char *data, int length, Local<Function> callback, Local<Function>function) {
    Isolate *isolate = Isolate::GetCurrent();

    // This is important, without it V8 won't GC the variables used by the match
    HandleScope handle_scope(isolate);

    const unsigned argc = 2;
    Local<Value> argv[argc] = {
            JSON::Parse(String::NewFromUtf8(isolate, data, NewStringType::kNormal, length).ToLocalChecked()),
            callback
    };

    function->Call(function, argc, argv);
}