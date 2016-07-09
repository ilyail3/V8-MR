//
// Created by ilya on 7/9/16.
//

#include <string>
#include "MapOperation.h"
#include "YieldCallback.h"


void MapOperation::map_record_cb(void *ref, const char *record, int size) {
    MapOperation *map = (MapOperation *) ref;

    Isolate *isolate = Isolate::GetCurrent();

    // This is important, without it V8 won't GC the variables used by the match
    HandleScope handle_scope(isolate);

    const unsigned argc = 2;
    Local<Value> argv[argc] = {
            JSON::Parse(String::NewFromUtf8(isolate, record, NewStringType::kNormal, size).ToLocalChecked()),
            map->callback
    };

    map->function->Call(map->function, argc, argv);
}

MapOperation::MapOperation(Isolate::CreateParams create_params, KVWriter *writer) {
    this->create_params = create_params;
    this->writer = writer;
}

void MapOperation::map(RecordsReader *reader, const char *js_source) {
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
                                    js_source,
                                    NewStringType::kNormal).ToLocalChecked();

        // Compile the source code.
        Local<Script> script = Script::Compile(context, source).ToLocalChecked();

        // Run the script to get the result.
        script->Run(context);

        Local<Object> global = context->Global();

        Local<Value> v_unkown = global->Get(String::NewFromUtf8(isolate, "map"));

        //isolate->SetData(MAP_WRITER, &map_writer);

        callback = yield_callback_function(isolate, writer);

        if (v_unkown->IsFunction()) {
            function = Local<Function>::Cast(v_unkown);

            reader->get_records(this, MapOperation::map_record_cb);
        }
    }

    // Dispose the isolate and tear down V8.
    isolate->Dispose();

    //char last_file[200];
    //merge_inputs(dir_name, last_file);

    //sprintf(map_filename, "%s/%s", dir_name, last_file);
}