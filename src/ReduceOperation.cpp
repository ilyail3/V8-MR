//
// Created by ilya on 7/10/16.
//

#include "consts.h"
#include "ReduceOperation.h"
#include "YieldCallback.h"

using namespace v8;

ReduceOperation::ReduceOperation(Isolate::CreateParams create_params, KVWriter *writer) {
    this->writer = writer;
    this->create_params = create_params;
}

ReduceOperation::~ReduceOperation() {

}

void ReduceOperation::values_callback(const FunctionCallbackInfo<Value> &args) {
    Isolate *isolate = args.GetIsolate();
    HandleScope scope(isolate);

    if (args.Length() < 1) {
        isolate->ThrowException(Exception::TypeError(
                String::NewFromUtf8(isolate, "Wrong number of arguments")));
        return;
    }

    ReduceOperation* reduceOp = (ReduceOperation*) Local<External>::Cast(args.Data())->Value();

    reduceOp->values_iterated = true;

    Local<Function> v_function = Local<Function>::Cast(args[0]);

    Local<Value> argv[1];

    while(reduceOp->reader->has_next() && reduceOp->reader->next_type() == TypeValue){
        int size = reduceOp->reader->next(reduceOp->value);

        argv[0] = JSON::Parse(String::NewFromUtf8(
                isolate,
                reduceOp->value,
                NewStringType::kNormal, size
        ).ToLocalChecked());

        v_function->Call(v_function, 1, argv);
    }
}

void ReduceOperation::reduce_key(char *key, int size, Local<Function> function, Local<Function> yield_cb, Local<Function> values_cb) {
    Isolate *isolate = Isolate::GetCurrent();

    // This is important, without it V8 won't GC the variables used by the match
    HandleScope handle_scope(isolate);

    const unsigned argc = 3;
    Local<Value> argv[argc] = {
            JSON::Parse(String::NewFromUtf8(isolate, key, NewStringType::kNormal, size).ToLocalChecked()),
            values_cb,
            yield_cb
    };

    function->Call(function, argc, argv);
}

void ReduceOperation::reduce(KVStreamReader *reader, const char *javascript) {
    char key[MAX_KEY_SIZE];
    int size;
    this->reader = reader;

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

        Local<Value> v_unkown = global->Get(String::NewFromUtf8(isolate, "reduce"));

        //isolate->SetData(MAP_WRITER, &map_writer);


        if (v_unkown->IsFunction()) {
            Local<Function> v_function = Local<Function>::Cast(v_unkown);


            Local<Function> yield_cb = yield_callback_function(isolate, writer);

            Local<Function> values_cb = FunctionTemplate::New(isolate, values_callback, External::New(isolate, this))->GetFunction();


            while (reader->has_next()) {
                values_iterated = false;

                size = reader->next(key);

                reduce_key(key, size, v_function, yield_cb, values_cb);


                if (!values_iterated) {
                    // Values are ignored, iterate over them any way since it's required to reach next key
                    while(reader->has_next() && reader->next_type() == TypeValue){
                        reader->next(key);
                    }
                }
            }

        }

        // Convert the result to an UTF8 string and print it.
        //String::Utf8Value utf8(result);
        //printf("%s\n", *utf8);
    }

    // Dispose the isolate and tear down V8.
    isolate->Dispose();
}