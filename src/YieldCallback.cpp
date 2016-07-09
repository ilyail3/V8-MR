//
// Created by ilya on 7/9/16.
//

#include "YieldCallback.h"

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

    KVWriter *mapWriter = (KVWriter *) Local<External>::Cast(args.Data())->Value();


    Local<Value> argv[] = {
            args[0]
    };

    if (args[0]->IsUndefined())
        argv[0] = Null(isolate);

    v8::String::Utf8Value key(stringify->Call(stringify, 1, argv));

    if (args.Length() == 1) {
        mapWriter->write(*key);
    } else {
        argv[0] = args[1];

        if (args[1]->IsUndefined())
            argv[0] = Null(isolate);

        v8::String::Utf8Value value(stringify->Call(stringify, 1, argv));

        mapWriter->write(*key, *value);
    }
}

Local<Function> yield_callback_function(v8::Isolate* isolate, KVWriter* writer){
    FunctionTemplate::New(isolate, yield_callback, External::New(isolate, writer))->GetFunction();
}

