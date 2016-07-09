#pragma once

#include <v8/v8.h>
#include "writer/KVWriter.h"

using namespace v8;

Local<Function> yield_callback_function(Isolate* isolate, KVWriter* writer);

