//
// Created by ilya on 7/11/16.
//

#include <zlib.h>
#include <cassandra.h>
#include "MapCassandraOperation.h"
#include "consts.h"
#include "YieldCallback.h"

MapCassandraOperation::MapCassandraOperation(Isolate::CreateParams create_params, KVWriter *writer, const char* cassandra_nodes) {
    this->create_params = create_params;
    this->writer = writer;
    this->cassandra_nodes = cassandra_nodes;
}

void MapCassandraOperation::map(const char* account, int year, int month, int bucket, const char *javascript) {

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

            CassFuture* connect_future = NULL;
            CassCluster* cluster = cass_cluster_new();
            CassSession* session = cass_session_new();

            /* Add contact points */
            cass_cluster_set_contact_points(cluster, "stormn");

            connect_future = cass_session_connect(session, cluster);

            if (cass_future_error_code(connect_future) == CASS_OK) {
                CassFuture* close_future = NULL;

                /* Build statement and execute query */
                CassStatement* statement
                        = cass_statement_new("SELECT date,event_id,region,event "
                                                     "FROM ct.data_bucket WHERE account = ? AND year = ? "
                                                     "AND month = ? AND bucket = ?", 4);

                cass_statement_bind_string(statement, 0, account);
                cass_statement_bind_int32(statement, 1, year);
                cass_statement_bind_int32(statement, 2, month);
                cass_statement_bind_int32(statement, 3, bucket);


                CassFuture* result_future = cass_session_execute(session, statement);

                if(cass_future_error_code(result_future) == CASS_OK) {
                    /* Retrieve result set and iterate over the rows */
                    const CassResult* result = cass_future_get_result(result_future);
                    CassIterator* rows = cass_iterator_from_result(result);

                    while(cass_iterator_next(rows)) {
                        const CassRow* row = cass_iterator_get_row(rows);

                        const CassValue* date = cass_row_get_column_by_name(row, "date");
                        const CassValue* event_id = cass_row_get_column_by_name(row, "event_id");
                        const CassValue* region = cass_row_get_column_by_name(row, "region");
                        const CassValue* event = cass_row_get_column_by_name(row, "event");

                        size_t event_len;
                        const char* event_str;

                        cass_value_get_string(event, &event_str, &event_len);
                        /*printf("event: '%.*s'\n",
                               (int)event_len, event_str);*/

                        call((char*)event_str, (int)event_len, callback, function);
                    }

                    cass_result_free(result);
                    cass_iterator_free(rows);
                } else {
                    /* Handle error */
                    const char* message;
                    size_t message_length;
                    cass_future_error_message(result_future, &message, &message_length);
                    fprintf(stderr, "Unable to run query: '%.*s'\n",
                            (int)message_length, message);
                }

                cass_statement_free(statement);
                cass_future_free(result_future);

                /* Close the session */
                close_future = cass_session_close(session);
                cass_future_wait(close_future);
                cass_future_free(close_future);
            } else {
                /* Handle error */
                const char* message;
                size_t message_length;
                cass_future_error_message(connect_future, &message, &message_length);
                fprintf(stderr, "Unable to connect: '%.*s'\n",
                        (int)message_length, message);
            }

            cass_future_free(connect_future);
            cass_cluster_free(cluster);
            cass_session_free(session);



        }


    }

    // Dispose the isolate and tear down V8.
    isolate->Dispose();


}

void MapCassandraOperation::call(char *data, int length, Local<Function> callback, Local<Function>function) {
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