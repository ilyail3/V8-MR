//
// Created by ilya on 7/9/16.
//

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <map>


#include "libplatform/libplatform.h"
#include "v8.h"
#include "writer/SeqWriter.h"
#include "reader/CloudTrailReader.h"
#include "MapOperation.h"
#include "reader/KVStreamReader.h"
#include "SortedMerge.h"
#include "ReduceOperation.h"
#include "RecordCompressor.h"
#include "MapSeqOperation.h"
#include "MapCassandraOperation.h"
#include <sys/stat.h>
#include <boost/filesystem.hpp>
#include <rapidjson/filereadstream.h>
#include <aws/core/Aws.h>

#include <aws/sqs/SQSClient.h>
#include <aws/sqs/model/ReceiveMessageRequest.h>
#include <aws/sqs/model/DeleteMessageRequest.h>
#include <aws/core/auth/AWSCredentialsProvider.h>

#include "rapidjson/document.h"
using namespace rapidjson;

using namespace v8;
using namespace std;
using namespace boost::filesystem;

class ArrayBufferAllocator : public v8::ArrayBuffer::Allocator {
public:
    virtual void *Allocate(size_t length) {
        void *data = AllocateUninitialized(length);
        return data == NULL ? data : memset(data, 0, length);
    }

    virtual void *AllocateUninitialized(size_t length) {
        return malloc(length);
    }

    virtual void Free(void *data, size_t) {
        free(data);
    }
};

void map_function(Isolate::CreateParams create_params, const char* filename, const char* dir_output, const char* js_source){

    struct stat sb;

    if (stat(filename, &sb) == 0 && S_ISDIR(sb.st_mode)){
        boost::filesystem::recursive_directory_iterator itr(filename);
        SeqWriter writer(dir_output);

        while (itr != boost::filesystem::recursive_directory_iterator())
        {

            if(is_regular_file(itr->path())){
                //std::cout << itr->path().string() << std::endl;

                CloudTrailReader reader(itr->path().c_str());

                MapOperation mapOperation(create_params, &writer);

                mapOperation.map(&reader, js_source);
            }

            ++itr;
        }
    } else {
        SeqWriter writer(dir_output);
        CloudTrailReader reader(filename);

        MapOperation mapOperation(create_params, &writer);

        mapOperation.map(&reader, js_source);
    }
}

void map_seq_function(Isolate::CreateParams create_params, const char* filename, const char* dir_output, const char* js_source){
    clock_t begin = clock();

    SeqWriter writer(dir_output);
    MapSeqOperation seq(create_params, &writer);
    seq.map(filename, js_source);

    clock_t end = clock();
    double elapsed_secs = double(end - begin) / CLOCKS_PER_SEC;

    printf("Map took %0.2f seconds\n", elapsed_secs);
}

/*void map_cassandra_function(Isolate::CreateParams create_params, const char* account, int year, int month, int bucket, const char* dir_output, const char* js_source){
    clock_t begin = clock();

    SeqWriter writer(dir_output);
    MapCassandraOperation seq(create_params, &writer);
    seq.map(account, year, month, bucket, js_source);

    clock_t end = clock();
    double elapsed_secs = double(end - begin) / CLOCKS_PER_SEC;

    printf("Map took %0.2f seconds\n", elapsed_secs);
}*/

char* read_file(const char* filename){
    FILE *fp;
    long lSize;
    char *buffer;

    fp = fopen ( filename , "rb" );
    if( !fp ) perror(filename),exit(1);

    fseek( fp , 0L , SEEK_END);
    lSize = ftell( fp );
    rewind( fp );

    /* allocate memory for entire content */
    buffer = (char *) calloc(1, lSize + 1 );
    if( !buffer ) fclose(fp),fputs("memory alloc fails",stderr),exit(1);

    /* copy the file into the buffer */
    if( 1!=fread( buffer , lSize, 1 , fp) )
        fclose(fp),free(buffer),fputs("entire read fails",stderr),exit(1);

    /* do your work here, buffer is a string contains the whole text */

    fclose(fp);
    return buffer;
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

    /*RecordCompressor comp;
    comp.compress_dir("/tmp/sync","/tmp/records2.bin.gz");*/

    /*map_seq_function(
            create_params,
            argv[1],
            "/mnt/ramdisk/map_results",
            "function map(obj,yield){ yield(obj.userAgent); }"
    );*/



    char* config_json = read_file(argv[1]);
    Document config;

    config.Parse(config_json);

    assert(config.HasMember("aws"));

    const Aws::String access_key(config["aws"]["access_key"].GetString());
    const Aws::String secret_key(config["aws"]["secret_key"].GetString());

    Aws::SDKOptions options;
    Aws::InitAPI(options);

    Aws::Client::ClientConfiguration client_config;
    client_config.region = Aws::Region::US_EAST_1;
    client_config.connectTimeoutMs = 60000;
    client_config.requestTimeoutMs = 60000;

    const Aws::Auth::AWSCredentials creds(access_key, secret_key);
    Aws::SQS::SQSClient client(creds, client_config);


    const char* queue_url = config["task_sqs"]["url"].GetString();
    Aws::SQS::Model::ReceiveMessageRequest req;
    req.SetWaitTimeSeconds(20);
    req.SetMaxNumberOfMessages(1);
    req.SetQueueUrl(queue_url);

    while(true){
        auto response = client.ReceiveMessage(req);

        if(!response.IsSuccess()){
            printf("Error:%s\n", response.GetError().GetMessage().c_str());
            printf("Exception name:%s\n", response.GetError().GetExceptionName().c_str());
            exit(1);
        }
        auto msg_vector = response.GetResult().GetMessages();

        for(auto msg = msg_vector.begin() ; msg != msg_vector.end() ; msg ++){
            printf("Msg:%s\n", msg->GetBody().c_str());

            // Process message


            // Delete message from the queue
            Aws::SQS::Model::DeleteMessageRequest del_message;
            del_message.SetQueueUrl(queue_url);
            del_message.SetReceiptHandle(msg->GetReceiptHandle());

            auto res = client.DeleteMessage(del_message);

            if(!res.IsSuccess()){
                printf("Failed to delete message\n");
                printf("Error:%s\n", response.GetError().GetMessage().c_str());
                printf("Exception name:%s\n", response.GetError().GetExceptionName().c_str());
                exit(1);
            }
        }

        printf("messages: %ld\n", msg_vector.size());
    }



    Aws::ShutdownAPI(options);



    /*map_cassandra_function(
            create_params,
            "181239768896",
            2014,
            4,
            8,
            "/mnt/ramdisk/map_results",
            "function map(obj,yield){ yield(obj.userAgent); }"
    );*/



    /*KVStreamReader reader("/tmp/map_results/00000.map");

    while(reader.has_next()){
        char item[1000];

        if(reader.next_type() == Key) {
            printf("key:");
        } else {
            printf("value:");
        }

        reader.next(item);

        printf("%s\n", item);

    }*/
    /*{
        SortedMerge merger;
        SeqFileWriter writer("/tmp/merge");

        const char *files[3];
        files[0] = "/tmp/map_results/00000.map";
        files[1] = "/tmp/map_results/00001.map";
        files[2] = nullptr;


        merger.merge((char **) files, &writer);
    }*/
    /*{
        SeqWriter writer("/mnt/ramdisk/reduce_res");

        ReduceOperation r(create_params, &writer);

        KVStreamReader reader((char *) "/mnt/ramdisk/map_results/00000.map");

        r.reduce(&reader,
                 "function reduce(key, values_cb, yield){ var i = 0 ; values_cb(function(v){ i += 1; }); yield(key, i); }");
    }*/

    V8::Dispose();
    V8::ShutdownPlatform();
    //delete platform;
    return 0;
}