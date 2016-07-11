//
// Created by ilya on 7/9/16.
//

#include "CloudTrailReader.h"

#include <iostream>
#include <zlib.h>
#include "rapidjson/reader.h"
#include "rapidjson/filereadstream.h"
#include "rapidjson/writer.h"
#include "../consts.h"


using namespace std;
using namespace rapidjson;

struct MyHandler {
    bool records_start = false;
    int array_depth = -1;
    int object_depth = 0;

    void* ref;
    record_callback rcallback;


    StringBuffer s;
    Writer<StringBuffer> writer;

    MyHandler(){
        writer.Reset(s);
    }

    bool rel(){
        return records_start && array_depth >= 0 && object_depth >= 0;
    }

    bool Null() {
        if(rel())
            writer.Null();


        return true;
    }
    bool Bool(bool b) {
        if(rel())
            writer.Bool(b);


        return true;
    }
    bool Int(int i) {
        if(rel())
            writer.Int(i);


        return true;
    }
    bool Uint(unsigned u) {
        if(rel())
            writer.Uint(u);


        return true;
    }
    bool Int64(int64_t i) {
        if(rel())
            writer.Int64(i);


        return true;
    }
    bool Uint64(uint64_t u) {
        if(rel())
            writer.Uint64(u);


        return true;
    }
    bool Double(double d) {
        if(rel())
            writer.Double(d);

        return true;
    }
    bool RawNumber(const char* str, SizeType length, bool copy) {
        if(rel())
            writer.RawNumber(str, length, copy);

        return true;
    }
    bool String(const char* str, SizeType length, bool copy) {
        if(rel())
            writer.String(str, length, copy);

        return true;
    }
    bool StartObject() {
        if(records_start) {
            if(rel())
                writer.StartObject();

            object_depth++;
        }

        return true;
    }
    bool Key(const char* str, SizeType length, bool copy) {
        //cout << "Key(" << str << ", " << length << ", " << boolalpha << copy << ")" << endl;
        //return true;

        if(!records_start){
            if(length == 7 && memcmp(str, "Records", 7) == 0)
                records_start = true;
        } else if(rel()) {
            writer.Key(str, length, copy);
        }

        return true;
    }
    bool EndObject(SizeType memberCount) {
        if(records_start) {
            object_depth--;

            if(rel())
                writer.EndObject(memberCount);

            //cout << "obj: " << object_depth << " arr:" << array_depth << endl;
            if(object_depth == 0 and array_depth == 0) {
                //cout << endl << "NEW OBJECT" << endl << endl;
                ObjectComplete();
            }
        }

        return true;
    }
    bool StartArray() {
        if(records_start) {
            if(rel())
                writer.StartArray();

            array_depth++;
        }

        return true;
    }
    bool EndArray(SizeType elementCount) {
        if(records_start) {
            array_depth--;

            if(rel())
                writer.EndArray(elementCount);
        }

        return true;
    }

    void ObjectComplete(){
        //cout << s.GetString() << endl;
        std::string object = s.GetString();

        // don't expect size to be bigger than int, so cast not a problem
        rcallback(ref, object.c_str(), (int)object.size());

        s.Clear();
        // Reset is required after Clear, since we're adopting a new root
        writer.Reset(s);
    }

    void Close(){
        if(s.GetSize() > 0)
            ObjectComplete();
    }
};

CloudTrailReader::CloudTrailReader(const char *filename) {
    this->filename = filename;
}



int CloudTrailReader::get_records(void *ref, record_callback callback) {
    // Try to de-compress in memory
    auto filename_len = strlen(filename);

    if(filename_len > 5 && strcmp(filename + filename_len - 5, ".json") == 0)
        process_plain(filename, ref, callback);
    else if(filename_len > 3 && strcmp(filename + filename_len - 3, ".gz") == 0)
        process_gzip(filename, ref, callback);

    return 0;
};

void CloudTrailReader::process_plain(const char *filename, void *ref, record_callback callback) {
    MyHandler handler;

    handler.ref = ref;
    handler.rcallback = callback;

    FILE* fh = fopen(filename,"r");
    char buffer[MAX_VAL_SIZE];

    FileReadStream readStream(fh, buffer, MAX_VAL_SIZE);
    Reader reader;

    reader.Parse(readStream, handler);
    handler.Close();

    fclose(fh);
}

void CloudTrailReader::process_gzip(const char *filename, void *ref, record_callback callback) {
    char buf[8000];

    gzFile fi = gzopen(filename,"rb");
    if (fi == NULL) {
        printf("Error: Failed to gzopen %s\n", filename);
        exit(0);
    }

    gzrewind(fi);

    char* tmpname = tempnam(nullptr, "v8mr");
    FILE* fh = fopen(tmpname, "wb");

    while(!gzeof(fi))
    {
        int len = gzread(fi,buf,sizeof(buf));
        fwrite(buf, len, 1, fh);
        //buf contains len bytes of decompressed data
    }
    gzclose(fi);
    fclose(fh);

    //printf("Extracted file to:%s\n", tmpname);

    process_plain(tmpname, ref, callback);
    unlink(tmpname);
}
