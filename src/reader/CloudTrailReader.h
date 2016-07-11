//
// Created by ilya on 7/9/16.
//
#pragma once
#include "RecordsReader.h"


class CloudTrailReader : public RecordsReader {
public:
    CloudTrailReader(const char* filename);

    virtual int get_records(void* ref, record_callback callback);
private:
    const char* filename;

    void process_plain(const char* filename, void *ref, record_callback callback);
    void process_gzip(const char* filename, void *ref, record_callback callback);
};