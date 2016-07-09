#pragma once

typedef void(*record_callback)(void*, const char* str, int len);

class RecordsReader{
public:
    virtual int get_records(void* ref, record_callback callback) = 0;
};
