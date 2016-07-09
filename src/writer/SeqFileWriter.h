//
// Created by ilya on 5/14/16.
//
#pragma once


#include <map>
#include <vector>
#include <cstring>
#include "KVWriter.h"

#define MAX_ITEM_SIZE 60000

struct cmp_str
{
    bool operator()(char const *a, char const *b)
    {
        return strcmp(a, b) < 0;
    }
};

class SeqFileWriter : public KVWriter {
private:
    static const size_t BUFFER_SIZE = 1000000;
    char* buffer;
    size_t offset = 0;

    char* copy_string(char* string);

    std::map<char*, std::vector<char*>, cmp_str> data;
    unsigned int file_index = 0;

    const char* dir_name;

    void flush();

public:
    virtual void write(char* key, char* value = nullptr);


    ~SeqFileWriter();
    SeqFileWriter(const char* dir_name);
};
