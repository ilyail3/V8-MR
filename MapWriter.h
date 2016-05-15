//
// Created by ilya on 5/14/16.
//
#pragma once


#include <map>
#include <vector>

#define MAX_ITEM_SIZE 60000

struct cmp_str
{
    bool operator()(char const *a, char const *b)
    {
        return strcmp(a, b) < 0;
    }
};

void write_short(FILE* file_handler, size_t value);

void write_string(FILE* file_handler, char* string);

class MapWriter {
private:
    static const size_t BUFFER_SIZE = 1000000;
    char* buffer;
    size_t offset = 0;

    char* copy_string(char* string);
    void add_item(char* key, char* value);

    std::map<char*, std::vector<char*>, cmp_str> data;

    unsigned int file_index = 0;
    const char* dir_name;

public:
    void write_key(char* key);
    void write_key_value(char* key, char* value);
    void flush();

    ~MapWriter();
    MapWriter(const char* dir_name);
};
