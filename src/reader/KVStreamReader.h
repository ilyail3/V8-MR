//
// Created by ilya on 7/10/16.
//
#pragma once

#include <fstream>

enum NextType{
    Key,
    Value
};

class KVStreamReader {
public:
    KVStreamReader(char* filename);
    ~KVStreamReader();

    bool has_next();
    NextType next_type();

    int next(char* str);
    char* peek();
private:
    NextType _next_type;
    bool _next_key;
    bool _has_next;
    unsigned short _next_size;

    char buffer[10000];

    std::ifstream fh;

    void read_next();
};

