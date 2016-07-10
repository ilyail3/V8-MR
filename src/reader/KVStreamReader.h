//
// Created by ilya on 7/10/16.
//
#pragma once

#include <fstream>
#include "../consts.h"

enum NextType{
    TypeKey,
    TypeValue
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

    char buffer[MAX_VAL_SIZE];

    std::ifstream fh;

    void read_next();
};

