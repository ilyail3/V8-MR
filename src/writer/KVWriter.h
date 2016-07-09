//
// Created by ilya on 7/9/16.
//
#pragma once

class KVWriter{
public:
    virtual void write(char* key, char* value = nullptr) = 0;
};