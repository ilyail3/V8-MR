//
// Created by ilya on 7/10/16.
//

#pragma once


#include <fstream>

class SeqFileWriter {
public:
    SeqFileWriter(const char* filename);
    ~SeqFileWriter();

    void write_key(char* string);
    void write_value(char *string);

private:
    std::ofstream fh;
    bool first;

    void write_short(uint16_t value);
    void write_string(char* string);
};

