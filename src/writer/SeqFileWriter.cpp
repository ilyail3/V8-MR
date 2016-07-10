//
// Created by ilya on 7/10/16.
//

#include <cstring>
#include "SeqFileWriter.h"

SeqFileWriter::SeqFileWriter(const char* filename) : fh(filename, std::ios::out | std::ios::binary) {
    first = true;
}

SeqFileWriter::~SeqFileWriter() {
    fh.close();
}

void SeqFileWriter::write_key(char *string) {
    if(first)
        first = false;
    else
        write_short(0);

    write_string(string);
}

void SeqFileWriter::write_value(char *string) {
    write_string(string);
}

void SeqFileWriter::write_short(uint16_t value) {
    fh.write((char*)&value, 2);
}

void SeqFileWriter::write_string(char *string) {
    auto len = strlen(string);
    write_short((uint16_t)len);
    fh.write(string, len);
}