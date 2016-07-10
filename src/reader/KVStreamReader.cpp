//
// Created by ilya on 7/10/16.
//

#include <cstring>
#include "KVStreamReader.h"

KVStreamReader::KVStreamReader(char *filename) : fh(filename) {
    _next_key = true;
    _has_next = true;
    read_next();
}

KVStreamReader::~KVStreamReader() {
    fh.close();
}

void KVStreamReader::read_next() {
    if(!_next_key) {
        fh.read(buffer, 2);

        if(*(unsigned short*)buffer == 0){
            _next_key = true;
            read_next();
            return;
        }

    } else {
        fh.read(buffer, 2);

        if(!fh){
            _has_next = false;
            return;
        }
    }

    _next_size = *(unsigned short*)buffer;
    fh.read(buffer, _next_size);
    buffer[_next_size] = 0;

    if(_next_key){
        _next_type = TypeKey;
        _next_key = false;
    } else
        _next_type = TypeValue;

    _has_next = true;
}

bool KVStreamReader::has_next() {
    return _has_next;
}

int KVStreamReader::next(char *str) {
    memcpy(str, buffer, _next_size);
    str[_next_size] = 0;
    unsigned short ret_next_size = _next_size;

    read_next();

    return ret_next_size;
}

char* KVStreamReader::peek() {
    return buffer;
}

NextType KVStreamReader::next_type() {
    return _next_type;
}