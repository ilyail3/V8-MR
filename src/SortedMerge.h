//
// Created by ilya on 7/10/16.
//
#pragma once

#include "writer/KVWriter.h"
#include "writer/SeqFileWriter.h"

class SortedMerge {
public:
    void merge(char** file, SeqFileWriter* writer);
};



