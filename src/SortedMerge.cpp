//
// Created by ilya on 7/10/16.
//

#include <vector>
#include <cstring>
#include "SortedMerge.h"
#include "reader/KVStreamReader.h"

void SortedMerge::merge(char **file, SeqFileWriter *writer) {
    std::vector<KVStreamReader*> readers;
    char item[10000];
    char key[10000];

    for(int i = 0 ; file[i] != nullptr ; i++){
        readers.push_back(new KVStreamReader(file[i]));
    }



    while(true){
        char* smallest = nullptr;


        for(auto it = readers.begin() ; it != readers.end() ; it++){
            if((*it)->has_next()){
                if(smallest == nullptr)
                    smallest = (*it)->peek();
                else if(strcmp(smallest, (*it)->peek()) > 0)
                    smallest = (*it)->peek();

            }
        }

        if(smallest == nullptr) break;
        strcpy(key, smallest);
        writer->write_key(key);

        for(auto it = readers.begin() ; it != readers.end() ; it++) {
            if ((*it)->has_next() && strcmp(key, (*it)->peek()) == 0) {
                (*it)->next(item);

                while((*it)->has_next() && (*it)->next_type() == Value){
                    (*it)->next(item);

                    writer->write_value(item);
                }


            }
        }
    }

    for(auto it = readers.begin() ; it != readers.end() ; it++) {
        delete *it;
    }
}