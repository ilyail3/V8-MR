//
// Created by ilya on 5/15/16.
//

#include <cstring>
#include "OutputWriter.h"
#include "MergeMap.h"

void pretty_print(char* input_seq_file, const char* output_file){
    char item[MAX_ITEM_SIZE];

    FILE *input = fopen(input_seq_file, "r");
    FILE *output = fopen(output_file, "w");

    fprintf(output, "[");

    read_string(input, item);
    bool first_key = true;

    while(strlen(item) != 0){
        if(first_key)
            first_key = false;
        else {
            fprintf(output, ",\n");
        }

        fprintf(output, "[%s,[", item);

        bool first_value = true;

        read_string(input, item);
        while(strlen(item) != 0){
            if(first_value)
                first_value = false;
            else {
                fprintf(output, ",");
            }

            fprintf(output, "%s", item);

            read_string(input, item);
        }

        fprintf(output, "]]");
        read_string(input, item);
    }

    fprintf(output, "]");

    fclose(input);
    fclose(output);
}