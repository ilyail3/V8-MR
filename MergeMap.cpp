//
// Created by ilya on 5/15/16.
//

#include <dirent.h>
#include <cstring>
#include <cstdio>
#include <unistd.h>
#include <cstdint>
#include <cstdlib>
#include "OutputWriter.h"

struct dirent* readreal(DIR* dir_handler){
    struct dirent *dp;
    while ((dp=readdir(dir_handler)) != NULL && (strcmp(dp->d_name, "..") == 0 || strcmp(dp->d_name, ".") == 0)) {
    }

    return dp;
}

void read_string(FILE* file_handler, char* output){
    uint16_t string_size;

    if(fread(&string_size, sizeof(uint16_t), 1, file_handler) == 0 || string_size == 0){
        output[0] = 0;
        return;
    }

    if(fread(output, sizeof(char), string_size, file_handler) != string_size){
        perror("file corrupt");
        exit(1);
    }

    output[string_size] = 0;
}


void merge_files(FILE* output, FILE* input1, FILE* input2){
    char input_key1[MAX_ITEM_SIZE];
    char input_key2[MAX_ITEM_SIZE];
    char member[MAX_ITEM_SIZE];

    read_string(input1, (char *)&input_key1);
    read_string(input2, (char *)&input_key2);
    int compare_result;

    while(strlen(input_key1) + strlen(input_key2) > 0){
        compare_result = strcmp(input_key1, input_key2);

        // input 1 is first, use it
        if(strlen(input_key2) == 0 || compare_result < 0){
            write_string(output, input_key1);

            read_string(input1, (char*)&member);

            while(strlen(member) > 0){
                write_string(output, member);
                read_string(input1, (char*)&member);
            }

            write_short(output, 0);

            // Read the next key for this input
            read_string(input1, (char*)input_key1);
        } else if(strlen(input_key1) == 0 || compare_result > 1){
            write_string(output, input_key2);

            read_string(input2, (char*)&member);

            while(strlen(member) > 0){
                write_string(output, member);
                read_string(input2, (char*)&member);
            }

            write_short(output, 0);

            // Read the next key for this input
            read_string(input2, (char*)input_key2);
        } else {
            // Write input_key1(they are going to be equal in that case
            write_string(output, input_key1);

            read_string(input1, (char*)&member);

            while(strlen(member) > 0){
                write_string(output, member);
                read_string(input1, (char*)&member);
            }

            read_string(input2, (char*)&member);

            while(strlen(member) > 0){
                write_string(output, member);
                read_string(input2, (char*)&member);
            }

            write_short(output, 0);

            // Read the next key for both inputs
            read_string(input1, (char*)input_key1);
            read_string(input2, (char*)input_key2);
        }

    }
}

void merge_inputs(const char* dir_name, char* map_output){
    DIR* dir_handler = opendir(dir_name);

    struct dirent *dp = readreal(dir_handler);
    // get the firs member of any pair
    char* last_file = dp->d_name;
    int level = 0;
    char output_filename[300];
    char input1_filename[300], input2_filename[300];

    dp = readreal(dir_handler);
    while(dp != NULL){
        int index = 0;

        while(dp != NULL) {
            sprintf((char*)&output_filename, "%05d_%05d.map", level, index);

            printf("Merge %s, %s into %s\n", last_file, dp->d_name, output_filename);

            sprintf((char*)&output_filename, "%s/%05d_%05d.map", dir_name, level, index);

            FILE *output = fopen(output_filename, "w");

            sprintf((char*)&input1_filename, "%s/%s", dir_name, last_file);
            FILE *input1 = fopen(input1_filename, "r");

            sprintf((char*)&input2_filename, "%s/%s", dir_name, dp->d_name);
            FILE *input2 = fopen(input2_filename, "r");

            merge_files(output, input1, input2);

            fclose(output); fclose(input1); fclose(input2);
            unlink(input1_filename); unlink(input2_filename);

            // Read the next pair of files
            dp = readreal(dir_handler);

            if(dp != NULL){
                last_file = dp->d_name;
            }

            dp = readreal(dir_handler);

            index++;
        }

        closedir(dir_handler);
        dir_handler = opendir(dir_name);
        dp = readreal(dir_handler);
        last_file = dp->d_name;
        dp = readreal(dir_handler);
        level++;
    }

    strcpy(map_output, last_file);

    closedir(dir_handler);
}