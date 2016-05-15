//
// Created by ilya on 5/14/16.
//

#include <cstring>
#include <fcntl.h>
#include <sys/stat.h>
#include <dirent.h>
#include "OutputWriter.h"
#include <unistd.h>

const char* null_string = "null";
const char* vector_term = "\0";

void write_short(FILE* file_handler, size_t value){
    uint16_t real_size = (uint16_t)value;
    fwrite(&real_size, sizeof(uint16_t), 1, file_handler);
}

void write_string(FILE* file_handler, char* string){
    write_short(file_handler, strlen(string));
    fwrite(string, sizeof(char), strlen(string), file_handler);
}

void OutputWriter::flush() {

    char flush_output[200] = "";
    // Get directory for output filename
    sprintf((char*)&flush_output, "%s/%05d.map", dir_name, file_index);


    if(data.size() > 0) {
        // If first file, make sure the directory exists as well
        if(file_index == 0){
            struct stat s;
            int error = stat(dir_name, &s);

            if(error == -1){
                if(ENOENT == errno) {
                    mkdir(dir_name, S_IRWXU);
                } else {
                    perror("stat");
                    exit(1);
                }
            } else {
                // Clean all the files
                DIR* dir = opendir(dir_name);
                dirent* rd;

                char filename[500];

                while((rd = readdir(dir)) != NULL){
                    if(strcmp(rd->d_name, "..") != 0 && strcmp(rd->d_name, ".") != 0){
                        sprintf(filename, "%s/%s", dir_name, rd->d_name);
                        unlink(filename);
                    }
                }

                closedir(dir);
            }
        }


        printf("flushing to:%s\n", flush_output);
        FILE *file_handler = fopen(flush_output, "w");

        if (file_handler == NULL) {
            perror("flush file");
            exit(1);
        }

        // Don't do that since it's hard to maintain when doing seek-less merges
        //write_short(file_handler, data.size());

        // Do the actual flushing here
        std::vector<char *>::iterator vector_id;
        for (std::map<char *, std::vector<char *>>::iterator it = data.begin(); it != data.end(); ++it) {
            write_string(file_handler, it->first);

            for(vector_id = it->second.begin(); vector_id != it->second.end() ; ++vector_id)
                write_string(file_handler, *vector_id);

            write_short(file_handler, 0);
        }

        fclose(file_handler);
    }


    file_index ++;

    // Finally, clean offset to reuse the buffer and clean data
    offset = 0;
    data.clear();

    printf("Flush map\n");
}

char* OutputWriter::copy_string(char *string) {
    size_t len = strlen(string);
    char* location = (char*)buffer + offset;

    memcpy(location, string, len);
    location[len] = 0;

    // Advance the offset by length + 1
    offset += len + 1;

    return location;
}

void OutputWriter::add_item(char *key, char *value) {
    if(data.find(key) == data.end())
        data[key] = std::vector<char*>();

    data[key].push_back(value);
}

void OutputWriter::write_key(char *key) {
    if(offset+strlen(key)+1 > BUFFER_SIZE)
        flush();

    add_item(copy_string(key), (char*)null_string);

}

void OutputWriter::write_key_value(char *key, char *value) {
    if(offset+strlen(key)+strlen(value)+2 > BUFFER_SIZE)
        flush();

    char* key_copy = copy_string(key);
    char* value_copy = copy_string(value);
    add_item(key_copy, value_copy);
}

OutputWriter::~OutputWriter() {
    flush();
    printf("MapWriterDispose\n");

    free(buffer);
}

OutputWriter::OutputWriter(const char *dir_name) {
    this->dir_name = dir_name;

    buffer = (char*)malloc(BUFFER_SIZE);
}