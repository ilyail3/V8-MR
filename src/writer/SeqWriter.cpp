//
// Created by ilya on 5/14/16.
//

#include "SeqWriter.h"
#include "SeqFileWriter.h"

#include <cstring>
#include <fcntl.h>
#include <sys/stat.h>
#include <dirent.h>

#include <unistd.h>

const char* null_string = "null";

void SeqWriter::flush() {
    if(data.size() > 0) {
        char flush_output[200] = "";
        // Get directory for output filename
        sprintf((char*)&flush_output, "%s/%05d.map", dir_name, file_index);

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


        printf("flushing to:%s %ld records\n", flush_output, records);
        SeqFileWriter write(flush_output);

        // Don't do that since it's hard to maintain when doing seek-less merges
        //write_short(file_handler, data.size());

        // Do the actual flushing here
        std::vector<char *>::iterator vector_id;
        for (std::map<char *, std::vector<char *>>::iterator it = data.begin(); it != data.end(); ++it) {
            write.write_key(it->first);

            for(vector_id = it->second.begin(); vector_id != it->second.end() ; ++vector_id)
                write.write_value(*vector_id);
        }
    }


    file_index ++;

    // Finally, clean offset to reuse the buffer and clean data
    offset = 0;
    data.clear();

    printf("Flush map\n");
}

char* SeqWriter::copy_string(char *string) {
    size_t len = strlen(string);
    char* location = (char*)buffer + offset;

    memcpy(location, string, len);
    location[len] = 0;

    // Advance the offset by length + 1
    offset += len + 1;

    return location;
}

void SeqWriter::write(char *key, char *value) {
    if(offset+strlen(key)+1 > BUFFER_SIZE)
        flush();

    records++;

    if(data.find(key) == data.end()) {
        // If doesn't exist create and copy key
        key = copy_string(key);
        data[key] = std::vector<char *>();
    }

    if(value == nullptr)
        data[key].push_back((char*)null_string);
    else
        data[key].push_back(copy_string(value));
}

SeqWriter::~SeqWriter() {
    flush();
    printf("MapWriterDispose\n");

    free(buffer);
}

SeqWriter::SeqWriter(const char *dir_name) {
    this->dir_name = dir_name;

    buffer = (char*)malloc(BUFFER_SIZE);
}