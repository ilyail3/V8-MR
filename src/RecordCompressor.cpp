//
// Created by ilya on 7/11/16.
//

#include <boost/filesystem/operations.hpp>
#include <zlib.h>
#include "RecordCompressor.h"
#include "reader/CloudTrailReader.h"

void write_reocrd(void* ref, const char* str, int len){
    gzFile f = gzFile(ref);
    uint16_t size = (uint16_t)len;

    gzwrite(f, &size, 2);
    gzwrite(f, str, len);
}

void RecordCompressor::compress_dir(const char *dir, const char *output_file) {
    boost::filesystem::recursive_directory_iterator itr(dir);
    gzFile f = gzopen(output_file, "wb");

    while (itr != boost::filesystem::recursive_directory_iterator())
    {

        if(is_regular_file(itr->path())){
            //std::cout << itr->path().string() << std::endl;

            CloudTrailReader reader(itr->path().c_str());

            reader.get_records(f, write_reocrd);
        }

        ++itr;
    }

    gzclose(f);
}