#pragma once

#include <iostream>
#include <fstream>
#include <cstdint>
#include <string.h>

typedef uint32_t page_id_t;

#define PAGE_SIZE 4096 //4KB
#define INVALID_PAGE_ID -1
#define DIRECTORY_PAGE_ID 0


struct Page {
        char* data_[PAGE_SIZE]{};
        page_id_t page_id_ = INVALID_PAGE_ID;
};


class DiskManager {
    public:

        DiskManager(std::string main_db_file): main_db_file_(main_db_file){
            main_db_stream_.open(main_db_file_, std::ios::binary | std::ios::in | std::ios::out);
            if (!main_db_stream_.is_open()) {
                main_db_stream_.clear();
                main_db_stream_.open(main_db_file_, std::ios::binary | std::ios::trunc | std::ios::out | std::ios::in);
            }
        }
        ~DiskManager(){
            main_db_stream_.close();
        }

        // returns 1 in case of failure and 0 in case of success.
        int readPage(page_id_t page_id, char* ouput_buffer);
        int writePage(page_id_t page_id, char* input_buffer);
    private:
        std::fstream main_db_stream_;
        std::string main_db_file_;
};

int DiskManager::readPage(page_id_t page_id, char* output_buffer){
    int offset = page_id * PAGE_SIZE;

    main_db_stream_.seekp(offset);
    main_db_stream_.read(output_buffer, PAGE_SIZE);
    if (main_db_stream_.bad()) {
        std::cerr << "Read page error: bad file format" << std::endl;
        return 1;
    }

    int read_count = main_db_stream_.gcount();
    if (read_count < PAGE_SIZE) {
        std::cerr << "Read page error: invalid read count" << std::endl;
        main_db_stream_.clear();
        memset(output_buffer + read_count, 0, PAGE_SIZE - read_count);
        return 1;
    }
    return 0;
}


int DiskManager::writePage(page_id_t page_id, char* input_buffer){
    int offset = page_id * PAGE_SIZE;
    main_db_stream_.seekp(offset);
    main_db_stream_.write(input_buffer, PAGE_SIZE);
    if (main_db_stream_.bad()) {
        std::cerr << "I/O error while writing" << std::endl;
        return 1;
    }
    main_db_stream_.flush();
    return 0;
}

