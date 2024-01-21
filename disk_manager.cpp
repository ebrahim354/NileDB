#pragma once

#include <iostream>
#include <fstream>
#include <cstdint>
#include <string.h>
#include <sys/types.h>
#include <unordered_map>


struct PageID{
    std::string file_name_{0};
    int32_t page_num_{0};
};


#define PAGE_SIZE 4096 //4KB
#define FILE_EXT ".NDB"
const PageID INVALID_PAGE_ID = PageID{ .file_name_ = "INVALID_FILE_NAME", .page_num_ = -1 };




struct Page {
        char* data_[PAGE_SIZE]{};
        PageID page_id_ = INVALID_PAGE_ID;
};


class DiskManager {
    public:
        DiskManager();
        ~DiskManager(){
            for(auto &file : cached_files_){
                file.second.close();
            }
        }
        // 1 on failure, 0 on success.
        int openFile(std::string file_name);
        // returns 1 in case of failure and 0 in case of success.
        int readPage(PageID page_id, char* ouput_buffer);
        int writePage(PageID page_id, char* input_buffer);
        
    private:
        std::unordered_map<std::string, std::fstream> cached_files_;
};


int DiskManager::readPage(PageID page_id, char* output_buffer) {
    std::string file_name =  page_id.file_name_;
    uint32_t page_num = page_id.page_num_;
    int offset = page_num * PAGE_SIZE;
    int err = openFile(file_name);
    if(err) return 1;
    auto file_stream = &cached_files_[file_name];

    file_stream->seekp(offset);
    file_stream->read(output_buffer, PAGE_SIZE);

    int read_count = file_stream->gcount();
    if (read_count < PAGE_SIZE) {
        std::cerr << "Read page error: invalid read count" << std::endl;
        memset(output_buffer + read_count, 0, PAGE_SIZE - read_count);
        return 1;
    }
    return 0;
}

int DiskManager::writePage(PageID page_id, char* input_buffer) {
    std::string file_name =  page_id.file_name_;
    uint32_t page_num = page_id.page_num_;
    int offset = page_num * PAGE_SIZE;
    int err = openFile(file_name);
    if(err) return 1;

    auto file_stream = &cached_files_[file_name];
    file_stream->seekp(offset);
    file_stream->write(input_buffer, PAGE_SIZE);
    if (file_stream->bad()) {
        std::cerr << "I/O error while writing" << std::endl;
        return 1;
    }
    file_stream->flush();
    return 0;
}

int DiskManager::openFile(std::string file_name){
    // bad file format.
    std::string::size_type n = file_name.rfind(FILE_EXT);
    if (n == std::string::npos) 
        return 1;
    // cache miss
    if (!cached_files_.count(file_name)) 
        cached_files_[file_name] = std::fstream(file_name, std::ios::binary | std::ios::app | std::ios::out | std::ios::in);
    // file doesn't exist.
    // create a new one and return 1 on failure.
    if(!cached_files_[file_name].is_open()){
        cached_files_[file_name].open(file_name, std::ios::binary | std::ios::trunc | std::ios::out | std::ios::in);
        if (cached_files_[file_name].is_open()) {
            cached_files_.erase(file_name);
            return 1;
        }
         
    }
    return 0;
}



