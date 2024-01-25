#pragma once

#include <iostream>
#include <fstream>
#include <cstdint>
#include <string.h>
#include <sys/types.h>
#include <unordered_map>
#include "page.cpp"




// meta data for managing files:
// num_of_pages_ >= 1, there is always at least one meta page on a file.
// freelist_ptr_ = first 4 bytes.
// num_of_pages_ = second 4 bytes.
struct FileMeta {
    std::fstream fs_;
    int freelist_ptr_;   
    int num_of_pages_;
};


class DiskManager {
    public:
        DiskManager();
        ~DiskManager();
        // returns 1 in case of failure and 0 in case of success.
        int readPage(PageID page_id, char* ouput_buffer);
        int writePage(PageID page_id, char* input_buffer);

        // page_id is the output and return value 1 in case of failure.
        int allocateNewPage(std::string file_name, char* buffer ,PageID *page_id);
        int deallocatePage(PageID page_id);

    private:
        // 1 on failure, 0 on success.
        int openFile(std::string file_name);
        // first 4 bytes of a file indicates the next free page number.
        // second 4 bytes of a file indicates the number of pages on a file. 
        // in case of value of 0 means now current free pages
        // append to the end of the file for new pages
        std::unordered_map<std::string, FileMeta> cached_files_;
};


DiskManager::~DiskManager(){
    for(auto &file : cached_files_){
        // need to write the changes of free list pointer and number of pages before closing.
        // will be changed by adding fault handling.
        char bytes[8]; 
        for(int i = 0; i < 4; ++i){
            bytes[3 - i] = (file.second.freelist_ptr_ >> (i * 8));
        }
        for(int i = 0; i < 4; ++i){
            bytes[7 - i] = (file.second.num_of_pages_ >> (i * 8));
        }
        file.second.fs_.write(bytes, sizeof(int) * 2);
        file.second.fs_.close();
    }
}

int DiskManager::deallocatePage(PageID page_id) {
    auto file_name = page_id.file_name_;
    auto page_num = page_id.page_num_;

    int err = openFile(file_name);
    if(err) return 1;

    auto file_stream = &cached_files_[file_name].fs_;
    int  cur_ptr = cached_files_[file_name].freelist_ptr_;
    char* bytes = static_cast<char*>(static_cast<void*>(&cur_ptr));


    file_stream->seekp(page_num * PAGE_SIZE);
    file_stream->write(bytes, sizeof(int));

    if (file_stream->bad()) {
        std::cerr << "I/O error while writing" << std::endl;
        return 1;
    }

    // freelist_ptr_ value on the disk will be updated by the destructor.(fault handling might change this).
    cached_files_[file_name].freelist_ptr_ = page_num;
    file_stream->flush();
    return 0; 
}


int DiskManager::allocateNewPage(std::string file_name, char* buffer , PageID *page_id){
    // file_name is a param to make the usage of function more clear, we can provide it inside page_id
    // but it's better to separate input from output.
    int err = openFile(file_name);
    if(err) return 1;
    page_id->file_name_ = file_name;
    auto file_stream = &cached_files_[file_name].fs_;
    int next_free_page = cached_files_[file_name].freelist_ptr_;
    int offset_to_eof = (cached_files_[file_name].num_of_pages_) * PAGE_SIZE;
    // no free pages => append to the end.
    if(next_free_page == 0){
        file_stream->seekp(offset_to_eof);
        file_stream->write(buffer, PAGE_SIZE);

        if (file_stream->bad()) {
            std::cerr << "I/O error while writing" << std::endl;
            return 1;
        }
        page_id->page_num_ = offset_to_eof;
        // num_of_pages_ value on the disk will be updated by the destructor
        // (adding fault and log handling might change this).
        cached_files_[file_name].num_of_pages_++;
    } else {
        char bytes[sizeof(int)]; 
        int next;
        // seek to start of page -> read first 4 bytes
        // -> seek back to the start -> write the buffer (slow for now but should work 
        // maybe replaced with a freelist array at the meta page instead).
        int next_free_offset = next_free_page * PAGE_SIZE;
        file_stream->seekp(next_free_offset);
        file_stream->read(bytes, sizeof(bytes));

        int read_count = file_stream->gcount();
        if (read_count < sizeof(bytes)) {
            std::cerr << "Read page error: invalid read count" << std::endl;
            return 1;
        }

        file_stream->seekp(next_free_offset);
        file_stream->write(buffer, PAGE_SIZE);

        if (file_stream->bad()) {
            std::cerr << "I/O error while writing" << std::endl;
            return 1;
        }
        page_id->page_num_ = next_free_page;

        memcpy(&next, bytes, sizeof(int));
        // freelist_ptr_ value on the disk will be updated by the destructor.(fault handling might change this).
        cached_files_[file_name].freelist_ptr_ = next;
    }
    file_stream->flush();
    return 0;
}


int DiskManager::readPage(PageID page_id, char* output_buffer) {
    std::string file_name =  page_id.file_name_;
    uint32_t page_num = page_id.page_num_;
    int offset = page_num * PAGE_SIZE;
    int open_err = openFile(file_name);
    if(open_err) return 1;
    auto file_stream = &cached_files_[file_name].fs_;

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
    int open_err = openFile(file_name);
    if(open_err) return 1;

    auto file_stream = &cached_files_[file_name].fs_;
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
    // open the file
    if (!cached_files_.count(file_name)) {
        cached_files_[file_name] = {
            .fs_ = std::fstream (file_name, std::ios::binary | std::ios::out | std::ios::in),
            .freelist_ptr_ = 0,
            .num_of_pages_ = 1,
        };
    }
    // file doesn't exist.
    // create a new one and return 1 on failure.
    if(!cached_files_[file_name].fs_.is_open()){
        cached_files_[file_name].fs_.open(file_name, std::ios::binary | std::ios::out | std::ios::in);
        cached_files_[file_name].fs_.clear();
        if (!cached_files_[file_name].fs_.is_open()) {
            cached_files_.erase(file_name);
            return 1;
        }

        char first_page[PAGE_SIZE]{};
        // assigning second int to 1;
        first_page[0+sizeof(int)] = first_page[1+sizeof(int)] =   first_page[2+sizeof(int)] = 0x0;
        first_page[2+sizeof(int)] = 0x01;
        // write first page with zeros.
        // this is kind of expensive but happens when creating tables only.
        cached_files_[file_name].fs_.write(first_page, PAGE_SIZE);

        if (cached_files_[file_name].fs_.bad()) {
            std::cerr << "I/O error while writing" << std::endl;
            cached_files_.erase(file_name);
            return 1;
        }
        cached_files_[file_name].freelist_ptr_= 0;
        cached_files_[file_name].num_of_pages_= 1;
        return 0;
    }
    // at this point we need to get the next free page of this file.
    // read the first and secocnd 4 bytes (sizeof int) then put them into the cache.
    char bytes[sizeof(int) * 2]; 
    cached_files_[file_name].fs_.read(bytes, sizeof(bytes));
    int read_count = cached_files_[file_name].fs_.gcount();
    if (read_count < PAGE_SIZE) {
        std::cerr << "Read page error: invalid read count" << std::endl;
        return 1;
    }
    memcpy(&cached_files_[file_name].freelist_ptr_, bytes,  sizeof(int));
    memcpy(&cached_files_[file_name].num_of_pages_, bytes + sizeof(int), sizeof(int));

    return 0;
}



