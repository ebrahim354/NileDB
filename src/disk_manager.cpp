#pragma once

#include <iostream>
#include <fstream>
#include <cstdint>
#include "string.h"
#include <thread>
#include <unordered_map>
#include <cassert>
#include "page.cpp"
#include "disk_manager.h"


DiskManager::DiskManager(){}
DiskManager::~DiskManager(){
    for(auto &file : cached_files_){
        // need to write the changes of free list pointer and number of pages before closing.
        // will be changed by adding fault handling.
        char bytes[8];
        memcpy(bytes, &file.second.freelist_ptr_, sizeof(int));
        memcpy(bytes+sizeof(int), &file.second.num_of_pages_, sizeof(int));
        file.second.fs_.seekp(0);
        file.second.fs_.write(bytes, sizeof(int) * 2);
        file.second.fs_.flush();
        file.second.fs_.close();
    }
}

int DiskManager::deallocatePage(PageID page_id) {
    assert(fid_to_fname.count(page_id.fid_) != 0); // TODO: replace assertion with an error message.
    auto file_name = fid_to_fname[page_id.fid_];
    auto page_num = page_id.page_num_;

    int err = openFile(file_name);
    if(err) return 1;

    auto file_stream = &cached_files_[file_name].fs_;
    int  cur_ptr = cached_files_[file_name].freelist_ptr_;
    char* bytes = static_cast<char*>(static_cast<void*>(&cur_ptr));


    file_stream->seekp(page_num * PAGE_SIZE);
    file_stream->write(bytes, sizeof(int));
    file_stream->flush();

    if (file_stream->bad()) {
        std::cerr << "I/O error while writing" << std::endl;
        return 1;
    }

    // TODO: clean this up
    cached_files_[file_name].freelist_ptr_ = page_num;
    {
        auto file = &cached_files_[file_name];
        char bytes[8];
        memcpy(bytes, &file->freelist_ptr_, sizeof(int));
        memcpy(bytes+sizeof(int), &file->num_of_pages_, sizeof(int));
        file->fs_.seekp(0);
        file->fs_.write(bytes, sizeof(int) * 2);
        file_stream->flush();
    }
    return 0; 
}


// fid is a param to make the usage of function more clear, we can provide it inside page_id
// but it's clearer to separate input from output.
int DiskManager::allocateNewPage(FileID fid, char* buffer , PageID *page_id){
  assert(fid_to_fname.count(fid) != 0); // TODO: replace assertion with an error message.
    auto file_name = fid_to_fname[fid];

    int err = openFile(file_name);
    if(err) return 1;
    page_id->fid_ = fid;
    auto file_stream = &cached_files_[file_name].fs_;
    int next_free_page = cached_files_[file_name].freelist_ptr_;
    int offset_to_eof = (cached_files_[file_name].num_of_pages_) * PAGE_SIZE;
    // when allocting a new page it can't be page 0.
    assert(offset_to_eof != 0 && "allocating page 0 is no possible"); 
    // no free pages => append to the end.
    if(next_free_page == 0){
        file_stream->seekp(offset_to_eof);
        file_stream->write(buffer, PAGE_SIZE);
        file_stream->flush();

        if (file_stream->bad()) {
            std::cerr << "I/O error while writing" << std::endl;
            return 1;
        }
        page_id->page_num_ = offset_to_eof / PAGE_SIZE;
        // num_of_pages_ value on the disk will be updated by the destructor
        // (adding fault and log handling might change this).

        cached_files_[file_name].num_of_pages_++;
        char bytes[8];
        memcpy(bytes, &cached_files_[file_name].freelist_ptr_, sizeof(int));
        memcpy(bytes+sizeof(int), &cached_files_[file_name].num_of_pages_, sizeof(int));
        file_stream->seekp(0);
        file_stream->write(bytes, sizeof(int) * 2);
        file_stream->flush();
    } else {
        char bytes[4];
        int next;
        // seek to start of page -> read first 4 bytes
        // -> seek back to the start -> write the buffer (slow for now but should work 
        // maybe replaced with a freelist array at the meta page instead).
        int next_free_offset = next_free_page * PAGE_SIZE;
        file_stream->seekp(next_free_offset);
        file_stream->read(bytes, 4);

        uint32_t read_count = file_stream->gcount();
        if (read_count < sizeof(bytes)) {
            std::cerr << "allocate new page error: invalid read count" << std::endl;
            return 1;
        }

        file_stream->seekp(next_free_offset);
        file_stream->write(buffer, PAGE_SIZE);
        file_stream->flush();

        if (file_stream->bad()) {
            std::cerr << "I/O error while writing" << std::endl;
            return 1;
        }
        page_id->page_num_ = next_free_page;

        memcpy(&next, bytes, sizeof(int));
        // freelist_ptr_ value on the disk will be updated by the destructor.(fault handling might change this).
        // TODO: clean this up
        cached_files_[file_name].freelist_ptr_ = next;
        {
            char bytes[8];
            memcpy(bytes, &cached_files_[file_name].freelist_ptr_, sizeof(int));
            memcpy(bytes+sizeof(int), &cached_files_[file_name].num_of_pages_, sizeof(int));
            file_stream->seekp(0);
            file_stream->write(bytes, sizeof(int) * 2);
            file_stream->flush();
        }
    }
    file_stream->flush();
    return 0;
}


int DiskManager::readPage(PageID page_id, char* output_buffer) {
  assert(fid_to_fname.count(page_id.fid_) != 0); // TODO: replace assertion with an error message.
    auto file_name = fid_to_fname[page_id.fid_];
    uint32_t page_num = page_id.page_num_;
    int offset = page_num * PAGE_SIZE;
    int open_err = openFile(file_name);
    if(open_err) return 1;
    auto file_stream = &cached_files_[file_name].fs_;

    file_stream->seekp(offset);
    file_stream->read(output_buffer, PAGE_SIZE);

    int read_count = file_stream->gcount();
    if (read_count < PAGE_SIZE) {
        file_stream->clear();
        memset(output_buffer + read_count, 0, PAGE_SIZE - read_count);
        return 1;
    }
    return 0;
}

int DiskManager::writePage(PageID page_id, char* input_buffer) {
    assert(fid_to_fname.count(page_id.fid_) != 0); // TODO: replace assertion with an error message.
    auto file_name = fid_to_fname[page_id.fid_];
    uint32_t page_num = page_id.page_num_;
    int offset = page_num * PAGE_SIZE;
    int open_err = openFile(file_name);
    if(open_err) return 1;

    auto file_stream = &cached_files_[file_name].fs_;
    file_stream->seekp(offset);
    file_stream->write(input_buffer, PAGE_SIZE);
    if (file_stream->bad()) {
        std::cerr << "I/O error while writing" << std::endl;
        file_stream->clear();
        return 1;
    }
    file_stream->flush();
    return 0;
}

int DiskManager::update_root_page_number(FileID fid, PageNum pnum){
    assert(sizeof(pnum) == 4);
    assert(fid_to_fname.count(fid) != 0); // TODO: replace assertion with an error message.
    auto file_name = fid_to_fname[fid];
    int open_err = openFile(file_name);
    if(open_err) {
        assert(0);
        return 1;
    }
    auto file_stream = &cached_files_[file_name].fs_;
    file_stream->seekp(ROOT_PNUM_OFFSET);

    char bytes[4];
    memcpy(bytes, &pnum, sizeof(pnum));
    file_stream->write(bytes, sizeof(pnum));
    if (file_stream->bad()) {
        std::cerr << "I/O error while writing" << std::endl;
        file_stream->clear();
        return 1;
    }
    file_stream->flush();
    return 0;
}

int DiskManager::openFile(String8 file_name){
    // bad file format.
    if(!str_ends_with(file_name, str_lit(FILE_EXT))){
        assert(0);
        return 1;
    }
    if(file_name.last_char() != 0) {
        assert(0);
        return 1; 
    }
    // cache miss
    // open the file
    if (!cached_files_.count(file_name)) {
        cached_files_[file_name] = {
            .fs_ = std::fstream ((char*)file_name.str_, std::ios::binary | std::ios::out | std::ios::in),
            .freelist_ptr_ = 0,
            .num_of_pages_ = 1,
        };
    }
    // file doesn't exist.
    // create a new one and return 1 on failure.
    if(!cached_files_[file_name].fs_.is_open()) {
        cached_files_[file_name].fs_ = std::fstream
            ((char*)file_name.str_, std::ios::binary | std::ios::trunc | std::ios::out | std::ios::in);
        cached_files_[file_name].fs_.clear();
        if (!cached_files_[file_name].fs_.is_open()) {
            cached_files_.erase(file_name);
            return 1;
        }

        char first_page[PAGE_SIZE]{0};
        int one = 1;
        int zero = 0;
        // assigning first 4 bytes to 0  => next free page for allocatation.
        memcpy(first_page, &zero, sizeof(int));
        // assigning second 4 bytes to 1 => cur number of pages inside of the file.
        memcpy(first_page+sizeof(int), &one, sizeof(int));
        
        // this is kind of expensive but happens when creating tables only.
        cached_files_[file_name].fs_.seekp(0);
        cached_files_[file_name].fs_.write(first_page, PAGE_SIZE);
        cached_files_[file_name].fs_.flush();
        if (cached_files_[file_name].fs_.bad()) {
            std::cout << "I/O error while writing" << std::endl;
            cached_files_.erase(file_name);
            return 1;
        }

        cached_files_[file_name].fs_.close();
        cached_files_[file_name].fs_ = std::fstream((char*)file_name.str_, std::ios::binary | std::ios::out | std::ios::in);
        cached_files_[file_name].freelist_ptr_ = 0;
        cached_files_[file_name].num_of_pages_ = 1;
        return 0;
    }
    // at this point we need to get the next free page of this file.
    // read the first and secocnd 4 bytes (sizeof int) then put them into the cache.
    char bytes[8]; 
    // seek to the begining of the file.
    cached_files_[file_name].fs_.seekp(0);
    cached_files_[file_name].fs_.read(bytes, 8);
    int read_count = cached_files_[file_name].fs_.gcount();

    int next_free_page = -1;
    int num_of_pages = -1;
    memcpy(&next_free_page, bytes, sizeof(int));
    memcpy(&num_of_pages, bytes+sizeof(int), sizeof(int));
    if (read_count < 8) {
        //std::cout << "open file error: invalid read count : " << read_count << " " << next_free_page << " " <<
        //   num_of_pages << std::endl;
        cached_files_[file_name].fs_.clear();
        return 1;
    }
    cached_files_[file_name].freelist_ptr_ = next_free_page;
    cached_files_[file_name].num_of_pages_ = num_of_pages;

    return 0;
}

bool DiskManager::deleteFile(FileID fid) {
    assert(fid_to_fname.count(fid));
    String8 file_name = fid_to_fname[fid];
    assert(file_name.last_char() == 0);
    fid_to_fname.erase(fid);
    if(cached_files_.count(file_name)) {
        cached_files_[file_name].fs_.close();
        cached_files_.erase(file_name);
    }
    return std::remove((char*)file_name.str_);
}
