#pragma once

#include <iostream>
#include <cassert>
#include <fstream>
#include <cstdint>
#include <string.h>
#include <sys/types.h>
#include <unistd.h>
#include <thread>
#include <unordered_map>
#include "page.cpp"



#define META_DATA_FILE_FID   0
#define META_DATA_FILE  "NILEDB_META_DATA.ndb"
#define META_DATA_FSM_FID   1
#define META_DATA_FSM   "NILEDB_META_DATA_fsm.ndb"
#define META_DATA_TABLE "NILEDB_META_DATA"



// meta data for managing files:
// num_of_pages_ >= 1, there is always at least one meta page on a file.
// freelist_ptr_ = first 4 bytes.
// num_of_pages_ = second 4 bytes.
struct FileMeta {
  std::string file_name_;
  std::fstream fs_;
  int freelist_ptr_;   
  int num_of_pages_;
};


class DiskManager {
    public:
        DiskManager(){
          // TODO: cache meta data file on boot.
          int status = openFile(META_DATA_FILE_FID, META_DATA_FILE);
          assert(status);
          status = openFile(META_DATA_FSM_FID, META_DATA_FSM);
          assert(status);
        }
        ~DiskManager();
        // returns 1 in case of failure and 0 in case of success.
        int readPage(PageID page_id, char* ouput_buffer);
        int writePage(PageID page_id, char* input_buffer);

        // page_id is the output and return value 1 in case of failure.
        int allocateNewPage(std::string file_name, char* buffer ,PageID *page_id);
        int deallocatePage(PageID page_id);

    private:
        // 1 on failure, 0 on success.
        int openFile(FileID fid, std::string file_name);
        // first 4 bytes of a file indicates the next free page number.
        // second 4 bytes of a file indicates the number of pages on a file. 
        // in case of value of 0 means now current free pages
        // append to the end of the file for new pages
        std::unordered_map<FileID, FileMeta> cached_files_;
};


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
  FileID file_id = page_id.file_id_;
  // file is not found.
  if(!cached_files_.count(file_id)) return 1;
  FileMeta* fm= &cached_files_[file_id];

  auto file_name = fm->file_name_;
  auto page_num = page_id.page_num_;

  int err = openFile(file_id, file_name);
  if(err) return 1;

  std::fstream* file_stream = &fm->fs_;
  int  cur_ptr = fm->freelist_ptr_;
  char* bytes = static_cast<char*>(static_cast<void*>(&cur_ptr));


  file_stream->seekp(page_num * PAGE_SIZE);
  file_stream->write(bytes, sizeof(int));
  file_stream->flush();

  if (file_stream->bad()) {
    std::cerr << "I/O error while writing" << std::endl;
    return 1;
  }

  // freelist_ptr_ value on the disk will be updated by the destructor.(fault handling might change this).
  fm->freelist_ptr_ = page_num;
  file_stream->flush();
  return 0; 
}


// page_id should have the file_id_ filled by the caller of this function.
int DiskManager::allocateNewPage(std::string file_name, char* buffer , PageID *page_id){
  if(!page_id || page_id->file_id_ < 0) return 1;

    int err = openFile(page_id->file_id_, file_name);
    FileMeta* fm = &cached_files_[page_id->file_id_];

    if(err) return 1;
    std::fstream* file_stream = &fm->fs_;
    int next_free_page = fm->freelist_ptr_;
    int offset_to_eof = (fm->num_of_pages_) * PAGE_SIZE;
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

        fm->num_of_pages_++;
        char bytes[8];
        memcpy(bytes, &fm->freelist_ptr_, sizeof(int));
        memcpy(bytes+sizeof(int), &fm->num_of_pages_, sizeof(int));
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
        fm->freelist_ptr_ = next;
    }
    file_stream->flush();
    return 0;
}


int DiskManager::readPage(PageID page_id, char* output_buffer) {
  FileID file_id = page_id.file_id_;
  // file is not found.
  if(!cached_files_.count(file_id)) return 1;
  FileMeta* fm= &cached_files_[file_id];

  std::string file_name =  fm->file_name_;
  uint32_t page_num = page_id.page_num_;
  int offset = page_num * PAGE_SIZE;
  int open_err = openFile(file_id, file_name);
  if(open_err) return 1;
  std::fstream* file_stream = &fm->fs_;

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
  FileID file_id = page_id.file_id_;
  // file is not found.
  if(!cached_files_.count(file_id)) return 1;
  FileMeta* fm = &cached_files_[file_id];

  std::string file_name =  fm->file_name_;
  uint32_t page_num = page_id.page_num_;
  int offset = page_num * PAGE_SIZE;
  int open_err = openFile(file_id, file_name);
  if(open_err) return 1;

  std::fstream* file_stream = &fm->fs_;
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

int DiskManager::openFile(FileID fid, std::string file_name){
    // bad file format.
    std::string::size_type n = file_name.rfind(FILE_EXT);
    if (fid < 0 || n == std::string::npos) 
        return 1;
    // cache miss
    // open the file
    if (!cached_files_.count(fid)) {
        cached_files_[fid] = {
            .fs_ = std::fstream (file_name, std::ios::binary | std::ios::out | std::ios::in),
            .freelist_ptr_ = 0,
            .num_of_pages_ = 1,
        };
    }
    // file doesn't exist.
    // create a new one and return 1 on failure.
    if(!cached_files_[fid].fs_.is_open()){
        cached_files_[fid].fs_ = std::fstream
            (file_name, std::ios::binary | std::ios::trunc | std::ios::out | std::ios::in);
        cached_files_[fid].fs_.clear();
        if (!cached_files_[fid].fs_.is_open()) {
            cached_files_.erase(fid);
            return 1;
        }

        char* first_page = new char[PAGE_SIZE];
        int one = 1;
        int zero = 0;
        // assigning first 4 bytes to 0  => next free page for allocatation.
        memcpy(first_page, &zero, sizeof(int));
        // assigning second 4 bytes to 1 => cur number of pages inside of the file.
        memcpy(first_page+sizeof(int), &one, sizeof(int));
        
        // this is kind of expensive but happens when creating tables only.
        cached_files_[fid].fs_.seekp(0);
        cached_files_[fid].fs_.write(first_page, PAGE_SIZE);
        cached_files_[fid].fs_.flush();
        if (cached_files_[fid].fs_.bad()) {
            // std::cout << "I/O error while writing" << std::endl;
            cached_files_.erase(fid);
            return 1;
        }

        cached_files_[fid].fs_.close();
        cached_files_[fid].fs_ = std::fstream(file_name, std::ios::binary | std::ios::out | std::ios::in);
        cached_files_[fid].freelist_ptr_ = 0;
        cached_files_[fid].num_of_pages_ = 1;
        delete [] first_page;
        return 0;
    }
    // at this point we need to get the next free page of this file.
    // read the first and secocnd 4 bytes (sizeof int) then put them into the cache.
    char bytes[8]; 
    // seek to the begining of the file.
    cached_files_[fid].fs_.seekp(0);
    cached_files_[fid].fs_.read(bytes, 8);
    int read_count = cached_files_[fid].fs_.gcount();

    int next_free_page = -1;
    int num_of_pages = -1;
    memcpy(&next_free_page, bytes, sizeof(int));
    memcpy(&num_of_pages, bytes+sizeof(int), sizeof(int));
    if (read_count < 8) {
        //std::cout << "open file error: invalid read count : " << read_count << " " << next_free_page << " " <<
        //   num_of_pages << std::endl;
        cached_files_[fid].fs_.clear();
        return 1;
    }
    cached_files_[fid].freelist_ptr_ = next_free_page;
    cached_files_[fid].num_of_pages_ = num_of_pages;

    return 0;
}


