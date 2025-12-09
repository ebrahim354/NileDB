#ifndef DISK_MANAGER_H
#define DISK_MANAGER_H

#include <iostream>
#include <fstream>
#include <cstdint>
#include <string.h>
#include <sys/types.h>
#include <unistd.h>
#include <thread>
#include <unordered_map>
#include <cassert>
#include "page.h"




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
        int allocateNewPage(FileID fid, char* buffer ,PageID *page_id);
        int deallocatePage(PageID page_id);
        bool deleteFile(FileID fid);

    private:
        // 1 on failure, 0 on success.
        int openFile(String file_name);
        // first 4 bytes of a file indicates the next free page number.
        // second 4 bytes of a file indicates the number of pages on a file. 
        // in case of value of 0 means no current free pages
        // append to the end of the file for new pages
        std::unordered_map<String, FileMeta> cached_files_;
};

#endif // DISK_MANAGER_H
