#pragma once

#include <string>
#include <shared_mutex>

struct PageID{
    std::string file_name_{0};
    int32_t page_num_{0};
};


#define PAGE_SIZE 4096 //4KB
#define FILE_EXT ".ndb" // nile db
#define SIZE_PAGE_HEADER = 8;
const PageID INVALID_PAGE_ID = PageID{ .file_name_ = "INVALID_FILE_NAME", .page_num_ = -1 };



struct Page {
        char* data_[PAGE_SIZE]{};
        PageID page_id_ = INVALID_PAGE_ID;
        int pin_count_ {0};
        bool is_dirty_;
        std::shared_mutex mutex_;

};
