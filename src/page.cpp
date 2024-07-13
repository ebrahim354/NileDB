#pragma once

#include <string>
#include <shared_mutex>
#include <cstring>


struct PageID{
    std::string file_name_{0};
    int32_t page_num_{0};

    bool operator<(const PageID &other) const { 
        int files_match = strcmp(file_name_.c_str(), other.file_name_.c_str());
        if(files_match == 0)  return page_num_ < other.page_num_;
        return (files_match < 0);
    }

    bool operator==(const PageID &other) const { 
        int files_match = strcmp(file_name_.c_str(), other.file_name_.c_str());
        if(files_match == 0)  return page_num_ == other.page_num_;
        return 0;
    }
  
};


#define PAGE_SIZE 4096 //4KB
#define FILE_EXT ".ndb" // nile db
#define SIZE_PAGE_HEADER = 8;
PageID INVALID_PAGE_ID = { .file_name_ = "INVALID_FILE_NAME", .page_num_ = -1 };



class Page {
    public:
        char data_[PAGE_SIZE]{};
        PageID page_id_ = INVALID_PAGE_ID;
        int pin_count_ {0};
        bool is_dirty_;
        std::shared_mutex mutex_;

        void ResetMemory() { 
            memset(data_, 0, PAGE_SIZE); 
        }

};

