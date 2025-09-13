#pragma once

#include <string>
#include <shared_mutex>
#include <cstring>

#define FileID int32_t

struct PageID{

    FileID file_id_{-1};
    int32_t page_num_{-1};
    PageID& operator=(const PageID &rhs){
        this->file_id_ = rhs.file_id_; 
        this->page_num_ = rhs.page_num_;
        return *this;
    }

    bool operator<(const PageID &other) const { 
        int files_match = (file_id_ == other.file_id_); 
        if(files_match)  return page_num_ < other.page_num_;
        return (file_id_ < other.file_id_);
    }

    bool operator==(const PageID &other) const { 
        int files_match = (file_id_ == other.file_id_); 
        if(files_match)  return page_num_ == other.page_num_;
        return (files_match);
    }
    bool operator!=(const PageID &other) const { 
        int files_match = (file_id_ == other.file_id_); 
        if(files_match)  return page_num_ != other.page_num_;
        return 1;
    }
};


#define PAGE_SIZE 4096 //4KB
#define FILE_EXT ".ndb" // nile db
#define SIZE_PAGE_HEADER = 8;
PageID INVALID_PAGE_ID = { .file_id_ = -1 , .page_num_ = -1 };



struct Page {
        char data_[PAGE_SIZE]{};
        PageID page_id_ = INVALID_PAGE_ID;
        int pin_count_ {0};
        bool is_dirty_;
        std::shared_mutex mutex_;

        void ResetMemory() { 
            memset(data_, 0, PAGE_SIZE); 
        }

};

