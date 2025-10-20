#pragma once

#include <string>
#include <shared_mutex>
#include <cstring>

#define FileID  int32_t
#define PageNum int32_t

#define INVALID_PAGE_NUM -1
#define INVALID_FID      -1

std::unordered_map<FileID, std::string> fid_to_fname;


struct PageID{

    FileID fid_{-1};
    PageNum page_num_{-1};
    bool isValidPage(){
      return (fid_ != INVALID_FID && page_num_ != INVALID_PAGE_NUM);
    }
    PageID& operator=(const PageID &rhs){
        this->fid_ = rhs.fid_; 
        this->page_num_ = rhs.page_num_;
        return *this;
    }

    bool operator<(const PageID &other) const { 
        int files_match = (fid_ == other.fid_); 
        if(files_match)  return page_num_ < other.page_num_;
        return (fid_ < other.fid_);
    }

    bool operator==(const PageID &other) const { 
        int files_match = (fid_ == other.fid_); 
        if(files_match)  return page_num_ == other.page_num_;
        return (files_match);
    }
    bool operator!=(const PageID &other) const { 
        int files_match = (fid_ == other.fid_); 
        if(files_match)  return page_num_ != other.page_num_;
        return 1;
    }
};


#define PAGE_SIZE 4096 //4KB
#define FILE_EXT ".ndb" // nile db
#define SIZE_PAGE_HEADER = 8;
PageID INVALID_PAGE_ID = { 
  .fid_      = INVALID_FID,
  .page_num_ = INVALID_PAGE_NUM
};



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

