#ifndef PAGE_H
#define PAGE_H

#include <string>
#include <shared_mutex>
#include <cstring>
#include "arena.h"

#define FileID  int32_t
#define PageNum int32_t

#define INVALID_PAGE_NUM -1
#define INVALID_FID      -1

#define PAGE_SIZE 256 // TODO: change to a higher size before release.
//#define PAGE_SIZE 4096 
#define FILE_EXT ".ndb" // nile db
#define SIZE_PAGE_HEADER = 8;

std::unordered_map<FileID, String> fid_to_fname;


struct PageID {
    FileID fid_{-1};
    PageNum page_num_{-1};

    bool isValidPage();
    PageID& operator=(const PageID &rhs);
    bool operator<(const PageID &other) const;
    bool operator==(const PageID &other) const;
    bool operator!=(const PageID &other) const;
};

const PageID INVALID_PAGE_ID = { 
    .fid_      = INVALID_FID,
    .page_num_ = INVALID_PAGE_NUM
};

struct Page {
    char data_[PAGE_SIZE]{};
    PageID page_id_ = INVALID_PAGE_ID;
    int pin_count_ {0};
    bool is_dirty_;
    std::shared_mutex mutex_;
    void ResetMemory();
};


#endif // PAGE_H
