#ifndef CACHE_MANAGER_H
#define CACHE_MANAGER_H

#include <cstdint>
#include <iostream>
#include <list>
#include <mutex>  

#include "disk_manager.h"
#include "lru_k_replacer.h"
#include "defines.h"

class CacheManager {
    public:
        CacheManager (size_t pool_size, DiskManager *disk_manager, size_t replacer_k);
        ~CacheManager();

        void show(bool hide_unpinned = false);

        // create a new page on the cache then persist it with allocatePage and returns a pointer to the page.
        // this is not effecient because we persist the new page twice once on creation and flushing,
        // should be optimized later.
        Page* newPage(FileID fid);
        Page* fetchPage(PageID page_id);
        bool unpinPage(PageID page_id, bool is_dirty);
        bool flushPage(PageID page_id);
        void flushAllPages();
        void resetPage(PageID page_id, u32 frame);
        bool deletePage(PageID page_id);
        bool deleteFile(FileID fid);

    private:

        const size_t pool_size_;
        Page *pages_;
        DiskManager *disk_manager_;
        std::map<PageID, uint32_t> page_table_;
        LRUKReplacer *replacer_;
        std::list<uint32_t> free_list_;
        std::mutex latch_;
};

#endif // CACHE_MANAGER_H
