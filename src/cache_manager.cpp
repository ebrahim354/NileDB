#pragma once

#include <cstdint>
#include <iostream>
#include <list>
#include <mutex>  

#include "disk_manager.cpp"
#include "lru_k_replacer.cpp"

class CacheManager {
    public:
        CacheManager (size_t pool_size, DiskManager *disk_manager, size_t replacer_k)
            : pool_size_(pool_size), disk_manager_(disk_manager) {
                pages_ = new Page[pool_size_];
                replacer_ = new LRUKReplacer(pool_size, replacer_k);

                // Initially, every page is in the free list.
                for (size_t i = 0; i < pool_size_; ++i) {
                    free_list_.emplace_back(static_cast<int>(i));
                }
            }

        ~CacheManager() {
            flushAllPages();
            delete[] pages_;
            delete replacer_;
        }

        void show() {
            std::cout << "free list size: " << free_list_.size() << std::endl;
            std::cout << "pages contents: " << std::endl;
            for (size_t i = 0; i < pool_size_; i++) {
                std::cout << "page number: " << i << " " << *pages_[i].data_ << " pinCnt: " << pages_[i].pin_count_
                    << " isDirty: " << pages_[i].is_dirty_ << " page_id: " << pages_[i].page_id_.file_name_ 
                    << " " << pages_[i].page_id_.page_num_ << std::endl;
            }
        }

        // create a new page on the cache then persist it with allocatePage and returns a pointer to the page.
        // this is not effecient because we persist the new page twice once on creation and flushing,
        // should be optimized later.
        Page* newPage(std::string file_name);
        Page* fetchPage(PageID page_id);
        bool unpinPage(PageID page_id, bool is_dirty);
        bool flushPage(PageID page_id);
        void flushAllPages();
        bool deletePage(PageID page_id);

    private:

        const size_t pool_size_;
        Page *pages_;
        DiskManager *disk_manager_;
        std::map<PageID, uint32_t> page_table_;
        LRUKReplacer *replacer_;
        std::list<uint32_t> free_list_;
        std::mutex latch_;
};




Page* CacheManager::newPage(std::string file_name){
    std::cout << " new page call " << std::endl;
    const std::lock_guard<std::mutex> lock(latch_);
    Page *new_page = nullptr;
    int32_t new_frame = -1;
    if (!free_list_.empty()) {
        new_frame = free_list_.back();
        free_list_.pop_back();
        replacer_->RecordAccess(new_frame);
        replacer_->SetEvictable(new_frame, false);
        new_page = &pages_[new_frame];
        new_page->ResetMemory();
    }
    if (new_frame == -1) {
        bool res = replacer_->Evict(&new_frame);
        if (!res || new_frame == -1) {
            return nullptr;
        }

        replacer_->RecordAccess(new_frame);
        replacer_->SetEvictable(new_frame, false);

        Page *page_to_be_flushed = &pages_[new_frame];
        page_table_.erase(page_to_be_flushed->page_id_);
        if (page_to_be_flushed->is_dirty_) {
            disk_manager_->writePage(page_to_be_flushed->page_id_, page_to_be_flushed->data_);
        }
        page_to_be_flushed->ResetMemory();
        new_page = page_to_be_flushed;
    }
    int err = disk_manager_->allocateNewPage(file_name,new_page->data_, &new_page->page_id_);
    std::cout << file_name << " " << new_page->page_id_.page_num_  << std::endl;
    if(err) {
        std::cout << " could not allocate a new page " << std::endl;
        return nullptr;
    }
    page_table_.insert({new_page->page_id_, new_frame});
    new_page->pin_count_ = 1;
    new_page->is_dirty_ = false;
    return new_page;
}



Page* CacheManager::fetchPage(PageID page_id){
    const std::lock_guard<std::mutex> lock(latch_);
    if (page_id.file_name_ == INVALID_PAGE_ID.file_name_ || page_id.page_num_ == INVALID_PAGE_ID.page_num_) {
        return nullptr;
    }
    int32_t frame = -1;
    auto res = page_table_.find(page_id);
    if (res != page_table_.end()) {
        frame = res->second;
    }
    if (res != page_table_.end() && frame != -1) {
        replacer_->SetEvictable(frame, false);
        replacer_->RecordAccess(frame);
        pages_[frame].pin_count_++;
        return &pages_[frame];
    }
    if (!free_list_.empty()) {
        char page_data[PAGE_SIZE]{};
        int err_reading_page = disk_manager_->readPage(page_id, page_data);
        // page id is not valid.
        if(err_reading_page) {
            std::cout << "couldn't fetch page number : " << page_id.page_num_ 
                << " from the file: " << page_id.file_name_ << std::endl;
            return nullptr;
        }

        frame = free_list_.back();
        free_list_.pop_back();
        replacer_->RecordAccess(frame);
        replacer_->SetEvictable(frame, false);
        page_table_.insert({page_id, frame});


        memcpy(pages_[frame].data_, page_data, PAGE_SIZE);
        pages_[frame].page_id_ = page_id;
        pages_[frame].pin_count_ = 1;
        pages_[frame].is_dirty_ = false;
        return &pages_[frame];
    }
    if (replacer_->Evict(&frame)) {
        char page_data[PAGE_SIZE]{};
        int err_reading_page = disk_manager_->readPage(page_id, page_data);
        // page id is not valid.
        if(err_reading_page) {
            std::cout << "couldn't fetch page number : " << page_id.page_num_ 
                << " from the file: " << page_id.file_name_ << std::endl;
            return nullptr;
        }

        replacer_->RecordAccess(frame);
        replacer_->SetEvictable(frame, false);
        page_table_.erase(pages_[frame].page_id_);
        page_table_.insert({page_id, frame});

        if (pages_[frame].is_dirty_) {
            disk_manager_->writePage(pages_[frame].page_id_, pages_[frame].data_);
            pages_[frame].ResetMemory();
        }

        memcpy(pages_[frame].data_, page_data, PAGE_SIZE);

        pages_[frame].page_id_ = page_id;
        pages_[frame].is_dirty_ = false;
        pages_[frame].pin_count_ = 1;
        return &pages_[frame];
    }
    return nullptr;
}



bool CacheManager::unpinPage(PageID page_id, bool is_dirty) {
    const std::lock_guard<std::mutex> lock(latch_);
    int32_t frame = -1;
    auto res = page_table_.find(page_id);
    if (res != page_table_.end()) {
        frame = res->second;
    }
    if (res == page_table_.end() || frame == -1 || pages_[frame].pin_count_ <= 0) {
        return false;
    }
    if (is_dirty) {
        pages_[frame].is_dirty_ = true;
    }
    pages_[frame].pin_count_--;
    if (pages_[frame].pin_count_ == 0) {
        replacer_->SetEvictable(frame, true);
    }
    return true;
}





bool CacheManager::flushPage(PageID page_id){
    const std::lock_guard<std::mutex> lock(latch_);
    int32_t frame = -1;
    auto res = page_table_.find(page_id);
    if (res != page_table_.end()) {
        frame = res->second;
    }
    bool invalid_page = page_id.page_num_ == INVALID_PAGE_ID.page_num_ || 
        page_id.file_name_ == INVALID_PAGE_ID.file_name_;
    if (invalid_page || res == page_table_.end() || frame == -1) {
        return false;
    }

    Page *page_to_be_flushed = &pages_[frame];
    disk_manager_->writePage(page_to_be_flushed->page_id_, page_to_be_flushed->data_);
    page_to_be_flushed->is_dirty_ = false;
    return true;
}



void CacheManager::flushAllPages() {
    const std::lock_guard<std::mutex> lock(latch_);
    for (size_t i = 0; i < pool_size_; i++) {
        bool invalid_page = pages_[i].page_id_.page_num_ == INVALID_PAGE_ID.page_num_ || 
            pages_[i].page_id_.file_name_ == INVALID_PAGE_ID.file_name_;
        if (invalid_page) continue;
        Page *page_to_be_flushed = &pages_[i];
        disk_manager_->writePage(page_to_be_flushed->page_id_, page_to_be_flushed->data_);
        page_to_be_flushed->is_dirty_ = false;
    }
}

bool CacheManager::deletePage(PageID page_id) {
    const std::lock_guard<std::mutex> lock(latch_);
    int32_t frame = -1;
    auto res = page_table_.find(page_id);
    if (res != page_table_.end()) {
        frame = res->second;
    }
    if (res == page_table_.end() && frame == -1) {
        return true;
    }
    if (pages_[frame].pin_count_ != 0) {
        return false;
    }
    page_table_.erase(page_id);
    replacer_->Remove(frame);
    free_list_.push_back(frame);
    pages_[frame].ResetMemory();
    pages_[frame].page_id_ = INVALID_PAGE_ID;
    pages_[frame].pin_count_ = 0;
    pages_[frame].is_dirty_ = false;
    int err = disk_manager_->deallocatePage(page_id);
    return !err;
}
