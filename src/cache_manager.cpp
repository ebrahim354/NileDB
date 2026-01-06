#pragma once

#include <cstdint>
#include <iostream>
#include <list>
#include <mutex>  

#include "disk_manager.cpp"
#include "lru_k_replacer.cpp"
#include "cache_manager.h"

CacheManager::CacheManager (size_t pool_size, DiskManager *disk_manager, size_t replacer_k)
    : pool_size_(pool_size), disk_manager_(disk_manager) {
        std::cout << "pool size: " << pool_size_ << "\n";
        std::cout << "replacer k: " << replacer_k << "\n";
        pages_ = new Page[pool_size_];
        replacer_ = new LRUKReplacer(pool_size, replacer_k);

        // Initially, every page is in the free list.
        for (size_t i = 0; i < pool_size_; ++i) {
            free_list_.emplace_back(static_cast<int>(i));
        }
    }

CacheManager::~CacheManager() {
    flushAllPages();
    delete[] pages_;
    delete replacer_;
}

void CacheManager::show(bool hide_unpinned) {
    std::cout << "free list size: " << free_list_.size() << std::endl;
    std::cout << "pages contents: " << std::endl;
    for (size_t i = 0; i < pool_size_; i++) {
        if(hide_unpinned && pages_[i].pin_count_ == 0) continue;
        std::cout << "page number: " << i << " " << *pages_[i].data_ << " pinCnt: " << pages_[i].pin_count_
            << " isDirty: " << pages_[i].is_dirty_ << " page_id: " << to_string(fid_to_fname[pages_[i].page_id_.fid_])
            << " " << pages_[i].page_id_.page_num_ << std::endl;
    }
}

Page* CacheManager::newPage(FileID fid){
    const std::lock_guard<std::mutex> lock(latch_);
    Page *new_page = nullptr;
    int32_t new_frame = -1;
    if (!free_list_.empty()) {
        new_frame = free_list_.back();
        free_list_.pop_back();
        replacer_->RecordAccess(new_frame);
        replacer_->SetEvictable(new_frame, false);
        new_page = &pages_[new_frame];
    }
    else if (new_frame == -1) {
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

    assert(new_frame != -1);


    int err = disk_manager_->allocateNewPage(fid, new_page->data_, &new_page->page_id_);
    if(err) {
        replacer_->SetEvictable(new_frame, true);
        std::cout << "could not allocate a new page " << to_string(fid_to_fname[fid]) << std::endl;
        return nullptr;
    }
    page_table_.insert({new_page->page_id_, new_frame});
    new_page->pin_count_ = 1;
    new_page->is_dirty_ = false;
    return new_page;
}



Page* CacheManager::fetchPage(PageID page_id){
    const std::lock_guard<std::mutex> lock(latch_);
    if (page_id.fid_ == INVALID_PAGE_ID.fid_ || page_id.page_num_ == INVALID_PAGE_ID.page_num_) {
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

    char page_data[PAGE_SIZE]{};
    int err_reading_page = disk_manager_->readPage(page_id, page_data);
    if(err_reading_page) {
        return nullptr;
    }

    if (!free_list_.empty()) {
        frame = free_list_.back();
        free_list_.pop_back();
    }
    else if (replacer_->Evict(&frame) && pages_[frame].is_dirty_) {
        disk_manager_->writePage(pages_[frame].page_id_, pages_[frame].data_);
        pages_[frame].ResetMemory();
    }

    if(frame == -1){
        show();
        replacer_->Show();
    }
    assert(frame != -1);

    replacer_->RecordAccess(frame);
    replacer_->SetEvictable(frame, false);
    page_table_.erase(pages_[frame].page_id_);
    page_table_.insert({page_id, frame});


    memcpy(pages_[frame].data_, page_data, PAGE_SIZE);

    pages_[frame].page_id_ = page_id;
    pages_[frame].is_dirty_ = false;
    pages_[frame].pin_count_ = 1;
    return &pages_[frame];
}



bool CacheManager::unpinPage(PageID page_id, bool is_dirty) {
    const std::lock_guard<std::mutex> lock(latch_);
    int32_t frame = -1;
    auto res = page_table_.find(page_id);
    if (res != page_table_.end()) {
        frame = res->second;
    }
    if (res == page_table_.end() || frame == -1 || pages_[frame].pin_count_ <= 0) {
        assert(0 && "unpin failed\n");
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
        page_id.fid_ == INVALID_PAGE_ID.fid_;
    if (invalid_page || res == page_table_.end() || frame == -1) {
        return false;
    }

    Page *page_to_be_flushed = &pages_[frame];
    disk_manager_->writePage(page_to_be_flushed->page_id_, page_to_be_flushed->data_);
    page_to_be_flushed->is_dirty_ = false;
    return true;
}
bool CacheManager::update_root_page_number(FileID fid, PageNum pnum){
    int err = disk_manager_->update_root_page_number(fid, pnum);
    assert(!err);
    return !err;
}



void CacheManager::flushAllPages() {
    const std::lock_guard<std::mutex> lock(latch_);
    for (size_t i = 0; i < pool_size_; i++) {
        bool invalid_page = pages_[i].page_id_.page_num_ == INVALID_PAGE_ID.page_num_ || 
            pages_[i].page_id_.fid_ == INVALID_PAGE_ID.fid_;
        if (invalid_page) continue;
        Page *page_to_be_flushed = &pages_[i];
        if(page_to_be_flushed->is_dirty_ == false) continue; 
        disk_manager_->writePage(page_to_be_flushed->page_id_, page_to_be_flushed->data_);
        page_to_be_flushed->is_dirty_ = false;
    }
}

void CacheManager::resetPage(PageID page_id, u32 frame){
    page_table_.erase(page_id);
    replacer_->Remove(frame);
    free_list_.push_back(frame);
    pages_[frame].ResetMemory();
    pages_[frame].page_id_ = INVALID_PAGE_ID;
    pages_[frame].pin_count_ = 0;
    pages_[frame].is_dirty_ = false;
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
    assert(pages_[frame].pin_count_ == 0);
    if (pages_[frame].pin_count_ != 0) {
        return false;
    }
    resetPage(page_id, frame);
    int err = disk_manager_->deallocatePage(page_id);
    assert(err == 0);
    return !err;
}

// TODO: make this thread safe.
bool CacheManager::deleteFile(FileID fid) {
    // loop over the entire page table and check for pages with the specified fid and delete them.
    // then call the disk manager to delete the file.
    std::vector<std::pair<PageID, i32>> to_be_erased;
    to_be_erased.reserve(16);
    for(auto& page: page_table_){
        if(page.first.fid_ != fid) continue;
        to_be_erased.push_back(page);
    }
    for(u32 i = 0; i < to_be_erased.size(); ++i){
        resetPage(to_be_erased[i].first, to_be_erased[i].second);
    }
    return disk_manager_->deleteFile(fid);
}
