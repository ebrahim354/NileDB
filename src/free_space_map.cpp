#pragma once
#include "cache_manager.cpp"
#include "free_space_map.h"
#include "page.cpp"

void FreeSpaceMap::init(CacheManager* cm, FileID fid) {
    cm_ = cm;
    fid_ = fid;
}

void FreeSpaceMap::destroy(){}

// TODO: page number 0 is not touchable because it's only used by the disk manager,
// therefore we should allocate an extra page each time the (newely allocated page has number 0),
// this should be the job of the disk manager and not the job of other layers.
int FreeSpaceMap::updateFreeSpace(PageID table_pid, u32 used_space){
    assert(table_pid != INVALID_PAGE_ID);
    assert(used_space <= PAGE_SIZE); // you can't use more than the size of a page?
    // we add 1 because page 0 is reserved by the disk manager.
    PageNum free_space_page_num = (table_pid.page_num_ / PAGE_SIZE) + 1; 
    u32 free_space_offset_in_page = (table_pid.page_num_ % PAGE_SIZE);

    Page* corresponding_page = cm_->fetchPage({.fid_ = fid_, .page_num_ = free_space_page_num});
    if(!corresponding_page){
        // if the page does not exist that means either the table_pid is invalid or we need a new page.
        corresponding_page = cm_->newPage(fid_);
        assert(corresponding_page); // could not allocate a new page for some reason.
        if(corresponding_page->page_id_.page_num_ == 0){
            cm_->unpinPage(corresponding_page->page_id_, false);
            corresponding_page = cm_->newPage(fid_);
            assert(corresponding_page);
        }
        if(corresponding_page->page_id_.page_num_ != free_space_page_num) {
            assert(0);
            // if the new page doesn't match the calculated number that means this table_pid is not correct. 
            cm_->deletePage(corresponding_page->page_id_);
            return 1;
        }
    }
    // at this point we know we are at the correct page.
    // calculate the fraction.
    // (used_space / PAGE_SIZE) will never exceed 1.
    assert(used_space < PAGE_SIZE);
    u8 fraction = ((f32)used_space / PAGE_SIZE) * MAX_FRACTION;

    *(u8*)(corresponding_page->data_ + free_space_offset_in_page) = fraction;
    cm_->flushPage(corresponding_page->page_id_);
    cm_->unpinPage(corresponding_page->page_id_, true);
    return 0;
}

// page_num (output).
// return 1 in case of could not find. 
int FreeSpaceMap::getFreePageNum(u32 freespace_needed, PageNum* out_page_num){
    assert(freespace_needed <= PAGE_SIZE);
    u8 needed_fraction = ((f32)freespace_needed / PAGE_SIZE) * MAX_FRACTION;
    // loop over the entire free space map, the first byte with fraction 0 means we will create a new page,
    // (because we can't acheive fraction 0 => each table page has a header that takes some amount of bytes).
    // starting page is always 1 because page 0 is reserved for the disk manager.
    PageID cur_pid = {.fid_ = fid_, .page_num_ = 1};
    while(true){
        Page* cur_page = cm_->fetchPage(cur_pid);
        if(!cur_page) return 1; // this page doesn't exist => didn't find a page.
        for(int i = 0; i < PAGE_SIZE; ++i){
            // skip page number '0'.
            if(cur_pid.page_num_ == 1 && i == 0) continue;
            u8 cur_fraction = *(u8*)(cur_page->data_+i);
            if(cur_fraction == 0) {
                cm_->unpinPage(cur_pid, false);
                return 1; // didn't find a page.
            }
            if(cur_fraction + needed_fraction < MAX_FRACTION) {
                *out_page_num = ((cur_pid.page_num_ - 1)*PAGE_SIZE)+i;
                cm_->unpinPage(cur_pid, false);
                return 0;
            }
        }
        // couldn't find free space on this page.
        cm_->unpinPage(cur_pid, false);
        cur_pid.page_num_++;
    }
    assert(0 && "UNREACHABLE");
    return 1;
}
