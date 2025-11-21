#pragma once
#include "table_iterator.h"
#include "cache_manager.cpp" 
#include "page.cpp"
#include "table_data_page.cpp"
#include "record.cpp"
#include <cstdint>


TableIterator::TableIterator(CacheManager *cm, PageID page_id): 
    cache_manager_(cm),
    cur_page_id_(page_id)
{
    cur_page_ = reinterpret_cast<TableDataPage*>(cache_manager_->fetchPage(cur_page_id_));
    if(cur_page_){
        cur_num_of_slots_ = cur_page_->getNumOfSlots();
        next_page_number_ = cur_page_->getNextPageNumber();
        prev_page_number_ = cur_page_->getPrevPageNumber();
    }
}
TableIterator::~TableIterator(){
    if(cur_page_)
        cache_manager_->unpinPage(cur_page_id_, false);
}

void TableIterator::clear() {
    if(cur_page_) {
        cache_manager_->unpinPage(cur_page_id_, false);
    }
}

bool TableIterator::hasNext() {
    // invalid current page.
    if(!cur_page_) return false;
    char* tmp = nullptr;
    int32_t next_slot = cur_slot_idx_+1;
    uint32_t rsize =  0;
    //uint32_t* rsize =  new uint32_t(0);
    // iterate through records of the current page.
    while(next_slot < cur_num_of_slots_ && cur_page_->getRecord(&tmp, &rsize, next_slot)){
        cur_slot_idx_ = next_slot;
        next_slot++;
    }
    cur_slot_idx_ = next_slot;
    // didn't find records inside of current page and this is the last page.
    if(cur_slot_idx_ >= cur_num_of_slots_ && next_page_number_ == 0){
        return false; 
    }
    // didn't find records inside of current page and this is not the last page.
    if(cur_slot_idx_ >= cur_num_of_slots_) {
        cache_manager_->unpinPage(cur_page_id_, false);
        cur_page_id_.page_num_ = next_page_number_;
        cur_page_ = reinterpret_cast<TableDataPage*>(cache_manager_->fetchPage(cur_page_id_));
        // invalid next_page_number or an error for some reason.
        if(!cur_page_) {
            return false;
        }

        cur_num_of_slots_ = cur_page_->getNumOfSlots();
        next_page_number_ = cur_page_->getNextPageNumber();
        prev_page_number_ = cur_page_->getPrevPageNumber();
        cur_slot_idx_ = -1;
        // search inside of the next page recursively, then return the final result.
        return hasNext();
    }
    return true;
}
// 0 in case of no more records.
int TableIterator::advance(){
    if(!hasNext()) return 0;
    return 1;
}

Record TableIterator::getCurRecordCpy(){
    char* cur_data = nullptr;
    //uint32_t* rsize = new uint32_t(0);
    uint32_t rsize = 0;
    int err = cur_page_->getRecord(&cur_data, &rsize, cur_slot_idx_);
    if(err) return Record(nullptr, 0);
    return  Record(cur_data, rsize);
}
Record* TableIterator::getCurRecordCpyPtr(){
    char* cur_data = nullptr;
    uint32_t rsize = 0;
    int err = cur_page_->getRecord(&cur_data, &rsize, cur_slot_idx_);
    if(err) return nullptr;
    return  new Record(cur_data, rsize);
}

RecordID TableIterator::getCurRecordID(){
    return RecordID(cur_page_id_, cur_slot_idx_);
}
