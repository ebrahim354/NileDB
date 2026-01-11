#pragma once
#include "table_iterator.h"
#include "cache_manager.cpp" 
#include "page.cpp"
#include "table_data_page.cpp"
#include "record.cpp"
#include <cstdint>

TableIterator::TableIterator(){}

TableIterator::TableIterator(CacheManager *cm, TableSchema* schema, PageID page_id):
    cache_manager_(cm), schema_(schema), cur_page_id_(page_id)
{}

void TableIterator::init() {
    assert(cache_manager_ != nullptr && schema_ != nullptr && cur_page_id_ != INVALID_PAGE_ID);
    cur_page_ = reinterpret_cast<TableDataPage*>(cache_manager_->fetchPage(cur_page_id_));
    if(cur_page_){
        cur_num_of_slots_ = cur_page_->getNumOfSlots();
        next_page_number_ = cur_page_->getNextPageNumber();
        prev_page_number_ = cur_page_->getPrevPageNumber();
    }
}

void TableIterator::destroy() {
    if(cur_page_) {
        cache_manager_->unpinPage(cur_page_id_, false);
        cur_page_ = nullptr;
        cache_manager_ = nullptr;
        cur_page_id_ = INVALID_PAGE_ID;
    }
}

bool TableIterator::hasNext() {
    // invalid current page.
    if(!cur_page_) return false;
    char* tmp = nullptr;
    int32_t next_slot = cur_slot_idx_+1;
    uint32_t rsize =  0;
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

// gets a view over the current record (the caller should make sure that the page is pinned after using this).
Record TableIterator::getCurRecord() {
    char* cur_data = nullptr;
    uint32_t rsize = 0;
    int err = cur_page_->getRecord(&cur_data, &rsize, cur_slot_idx_);
    if(err || rsize <= 0 || !cur_data) return Record(nullptr, 0);
    return  Record(cur_data, rsize);
}

// gets a copy of the current record (the caller doesn't need to make sure that the page is pinned).
Record TableIterator::getCurRecordCpy(Arena* arena){
    char* cur_data = nullptr;
    uint32_t rsize = 0;
    int err = cur_page_->getRecord(&cur_data, &rsize, cur_slot_idx_);
    if(err || rsize <= 0 || !cur_data) return Record(nullptr, 0);

    char* data_cpy = (char*) arena->alloc(rsize);
    memcpy(data_cpy, cur_data, rsize);
    return  Record(data_cpy, rsize);
}

int  TableIterator::getCurTupleCpy(Arena& arena, Tuple* out) {
    Record cur_r = getCurRecordCpy(&arena);
    RecordID cur_rid = getCurRecordID();
    *out = Tuple(&arena);
    out->resize(schema_->numOfCols());
    //*out = t;
    // translate the tuple.
    for(int i = 0; i < out->size(); ++i){
        Column col = schema_->getCol(i);
        // check the bitmap if this value is null.
        char * bitmap_ptr = cur_r.getFixedPtr(schema_->getSize())+(i/8);  
        int is_null = *bitmap_ptr & (1 << (i%8));
        if(is_null) {
            out->put_val_at(i, Value(NULL_TYPE));
            continue;
        }
        uint16_t sz = 0;
        char* content = schema_->getValue(i, cur_r, &sz);
        if(!content)
            return 1;
        // this means it's an overflow text.
        if(sz == MAX_U16 && col.getType() == VARCHAR) {
            PageNum pnum = *(PageNum*)content;
            OverflowIterator* it = (OverflowIterator*) arena.alloc(sizeof(OverflowIterator));
            *it = schema_->getTable()->get_overflow_iterator(pnum);

            out->put_val_at(i, Value((char*)it, OVERFLOW_ITERATOR, sz));
        } else {
            out->put_val_at(i, Value(content, col.getType(), sz));
        }
    }
    out->left_most_rid_ = cur_rid;
    return 0;
}

RecordID TableIterator::getCurRecordID(){
    return RecordID(cur_page_id_, cur_slot_idx_);
}
