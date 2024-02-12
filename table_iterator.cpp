#pragma once
#include "cache_manager.cpp" 
#include "table_data_page.cpp"
#include "record.cpp"


// read only Iterator for data pages.
class TableIterator {
    public:
        TableIterator(CacheManager *cm, PageID page_id): 
            cur_page_id_(page_id),
            cache_manager_(cm)
        {
            cur_page_ = reinterpret_cast<TableDataPage*>(cache_manager_->fetchPage(cur_page_id_));
            if(cur_page_){
                cur_num_of_slots_ = cur_page_->getNumOfSlots();
                next_page_number_ = cur_page_->getNextPageNumber();
                prev_page_number_ = cur_page_->getPrevPageNumber();
            }
            cur_slot_idx_ = 0;
        }
        ~TableIterator(){
            if(cur_page_)
                cache_manager_->unpinPage(cur_page_id_, false);
        }

        bool hasNext() {
            // invalid current page.
            if(!cur_page_) return false;
            char* tmp;
            // iterate through records of the current page.
            while(cur_slot_idx_ < cur_num_of_slots_ && cur_page_->getRecord(tmp, cur_slot_idx_)){
                cur_slot_idx_++;
            }
            // didn't find records inside of current page and this is the last page.
            if(cur_slot_idx_ >= cur_num_of_slots_ && next_page_number_ == 0){
                return false; 
            }
            // didn't find records inside of current page and this is not the last page.
            if(cur_slot_idx_ >= cur_num_of_slots_) {
                cache_manager_->unpinPage(cur_page_id_, false);
                cur_page_id_.page_num_ = next_page_number_;
                cur_page_ = reinterpret_cast<TableDataPage*>(cache_manager_->fetchPage(cur_page_id_));
                // invalid nex_page_number or an error for some reason.
                if(!cur_page_) return false;

                cur_num_of_slots_ = cur_page_->getNumOfSlots();
                next_page_number_ = cur_page_->getNextPageNumber();
                prev_page_number_ = cur_page_->getPrevPageNumber();
                cur_slot_idx_ = 0;
                // search inside of the next page recursively, then return the final result.
                return hasNext();
            }
            return true;
        }
        const Record* Next(){
            if(!hasNext()) return nullptr;
            char* cur_data;
            int err = cur_page_->getRecord(cur_data, cur_slot_idx_);
            if(err) return nullptr;
            return new Record(cur_data, cur_slot_idx_);
        }
    private:
        CacheManager *cache_manager_;
        PageID cur_page_id_;
        TableDataPage* cur_page_;
        uint32_t next_page_number_;
        uint32_t prev_page_number_;
        uint32_t cur_num_of_slots_;
        uint32_t cur_slot_idx_;
};


