#pragma once
#include "cache_manager.cpp" 
#include "page.cpp"
#include "btree_leaf_page.cpp"
#include "record.cpp"


// read only Iterator for index pages.
class IndexIterator {
    public:
        IndexIterator(CacheManager *cm, PageID page_id, int entry_idx): 
            cache_manager_(cm),
            cur_page_id_(page_id),
            entry_idx_(entry_idx)
        {
            if(cur_page_id_ != INVALID_PAGE_ID){
                cur_raw_page_ = cache_manager_->fetchPage(cur_page_id_);
                if(cur_raw_page_) cur_page_ = reinterpret_cast<BTreeLeafPage*>(cur_raw_page_->data_);

            }

        }
        ~IndexIterator(){
            if(cur_page_) cache_manager_->unpinPage(cur_page_id_, false);
        }

        bool hasNext() {
            // invalid current page.
            if(cur_page_id_ == INVALID_PAGE_ID || !cur_page_) return false;
            if(cur_page_->GetNextPageId() == INVALID_PAGE_ID || entry_idx_ >= cur_page_->GetSize()) return false;
            return true;

        }
        // 0 in case of no more records.
        int advance(){
            if(!hasNext()) return 0;
            PageID next_page_id = cur_page_->GetNextPageId();
            if (entry_idx_ + 1 == cur_page_->GetSize() && next_page_id != INVALID_PAGE_ID) {
                cache_manager_->unpinPage(cur_page_id_, false);

                cur_raw_page_ = cache_manager_->fetchPage(next_page_id);
                cur_page_ = reinterpret_cast<BTreeLeafPage*>(cur_raw_page_->data_);
                cur_page_id_ = next_page_id;
                entry_idx_ = 0;
            } else {
                entry_idx_++;
            }
            return 1;
        }

        RecordID getCurRecordID(){
            char* cur_data = nullptr;
            std::pair<IndexKey, RecordID> cur_entry = cur_page_->GetPointer(entry_idx_);
            return cur_entry.second;
        }

    private:
        CacheManager *cache_manager_ = nullptr;
        BTreeLeafPage* cur_page_ = nullptr;
        Page* cur_raw_page_ = nullptr;
        PageID cur_page_id_ = INVALID_PAGE_ID;
        int entry_idx_ = -1;
};


