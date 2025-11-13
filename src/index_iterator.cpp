#pragma once
#include "cache_manager.cpp" 
#include "page.cpp"
#include "btree_leaf_page.cpp"
#include "record.cpp"
#include "table_data_page.cpp"


// read only Iterator for index pages.
class IndexIterator {
    public:
        IndexIterator(CacheManager *cm, PageID page_id, int entry_idx = -1): 
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
            clear();
        }

        void clear() {
            if(cur_page_) cache_manager_->unpinPage(cur_page_id_, false);
        }

        bool isNull() {
            return (cur_page_ == nullptr || 
                    cur_raw_page_ == nullptr || 
                    cur_page_id_ == INVALID_PAGE_ID);
        }

        bool hasNext() {
            if(isNull()) return false;
            // invalid current page.
            if(cur_page_id_ == INVALID_PAGE_ID || !cur_page_) return false;
            if(cur_page_->get_next_page_number() == INVALID_PAGE_NUM && entry_idx_ + 1 >= cur_page_->get_num_of_slots()) 
              return false;
            return true;
        }
        void assign_to_null_page() {
            cur_page_ = nullptr;
            cur_raw_page_ = nullptr;
            cur_page_id_ = INVALID_PAGE_ID;
            entry_idx_ = -1;
        }
        // 0 in case of no more records.
        int advance(){
            if(isNull()) return 0;
            if(!hasNext()) {
                clear();           
                assign_to_null_page();
                return 0;
            }
            PageID next_page_id = cur_page_id_;
            next_page_id.page_num_ = cur_page_->get_next_page_number();
            if (entry_idx_ + 1 >= cur_page_->get_num_of_slots() && next_page_id.isValidPage()) {
                cache_manager_->unpinPage(cur_page_id_, false);

                cur_raw_page_ = cache_manager_->fetchPage(next_page_id);
                cur_page_ = reinterpret_cast<BTreeLeafPage*>(cur_raw_page_->data_);
                cur_page_id_ = next_page_id;
                entry_idx_ = 0;
                if(cur_page_->get_num_of_slots() == 0) return advance();
            } else {
                entry_idx_++;
            }
            return 1;
        }

        IndexKey getCurKey(){
            if(isNull() || entry_idx_ > cur_page_->get_num_of_slots()) return IndexKey();
            char* cur_data = nullptr;
            std::pair<IndexKey, RecordID> cur_entry = cur_page_->getPointer(entry_idx_);
            return cur_entry.first;
        }

        RecordID getCurRecordID(){
            if(isNull() || entry_idx_ > cur_page_->get_num_of_slots()) return RecordID(INVALID_PAGE_ID, -1);
            char* cur_data = nullptr;
            std::pair<IndexKey, RecordID> cur_entry = cur_page_->getPointer(entry_idx_);
            if(!cur_entry.first.data_) return RecordID(INVALID_PAGE_ID, -1);
            return cur_entry.second;
        }

        Record getCurRecordCpy(){
            if(isNull()) return Record(nullptr, 0);
            char* cur_data = nullptr;
            uint32_t rsize = 0;
            RecordID rid = getCurRecordID();
            if(rid.page_id_ == INVALID_PAGE_ID) return Record(nullptr, 0);
            //---------------------------------------------------------------------
            // TODO: usually you don't need to unpin the table page because you will mostly need it for future records,
            // so figure out a good way to optemize this. 
            // maybe remember all table pages and unpin them in the denstructor? (can be dangerous for larg tables).
            auto table_page = reinterpret_cast<TableDataPage*>(cache_manager_->fetchPage(rid.page_id_));
            if(!table_page) return Record(nullptr, 0);
            int err = table_page->getRecord(&cur_data, &rsize, rid.slot_number_);
            cache_manager_->unpinPage(rid.page_id_, false);
            //---------------------------------------------------------------------
            if(err) return Record(nullptr, 0);
            return  Record(cur_data, rsize);
        }

        // TODO: Fix copy pasta.
        Record* getCurRecordCpyPtr(){
            if(isNull()) return nullptr; 
            char* cur_data = nullptr;
            uint32_t rsize = 0;
            RecordID rid = getCurRecordID();
            if(rid.page_id_ == INVALID_PAGE_ID) return nullptr; 
            //---------------------------------------------------------------------
            // TODO: usually you don't need to unpin the table page because you will mostly need it for future records,
            // so figure out a good way to optemize this. 
            // maybe remember all table pages and unpin them in the denstructor? (can be dangerous for larg tables).
            auto table_page = reinterpret_cast<TableDataPage*>(cache_manager_->fetchPage(rid.page_id_));
            if(!table_page) return nullptr; 
            int err = table_page->getRecord(&cur_data, &rsize, rid.slot_number_);
            cache_manager_->unpinPage(rid.page_id_, false);
            //---------------------------------------------------------------------
            if(err) return nullptr;
            return  new Record(cur_data, rsize);
        }

        bool operator==(IndexIterator& rhs) {
            return (cur_page_id_ == rhs.cur_page_id_ && entry_idx_ == rhs.entry_idx_);
        }

    private:
        CacheManager *cache_manager_ = nullptr;
        BTreeLeafPage* cur_page_ = nullptr;
        Page* cur_raw_page_ = nullptr;
        PageID cur_page_id_ = INVALID_PAGE_ID;
        int entry_idx_ = -1;
};


