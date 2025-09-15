#pragma once
#include <cstdint>
#include "cache_manager.cpp"
#include "free_space_map.cpp"
#include "page.cpp"
#include "table_iterator.cpp"
#include "record.cpp"
#include "table_data_page.cpp"

/*
 * the current model we are using is:              "Heap File Organization".
 *
 * this class assumes that the "DirectoryManager", "MetaDataManager" or "CatalogManager" (multiple names same thing)
 * has done all the checks and created a Record wrapper arround the data in case of inserting or just wants the next
 * Record wrapper returned in case of table scans, Which means that the job of this class is to organize pages inside 
 * of the table file and to emplement an effecient access to records.
 */

/*  
 * what do we expect from this class:
 * 1- allow full table scan using TableIterator (wrapper around Record type).
 * 2- allow inserting a Record.
 * 3- allow updating  a Record.
 * 4- allow deleting  a Record.
 * 5- inserting updating and deleting should be space effecient using a simple free-space map (for now),
 *    a free-space map is a data structure that tells the user which page they are likely to find enough space 
 *    for inserting a new record.
 * 6- implement a table iterator to help doing scans.
 */
class Table {
    public:
        Table(CacheManager* cm, PageID first_page_id, FreeSpaceMap* fsm):
            cache_manager_(cm),
            first_page_id_(first_page_id),
            free_space_map_(fsm)
        {}
        ~Table(){
            delete free_space_map_;
        }

        // should use the free space map to find the closest free space inside of the file
        // or just append it to the end of the file.

        // rid (output)
        // return 1 in case of an error.
        int insertRecord(RecordID* rid, Record &record){
            rid->page_id_ = first_page_id_;
            uint32_t page_num = 0;
            TableDataPage* table_page = nullptr;
            int no_free_space = free_space_map_->getFreePageNum(record.getRecordSize(), &page_num);
            // no free pages
            // allocate a new one with the cache manager
            // or if there is free space fetch the page with enough free space.
            if(no_free_space) {
                table_page = reinterpret_cast<TableDataPage*>(cache_manager_->newPage(first_page_id_.fid_));
                // couldn't fetch any pages for any reason.
                if(table_page == nullptr) {
                    std::cout << " could not create a new table_page " << std::endl;
                    return 1;
                }
                table_page->init();
                // this is the last page.
                table_page->setNextPageNumber(0);
                // we assume this is also the first page and will be updated if not.
                table_page->setPrevPageNumber(0);
                // fetch the previous page if this page is not the first page.
                // this methods assumes that pages are connected which is not always the case, and it needs to be
                // improved.
                // should set the next and prev page pointers but we assume that pages are connected for now.
                
                // if you are not the first page:
                if(table_page->page_id_.page_num_ != 1){
                    auto prev_page_id = table_page->page_id_;
                    prev_page_id.page_num_--;
                    TableDataPage* prev_page = reinterpret_cast<TableDataPage*>
                        (cache_manager_->fetchPage(prev_page_id));
                    prev_page->setNextPageNumber(table_page->getPageNumber());
                    table_page->setPrevPageNumber(prev_page->getPageNumber());
                    cache_manager_->flushPage(prev_page->page_id_);
                    cache_manager_->unpinPage(prev_page->page_id_, true);
                }
                // if you are the first page and you just got created that means,
                // you are the first and last so we don't need to update any other pages.
                rid->page_id_ = table_page->page_id_;
                // empty page.
                free_space_map_->addPage(MAX_FRACTION - 1);
            } else {
                rid->page_id_.page_num_ = page_num;
                table_page = reinterpret_cast<TableDataPage*>(cache_manager_->fetchPage(rid->page_id_));
            }
            // couldn't fetch any pages for any reason.
            if(table_page == nullptr) {
                return 1;
            }
            // should lock the page in write mode (TODO).
            int err = table_page->insertRecord(record.getFixedPtr(0), record.getRecordSize(), &rid->slot_number_);
            
            if(err) {
                std::cout << " could not insert the record to the page " << std::endl;
                return 1;
            }
            err = free_space_map_->updateFreeSpace(table_page->page_id_.page_num_, table_page->getFreeSpaceSize());
            if(err){
                std::cout << "could not update free space map" << std::endl;
                return 1;
            }
            // flush and unpin the page then return.
            cache_manager_->flushPage(table_page->page_id_);
            cache_manager_->unpinPage(table_page->page_id_, true);
            // should unlock the page (TODO).
            // note: locking and unlocking the page should be added before doing any multithreaded operations.
            return 0;
        }

        // return 1 in case of an error.
        int deleteRecord(RecordID &rid){
            TableDataPage* table_page = reinterpret_cast<TableDataPage *>(cache_manager_->fetchPage(rid.page_id_));
            if(table_page == nullptr) return 1;
            int err = table_page->deleteRecord(rid.slot_number_);
            if(err) return err;
            cache_manager_->unpinPage(table_page->page_id_, true);
            return 0;
        }
        // return 1 in case of an error.
        // rid is both an input to find the record and an output of the new position of the updated record. 
        // updates are performed by deleting the old record followed by an insertion of the new one.
        int updateRecord(RecordID *rid, Record &new_record){
            TableDataPage* table_page = reinterpret_cast<TableDataPage *>(cache_manager_->fetchPage(rid->page_id_));
            if(table_page == nullptr) return 1;
            int err = table_page->deleteRecord(rid->slot_number_);
            if(err) return err;
            int insert_err = table_page->insertRecord(new_record.getFixedPtr(0), new_record.getRecordSize(), &rid->slot_number_);
            // inserted no need to find a new page.
            if(!insert_err) return 0;
            // this page is not enough.
            cache_manager_->unpinPage(table_page->page_id_, true);
            // in some scenarios we may delete the old record and encounter any problem while inserting the new one,
            // this needs to be handled later with transactions or doing updates inside of the page to check for enough
            // space first without deleting the old record.
            // WILL BE HANDLED LATER(TODO).
            return this->insertRecord(rid, new_record);
        }
        // we allow only forward scans for now via tableIterator.advance().
        TableIterator* begin() {
            return new TableIterator(cache_manager_, first_page_id_);
        }
    private:
        CacheManager* cache_manager_ = nullptr;
        PageID first_page_id_ = INVALID_PAGE_ID;
        FreeSpaceMap* free_space_map_ = nullptr;
};
