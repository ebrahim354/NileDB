#pragma once
#include <cstdint>
#include "table.h"
#include "cache_manager.cpp"
#include "free_space_map.cpp"
#include "page.cpp"
#include "table_iterator.cpp"
#include "record.cpp"
#include "table_data_page.cpp"

void Table::init(CacheManager* cm, PageID first_page_id, FreeSpaceMap* fsm) {
    cache_manager_  = cm;
    first_page_id_  = first_page_id;
    free_space_map_ = fsm;
}
void Table::destroy(){
    // delete free_space_map_;
}

// should use the free space map to find the closest free space inside of the file
// or just append it to the end of the file.

// rid (output)
// return 1 in case of an error.
int Table::insertRecord(RecordID* rid, Record &record){
    if((PAGE_SIZE-TABLE_PAGE_HEADER_SIZE) < record.getRecordSize() + TABLE_SLOT_ENTRY_SIZE){
        std::cout << "Record size is larger than page size.\n";
        return 1; 
    }
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
            //cache_manager_->flushPage(prev_page->page_id_);
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
        std::cout << "couldn't fetch any pages.\n";
        return 1;
    }
    // should lock the page in write mode (TODO).
    int err = table_page->insertRecord(record.getFixedPtr(0), record.getRecordSize(), &rid->slot_number_);

    if(err) {
        std::cout << " could not insert the record to the page " << std::endl;
        cache_manager_->unpinPage(table_page->page_id_, true);
        return 1;
    }
    err = free_space_map_->updateFreeSpace(table_page->page_id_.page_num_, table_page->getFreeSpaceSize());
    if(err){
        std::cout << "could not update free space map" << std::endl;
        cache_manager_->unpinPage(table_page->page_id_, true);
        return 1;
    }
    // flush and unpin the page then return.
    //cache_manager_->flushPage(table_page->page_id_);
    cache_manager_->unpinPage(table_page->page_id_, true);
    // should unlock the page (TODO).
    // note: locking and unlocking the page should be added before doing any multithreaded operations.
    return 0;
}

// return 1 in case of an error.
int Table::deleteRecord(RecordID &rid){
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
int Table::updateRecord(RecordID *rid, Record &new_record){
    TableDataPage* table_page = reinterpret_cast<TableDataPage *>(cache_manager_->fetchPage(rid->page_id_));
    if(table_page == nullptr) return 1;
    int err = table_page->deleteRecord(rid->slot_number_);
    if(err) {
        cache_manager_->unpinPage(table_page->page_id_, true);
        return err;
    }
    int insert_err = table_page->insertRecord(new_record.getFixedPtr(0), new_record.getRecordSize(), &rid->slot_number_);
    // inserted no need to find a new page.
    cache_manager_->unpinPage(table_page->page_id_, true);
    if(!insert_err) 
        return 0;
    // this page is not enough.
    // in some scenarios we may delete the old record and encounter any problem while inserting the new one,
    // this needs to be handled later with transactions or doing updates inside of the page to check for enough
    // space first without deleting the old record.
    // WILL BE HANDLED LATER(TODO).
    return this->insertRecord(rid, new_record);
}
// we allow only forward scans for now via tableIterator.advance().
TableIterator* Table::begin() {
    return new TableIterator(cache_manager_, first_page_id_);
}
