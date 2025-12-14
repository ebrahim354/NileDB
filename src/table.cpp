#pragma once
#include <cstdint>
#include "table.h"
#include "cache_manager.cpp"
#include "free_space_map.cpp"
#include "page.cpp"
#include "table_iterator.cpp"
#include "record.cpp"
#include "table_data_page.cpp"




void Table::init(CacheManager* cm, FileID fid) {
    assert(fid != INVALID_FID);
    cache_manager_  = cm;
    fid_  = fid;
    PageID zero_pid = {.fid_ = fid_, .page_num_ = 0};

    // TODO: consider moving this into the system catalog?
    //
    // page number 0 is reserved for meta data.
    Page* meta_page = cache_manager_->fetchPage(zero_pid); 
    assert(meta_page != 0);
    first_pnum_ = *(PageNum*)(meta_page->data_+ROOT_PNUM_OFFSET);
    if(first_pnum_ == 0) first_pnum_ = INVALID_PAGE_NUM;
    cache_manager_->unpinPage(zero_pid, false);

    // fsm file id is by convention the next file id after the table's file id.
    free_space_map_.init(cm, fid_+1); 
}

void Table::update_first_page_number(PageNum pnum) {
    first_pnum_ = pnum;
    cache_manager_->update_root_page_number(fid_, pnum);
}

void Table::destroy(){}

// rid (output)
// return 1 in case of an error.
int Table::insertRecord(RecordID* rid, Record &record){
    if((PAGE_SIZE-TABLE_PAGE_HEADER_SIZE) < record.getRecordSize() + TABLE_SLOT_ENTRY_SIZE){
        std::cout << "Record size is larger than page size.\n";
        return 1; 
    }
    rid->page_id_.fid_ = fid_;
    PageNum page_num = 0;
    TableDataPage* table_page = nullptr;
    // TODO: try to apply the best case :
    // (inserting without the slot entry size) => this assumes that there are free slots inside of the page,
    // if inserting into the page fails retry with worst case maybe ?,
    // (inserting with the slot entry size) => this assumes that there is no free slots inside of the page.
    // worst case.
    int no_free_space = 
        free_space_map_.getFreePageNum(record.getRecordSize() + TABLE_SLOT_ENTRY_SIZE, &page_num);
    // no free pages
    // allocate a new one with the cache manager
    // or if there is free space fetch the page with enough free space.
    if(no_free_space) {
        table_page = reinterpret_cast<TableDataPage*>(cache_manager_->newPage(fid_));
        // couldn't fetch any pages for any reason.
        if(table_page == nullptr) {
            std::cout << " could not create a new table_page " << std::endl;
            return 1;
        }
        if(first_pnum_ == INVALID_PAGE_NUM) update_first_page_number(table_page->page_id_.page_num_);
        table_page->init();
        // this is the last page.
        table_page->setNextPageNumber(0);
        // we assume this is also the first page and will be updated if not.
        table_page->setPrevPageNumber(0);

        // if you are not the first page:
        if(table_page->page_id_.page_num_ != first_pnum_){
            PageID first_page_id = {
                .fid_ = fid_,
                .page_num_ = first_pnum_,
            };
            auto first_page = (TableDataPage*)cache_manager_->fetchPage(first_page_id);

            // new pages are appended after the first page:
            // first_page->p1->p2->p3
            // first_page->new_page->p1->p2->p3
            //
            table_page->setPrevPageNumber(first_page->getPageNumber());
            table_page->setNextPageNumber(first_page->getNextPageNumber());
            first_page->setNextPageNumber(table_page->getPageNumber());

            cache_manager_->unpinPage(first_page->page_id_, true);
        }
        // if you are the first page and you just got created that means,
        // you are the first and last so we don't need to update any other pages.
        rid->page_id_ = table_page->page_id_;
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
    err = free_space_map_.updateFreeSpace(table_page->page_id_, table_page->getUsedSpaceSize());
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
    free_space_map_.updateFreeSpace(table_page->page_id_, table_page->getUsedSpaceSize());
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
    free_space_map_.updateFreeSpace(table_page->page_id_, table_page->getUsedSpaceSize());
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
TableIterator Table::begin() {
    return TableIterator(cache_manager_, {.fid_ = fid_, .page_num_ = first_pnum_});
}

FileID Table::get_fid(){
    return fid_;
}

