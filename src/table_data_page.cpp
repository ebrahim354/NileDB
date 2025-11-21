#pragma once
#include "table_data_page.h"
#include "page.cpp" 

void TableDataPage::init(){
    setPageNumber(this->page_id_.page_num_);
    // last byte.
    setFreeSpaceOffset(PAGE_SIZE - 1);
    // prev page number and next page number should be initialized by the user of the class.
}

char*    TableDataPage::getPtrTo(size_t offset){
    return data_+offset;
}

void TableDataPage::setPageNumber(uint32_t page_number){
    memcpy(getPtrTo(PAGE_NUMBER_OFFSET_), &page_number, sizeof(page_number)); 
}
uint32_t TableDataPage::getPageNumber(){
    return *reinterpret_cast<uint32_t *>(getPtrTo(PAGE_NUMBER_OFFSET_));
}

void TableDataPage::setPrevPageNumber(uint32_t prev_page_number){
    memcpy(getPtrTo(PREV_PAGE_NUMBER_OFFSET_), &prev_page_number, sizeof(prev_page_number)); 
}
uint32_t TableDataPage::getPrevPageNumber(){
    return *reinterpret_cast<uint32_t*>(getPtrTo(PREV_PAGE_NUMBER_OFFSET_));
}

void TableDataPage::setNextPageNumber(uint32_t next_page_number){
    memcpy(getPtrTo(NEXT_PAGE_NUMBER_OFFSET_), &next_page_number, sizeof(next_page_number)); 
}
uint32_t TableDataPage::getNextPageNumber(){
    return *reinterpret_cast<uint32_t*>(getPtrTo(NEXT_PAGE_NUMBER_OFFSET_));
}

void TableDataPage::setFreeSpaceOffset(uint32_t free_space_ptr){
    memcpy(getPtrTo(FREE_SPACE_PTR_OFFSET_), &free_space_ptr, sizeof(free_space_ptr)); 
}


uint32_t TableDataPage::getFreeSpaceOffset(){
    return*reinterpret_cast<uint32_t*>(getPtrTo(FREE_SPACE_PTR_OFFSET_));
}
char* TableDataPage::getFreeSpacePtr(){
    return getPtrTo(*reinterpret_cast<uint32_t*>(getPtrTo(FREE_SPACE_PTR_OFFSET_)));
}

void TableDataPage::setNumOfSlots(uint32_t slot_cnt){
    memcpy(getPtrTo(NUMBER_OF_SLOTS_OFFSET_), &slot_cnt, sizeof(slot_cnt)); 
}
uint32_t TableDataPage::getNumOfSlots(){
    return *reinterpret_cast<uint32_t*>(getPtrTo(NUMBER_OF_SLOTS_OFFSET_));
}


// return value should not be negative.
size_t TableDataPage::getFreeSpaceSize(){
    size_t end_of_slots_offset = SLOT_ARRAY_OFFSET_ + (getNumOfSlots() * SLOT_ENTRY_SIZE_);
    char* end_of_slots_ptr = getPtrTo(end_of_slots_offset);
    char* free_space_ptr = getFreeSpacePtr();

    // should add 1 because the free space ptr is pointing at an actual byte, But who cares.
    //        ^
    // ........
    return free_space_ptr - end_of_slots_ptr;
}

// returns 0 in case of success or 1 otherwise.
int TableDataPage::getRecord(char** rec_data, uint32_t* size, uint32_t slot_idx){
    // out of bound error.
    if(slot_idx >= getNumOfSlots())  return 1;
    size_t slot_offset = SLOT_ARRAY_OFFSET_ + (slot_idx * SLOT_ENTRY_SIZE_);
    uint32_t record_offset = *reinterpret_cast<uint32_t*>(getPtrTo(slot_offset));
    // access of a deleted record.
    if(record_offset == 0) return 1;

    uint32_t tmp = *reinterpret_cast<uint32_t*>(getPtrTo(slot_offset + ( SLOT_ENTRY_SIZE_ / 2 )));
    memcpy(size, &tmp, sizeof(uint32_t));

    *rec_data = getPtrTo(record_offset);
    return 0;
}

// slot_idx (output). 
// returns 1 in case of error.
int TableDataPage::insertRecord(char* rec_data, uint32_t rec_size, uint32_t* slot_idx){
    if(getFreeSpaceSize() < rec_size) return 1; 
    bool found_empty_slot = false;
    // search for free slots.
    for(uint32_t i = 0; i < getNumOfSlots(); ++i){
        size_t slot_offset = SLOT_ARRAY_OFFSET_ + (i * SLOT_ENTRY_SIZE_);
        uint32_t record_offset = *reinterpret_cast<uint32_t*>(getPtrTo(slot_offset));
        if(record_offset == 0){
            found_empty_slot = true;
            *slot_idx = i;
            break;
        }
    }

    size_t slot_offset;
    if(found_empty_slot){
        slot_offset = SLOT_ARRAY_OFFSET_ + ((*slot_idx) * SLOT_ENTRY_SIZE_);
    } else if(!found_empty_slot && getFreeSpaceSize() >= rec_size + SLOT_ENTRY_SIZE_){
        // new slot at the end of the array and update the slot count.
        *slot_idx = getNumOfSlots();
        slot_offset = SLOT_ARRAY_OFFSET_ + ((*slot_idx) * SLOT_ENTRY_SIZE_);
        setNumOfSlots(getNumOfSlots()+1);
    } else return 1;
    // at this point, there was a free slot or we alocated a new one,
    // either way, its offset is inside of the slot_offset variable. 
    // if we didn't reach this point, that means no free space available.

    auto new_free_space_offset = getFreeSpaceOffset() - rec_size;
    char* new_record_ptr = getFreeSpacePtr() - rec_size;
    memcpy(new_record_ptr, rec_data, rec_size);
    setFreeSpaceOffset(new_free_space_offset-1);
    // update the slot array to the new free ptr. (sounds weird but correct),
    // the slot array grows this way ----->> <<----- the free space grows that way,
    // starting from the last inserted record.
    memcpy(getPtrTo(slot_offset), &new_free_space_offset, sizeof(new_free_space_offset));
    memcpy(getPtrTo(slot_offset)+(SLOT_ENTRY_SIZE_/2), &rec_size, sizeof(rec_size));
    return 0;
}

// 0 in case of success 1 otherwise.
int TableDataPage::deleteRecord(uint32_t slot_idx){
    // slot is already deleted or invalid slot index.
    if(slot_idx >= getNumOfSlots())  return 1;
    size_t slot_offset = SLOT_ARRAY_OFFSET_ + (slot_idx * SLOT_ENTRY_SIZE_);
    uint32_t record_offset = *reinterpret_cast<uint32_t*>(getPtrTo(slot_offset));
    uint32_t record_size = *reinterpret_cast<uint32_t*>(getPtrTo(slot_offset + (SLOT_ENTRY_SIZE_ / 2)));
    if(record_offset == 0) return 1;
    // mark the slot pointer and size as 0.
    memset(getPtrTo(slot_offset), 0, SLOT_ENTRY_SIZE_);
    // shift everything starting from the free pointer by the size of the deleted record.
    memmove(getFreeSpacePtr()+record_size, getFreeSpacePtr(), getPtrTo(record_offset) - getFreeSpacePtr());
    // update the slot array with new positions.
    for(uint32_t i = 0; i < getNumOfSlots(); ++i){
        size_t cur_slot_offset = SLOT_ARRAY_OFFSET_ + (i * SLOT_ENTRY_SIZE_);
        uint32_t cur_record_offset = *reinterpret_cast<uint32_t*>(getPtrTo(slot_offset));
        if(cur_record_offset != 0 && cur_record_offset < record_offset){
            memset(getPtrTo(cur_slot_offset), cur_record_offset + record_size, SLOT_ENTRY_SIZE_/2);
        }
    }

    return 0;
}
