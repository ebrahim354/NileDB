#pragma once
#include "page.cpp" 
#include <cstdint>
#include <cstring>


class TableDataPage : public Page {
    public:
        void setPageNumber(uint32_t page_number);
        void setPrevPageNumber(uint32_t prev_page_number);
        void setNextPageNumber(uint32_t next_page_number);
        void setFreeSpaceOffset(uint32_t free_space_ptr);
        void setNumOfSlots(uint32_t slot_cnt);
        uint32_t getPageNumber();
        uint32_t getPrevPageNumber(); // 0 in case of first page.
        uint32_t getNextPageNumber(); // 0 in case of last  page.
        uint32_t getFreeSpaceOffset();
        uint32_t getNumOfSlots();
        char*    getFreeSpacePtr();
        // return value should not be negative.
        size_t   getFreeSpaceSize();
        // returns 0 in case of success or 1 otherwise.
        int      getRecord(char* rec_data, uint32_t slot_idx);
        // slot_idx (output). 
        // returns 1 in case of error.
        int      insertRecord(char* rec_data, uint32_t rec_size, uint32_t* slot_idx);
        // 0 in case of success 1 otherwise.
        int      deleteRecord(uint32_t slot_idx);
        // updating the record is the responsibility of the user of this class:
        // in case of modifying values without modifying the size the user can 
        // update the data anyway via the pointer, in case of modifying the size
        // the user should copy the data -> delete the record -> re-insert the new record.

    private:
        char*    getPtrTo(size_t offset){
            return data_+offset;
        }
        static const size_t PAGE_NUMBER_OFFSET_ = 0;
        static const size_t PREV_PAGE_NUMBER_OFFSET_ = 4;
        static const size_t NEXT_PAGE_NUMBER_OFFSET_ = 8;
        static const size_t FREE_SPACE_PTR_OFFSET_ = 12;
        static const size_t NUMBER_OF_SLOTS_OFFSET_ = 16;
        static const size_t SLOT_ARRAY_OFFSET_ = 20; 
        static const size_t SLOT_ENTRY_SIZE_ = 8; 
        /* 
         * first entry of the slot array.
         * each entry is 8 bytes long 
         * first  4 bytes => pointer to the starting position of the record
         * assigned to 0 in case of deleted records.
         * second 4 bytes => the size of the record.
         * 
         * */
};



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

    return free_space_ptr - end_of_slots_ptr;
}

// returns 0 in case of success or 1 otherwise.
int TableDataPage::getRecord(char* rec_data, uint32_t slot_idx){
    // out of bound error.
    if(slot_idx >= getNumOfSlots())  return 1;
    size_t slot_offset = SLOT_ARRAY_OFFSET_ + (slot_idx * SLOT_ENTRY_SIZE_);
    uint32_t record_offset = *reinterpret_cast<uint32_t*>(getPtrTo(slot_offset));
    // access of a deleted record.
    if(record_offset == 0) return 1;

    rec_data = getPtrTo(record_offset);
    return 0;
}

// slot_idx (output). 
// returns 1 in case of error.
int TableDataPage::insertRecord(char* rec_data, uint32_t rec_size, uint32_t* slot_idx){
    if(getFreeSpaceSize() < rec_size) return 1; 
    bool found_empty_slot = false;
    // search for free slots.
    for(int i = 0; i < getNumOfSlots(); ++i){
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
    setFreeSpaceOffset(new_free_space_offset);
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
    for(int i = 0; i < getNumOfSlots(); ++i){
        size_t cur_slot_offset = SLOT_ARRAY_OFFSET_ + (i * SLOT_ENTRY_SIZE_);
        uint32_t cur_record_offset = *reinterpret_cast<uint32_t*>(getPtrTo(slot_offset));
        if(cur_record_offset != 0 && cur_record_offset < record_offset){
            memset(getPtrTo(cur_slot_offset), cur_record_offset + record_size, SLOT_ENTRY_SIZE_/2);
        }
    }

    return 0;
}
