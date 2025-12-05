#ifndef TABLE_DATA_PAGE_H
#define TABLE_DATA_PAGE_H

#include "page.h"
#include <cstdint>
#include <cstring>


#define TABLE_PAGE_HEADER_SIZE 20
#define TABLE_SLOT_ENTRY_SIZE 8

class TableDataPage : public Page {
    public:
        // assumes that the ResetMemory function is called before this by the cache manager.
        void init();
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
        // return value should not be negative.
        size_t   getUsedSpaceSize();
        // returns 0 in case of success or 1 otherwise.
        int      getRecord(char** rec_data, uint32_t* size, uint32_t slot_idx);
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
        char*    getPtrTo(size_t offset);
        static const size_t PAGE_NUMBER_OFFSET_ = 0;
        static const size_t PREV_PAGE_NUMBER_OFFSET_ = 4;
        static const size_t NEXT_PAGE_NUMBER_OFFSET_ = 8;
        static const size_t FREE_SPACE_PTR_OFFSET_ = 12;
        static const size_t NUMBER_OF_SLOTS_OFFSET_ = 16;
        static const size_t SLOT_ARRAY_OFFSET_ = 20; 
        static const size_t SLOT_ENTRY_SIZE_ = TABLE_SLOT_ENTRY_SIZE; 
        /* 
         * first entry of the slot array.
         * each entry is 8 bytes long 
         * first  4 bytes => pointer to the starting position of the record
         * assigned to 0 in case of deleted records.
         * second 4 bytes => the size of the record.
         * 
         * */
};

#endif // TABLE_DATA_PAGE_H
