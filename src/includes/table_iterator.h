#ifndef TABLE_ITERATOR_H
#define TABLE_ITERATOR_H

#include "record.h"
#include "cache_manager.h"
#include "page.h"
#include "table_data_page.h"
#include <cstdint>


// read only Iterator for data pages.
class TableIterator {
    public:
        TableIterator(CacheManager *cm, PageID page_id);
        TableIterator();
        void init(); // this method pins the associated page.
        void destroy(); // thiss method releases the page.

        bool hasNext();

        // 0 in case of no more records.
        int advance();

        Record getCurRecord();
        Record getCurRecordCpy(Arena* arena);
        RecordID getCurRecordID();
    private:
        CacheManager *cache_manager_ = nullptr;
        PageID cur_page_id_ = INVALID_PAGE_ID;
        TableDataPage* cur_page_ = nullptr;
        uint32_t next_page_number_;
        uint32_t prev_page_number_;
        uint32_t cur_num_of_slots_;
        int32_t cur_slot_idx_ = -1;
};

#endif // TABLE_ITERATOR_H
