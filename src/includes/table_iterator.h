#ifndef TABLE_ITERATOR_H
#define TABLE_ITERATOR_H

#include "record.h"
#include "cache_manager.h"
#include "page.h"
#include "table_data_page.h"
#include "overflow_iterator.h"
#include <cstdint>


// read only Iterator for data pages.
class TableIterator {
    public:
        TableIterator(CacheManager *cm, TableSchema* schema, PageID page_id);
        TableIterator();
        void init(); // this method pins the associated page.
        void destroy(); // thiss method releases the page.

        bool hasNext();

        // 0 in case of no more records.
        int advance();

    private:
        Record getCurRecord();
        Record getCurRecordCpy(Arena* arena);
    public:
        int    getCurTupleCpy(Arena& arena, Tuple* out);
        RecordID getCurRecordID();
    private:
        PageID cur_page_id_ = INVALID_PAGE_ID;
        CacheManager *cache_manager_ = nullptr;
        TableSchema* schema_ = nullptr;
        TableDataPage* cur_page_ = nullptr;
        u32 next_page_number_;
        u32 prev_page_number_;
        u32 cur_num_of_slots_;
        i32 cur_slot_idx_ = -1;
};

#endif // TABLE_ITERATOR_H
