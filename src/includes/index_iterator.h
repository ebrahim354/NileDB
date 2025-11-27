#ifndef INDEX_ITERATOR_H
#define INDEX_ITERATOR_H


#include "cache_manager.h"
#include "page.h"
#include "btree_leaf_page.h"
#include "record.h"
#include "table_data_page.h"


// read only Iterator for index pages.
class IndexIterator {
    public:
        IndexIterator(CacheManager *cm = nullptr, PageID page_id = INVALID_PAGE_ID, int entry_idx = -1);
        ~IndexIterator();

        void clear();
        bool isNull();

        bool hasNext();
        void assign_to_null_page();
        // 0 in case of no more records.
        int advance();

        IndexKey getCurKey();
        RecordID getCurRecordID();
        Record getCurRecordCpy(Arena* arena);

        bool operator==(IndexIterator& rhs);

    private:
        CacheManager *cache_manager_ = nullptr;
        BTreeLeafPage* cur_page_ = nullptr;
        Page* cur_raw_page_ = nullptr;
        PageID cur_page_id_ = INVALID_PAGE_ID;
        int entry_idx_ = -1;
};

#endif //INDEX_ITERATOR_H
