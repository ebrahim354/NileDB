#ifndef BTREE_INDEX_H
#define BTREE_INDEX_H

#include <shared_mutex>
#include <deque>
#include "cache_manager.h"
#include "page.h"
#include "record.h"
#include "index_iterator.h"
#include "free_space_map.h"
#include "btree_page.h"
#include "btree_leaf_page.h"
#include "btree_internal_page.h"
#include "table_schema.h"

class BTreeIndex {
    public:
        void init(CacheManager* cm, FileID fid, PageID root_page_id, TableSchema* index_meta_schema);
        void destroy();

        void update_index_root(FileID fid, PageNum new_root_pid);
        void SetRootPageId(PageID root_page_id, int insert_record = 0);
        // true means value is returned.
        bool GetValue(IndexKey &key, std::vector<RecordID> *result);
        // new_page_raw (output).
        BTreeLeafPage* create_leaf_page(PageID parent_pid, Page** new_page_raw);
        // new_page_raw (output).
        BTreeInternalPage* create_internal_page(PageID parent_pid, Page** new_page_raw);
        // return true if inserted successfully.
        bool Insert(const IndexKey &key, const RecordID &value);
        void Remove(const IndexKey &key);
        IndexIterator begin();
        // for range queries
        IndexIterator begin(const IndexKey &key);
        IndexIterator end();
        void See();
        void ToString(BTreePage* page);

    private:
        bool isEmpty();
        CacheManager* cache_manager_ = nullptr;
        TableSchema*  index_meta_schema_ = nullptr;
        FileID fid_                  = -1;
        PageID root_page_id_         = INVALID_PAGE_ID;

        std::shared_mutex root_page_id_lock_;
};

#endif //BTREE_INDEX_H
