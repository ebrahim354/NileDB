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
#include "query_ctx.h"

#define TABLE_BTREE_NVALS 2

class BTreeIndex {
    public:
        void init(CacheManager* cm, FileID fid, PageID root_page_id,
                TableSchema* index_meta_schema, int nvals, bool is_unique);
        void destroy();

        void update_index_root(QueryCTX* ctx, FileID fid, PageNum new_root_pid);
        void SetRootPageId(QueryCTX* ctx, PageID root_page_id, int insert_record = 0);
        // true means value is returned.
        //bool GetValue(QueryCTX* ctx, IndexKey &key, Vector<RecordID> *result);
        // new_page_raw (output).
        BTreeLeafPage* create_leaf_page(PageID parent_pid, Page** new_page_raw);
        // new_page_raw (output).
        BTreeInternalPage* create_internal_page(PageID parent_pid, Page** new_page_raw);
        // return true if inserted successfully.
        //bool Insert(QueryCTX* ctx, const IndexKey &key, const RecordID &value);
        bool Insert(QueryCTX* ctx, const IndexKey &key);
        void Remove(QueryCTX* ctx, const IndexKey &key);
        IndexIterator begin();
        // for range queries
        IndexIterator begin(const IndexKey &key);
        IndexIterator end();
        void See();
        void ToString(BTreePage* page);
        FileID get_fid();

    private:
        bool isEmpty();
        CacheManager* cache_manager_ = nullptr;
        TableSchema*  index_meta_schema_ = nullptr;
        FileID fid_                  = -1;
        PageID root_page_id_         = INVALID_PAGE_ID;
        std::shared_mutex root_page_id_lock_;

        int index_nvals_ = -1;
        bool is_unique_index_ = false;
        // index_nvals_ => indicates the number of elements in the IndexKey that represent a value,
        // value is always at the end of the key and the key itself is at the start.
        // TODO: the name IndexKey is confusing so I should call it IndexCell
        // and it should provide an interface for getting key,value pairs.
        //
        // this mechanism helps the index to be more general.
        //
        // if the index is unique we have two cases:
        //
        // 1- nvals = 0 that means the index works as std::set.
        // 2- nvals > 0 that means the index works as std::map.
        //
        // if the index is not-unique we have two cases:
        //
        // 1- nvals = 0 that means the index works as std::multi_set,
        // this case is not supported by the index itself,
        // so the user must provide some dummy unique value and ignore it,
        // for example: in an executor the user could append the tuple with some curser-id as the value.
        //
        // 2- nvals > 0 that means the index works as std::multi_map.
        // this case only works for unique(key, value) combinations,
        // if the user knows that the values are always different it works perfectly,
        // for example: 
        // building a non-unique index on top of a table where the value is always a unique RecordID.
        // otherwise the technique from the previuse case must be used.
};

#endif //BTREE_INDEX_H
