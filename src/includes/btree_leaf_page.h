#ifndef BTREE_LEAF_PAGE_H
#define BTREE_LEAF_PAGE_H


#include "btree_page.h"
#include "record.h"

class BTreeLeafPage : public BTreePage {
    public:
        void Init(PageID page_id);

        PageID GetNextPageId(FileID fid);
        void SetNextPageId(PageID next_page_id);
        PageNum get_next_page_number();
        void set_next_page_number(PageNum next_page_num);

        //bool split_with_and_insert(BTreeLeafPage* new_page, IndexKey k, RecordID v);
        bool split_with_and_insert(Arena* arena, i32 nvals, bool unique_insertion,
                                    BTreeLeafPage* new_page, IndexKey k);
        inline IndexKey get_last_key_cpy(Arena* arena, int elements_to_chop);

        bool IsFull(IndexKey k);

        //IndexKey KeyAt(int index);
        //RecordID ValAt(int index);
        //void SetValAt(int index, RecordID v);
        //bool GetValue(IndexKey k, Vector<RecordID> *result);
        int GetPos(IndexKey k);
        int get_pos_upper_bound(IndexKey k);

        //bool Insert(IndexKey key, RecordID v);
        bool Insert(Arena* arena, IndexKey input_k, i32 nvals, bool is_unique);
        bool Remove(IndexKey k);
        //std::pair<IndexKey, RecordID> getPointer(int pos);
        IndexKey getPointer(int pos);
};

#endif //BTREE_LEAF_PAGE_H
