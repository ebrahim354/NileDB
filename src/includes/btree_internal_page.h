#ifndef BTREE_INTERNAL_PAGE_H
#define BTREE_INTERNAL_PAGE_H

#include "btree_page.h"
#include "math.h"
#include "page.h"

struct IndexKey;

class BTreeInternalPage : public BTreePage {
    public:
        void Init(PageID page_id, PageID parent_id = INVALID_PAGE_ID);

        // insert the first entry ever since the creation of the page 
        // (doesn't do any offsetting of the slot array).
        // the first entry has to be 2 page ids for left and right pages and the key itself.
        //          first_key
        //         /         \
        // left_page      right_page
        void insert_first_entry(PageID lpage, IndexKey key, PageID rpage);

        // insert a key at the start of the page 
        // (offsets the slot array and doesn't touch the left most page id).
        void insert_key_at_start(Arena* arena, IndexKey key, PageID);
        void remove_from_start();
        void remove_entry_at(int pos);
        void SetValAt(int index, const PageID &v);
        PageID NextPage(IndexKey key, FileID fid);
        PageID ValueAt(int index, FileID fid);
        int InsertionPosition(IndexKey k);

        bool IsFull(IndexKey k);
        /*
           bool TooShortBefore() const;
           */
        bool Insert(IndexKey key, PageID v);

        int NextPageOffset(IndexKey k);
        int PrevPageOffset(IndexKey k);
};

#endif //BTREE_INTERNAL_PAGE_H
