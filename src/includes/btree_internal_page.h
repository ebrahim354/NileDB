#ifndef BTREE_INTERNAL_PAGE_H
#define BTREE_INTERNAL_PAGE_H

#include "btree_page.h"
#include "math.h"
#include "page.h"

struct IndexKey;

class BTreeInternalPage : public BTreePage {
 public:
  void Init(PageID page_id, PageID parent_id = INVALID_PAGE_ID);
  void insert_key_at_start(Arena* arena, IndexKey key, PageID);
  void remove_from_start();
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

  //void InsertKeyAtStart(IndexKey k, PageID start_val);
  //void RemoveFromStart();
};

#endif //BTREE_INTERNAL_PAGE_H
