#ifndef BTREE_LEAF_PAGE_H
#define BTREE_LEAF_PAGE_H


#include <vector>
#include <math.h>

#include "btree_page.h"
#include "record.h"

class BTreeLeafPage : public BTreePage {
 public:
  void Init(PageID page_id, PageID parent_id = INVALID_PAGE_ID);

  PageID GetNextPageId(FileID fid);
  void SetNextPageId(PageID next_page_id);
  PageNum get_next_page_number();
  void set_next_page_number(PageNum next_page_num);

  bool split_with_and_insert(BTreeLeafPage* new_page, IndexKey k, RecordID v);
  inline IndexKey get_last_key_cpy(Arena* arena);

  bool IsFull(IndexKey k);

  //IndexKey KeyAt(int index);
  RecordID ValAt(int index);
  void SetValAt(int index, RecordID v);
  bool GetValue(IndexKey k, std::vector<RecordID> *result);
  int GetPos(IndexKey k);
  void Draw();

  bool Insert(IndexKey key, RecordID v);
  bool Remove(IndexKey k);
  std::pair<IndexKey, RecordID> getPointer(int pos);
};

#endif //BTREE_LEAF_PAGE_H
