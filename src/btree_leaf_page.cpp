#pragma once
#include <vector>
#include <math.h>

#include "btree_page.cpp"
#include "record.cpp"
#include "value.cpp"

#define LEAF_PAGE_HEADER_SIZE 28
#define LEAF_PAGE_SIZE ((PAGE_SIZE - LEAF_PAGE_HEADER_SIZE) / sizeof(RecordID))

/**
 * Store indexed key and record id(record id = page id combined with slot id,
 * see include/common/rid.h for detailed implementation) together within leaf
 * page. Only support unique key.
 *
 * Leaf page format (keys are stored in order):
 *  ----------------------------------------------------------------------
 * | HEADER | KEY(1) + RID(1) | KEY(2) + RID(2) | ... | KEY(n) + RID(n)
 *  ----------------------------------------------------------------------
 *
 *  Header format (size in byte, 28 bytes in total):
 *  ---------------------------------------------------------------------
 * | PageType (4) | LSN (4) | CurrentSize (4) | MaxSize (4) |
 *  ---------------------------------------------------------------------
 *  -----------------------------------------------
 * | ParentPageId (4) | PageId (4) | NextPageId (4)
 *  -----------------------------------------------
 */
class BTreeLeafPage : public BTreePage {
 public:
  // After creating a new leaf page from buffer pool, must call initialize
  // method to set default values
  void Init(PageID page_id, PageID parent_id = INVALID_PAGE_ID, int max_size = LEAF_PAGE_SIZE);
  // helper methods
  auto GetNextPageId() const -> PageID;
  void SetNextPageId(PageID next_page_id);

  auto KeyAt(int index) const -> Value;
  RecordID ValAt(int index) const;
  void SetKeyAt(int index, Value k);
  void SetValAt(int index, RecordID v);
  auto GetValue(Value k, std::vector<RecordID> *result) const -> bool;
  auto GetPos(Value k) const -> int;
  void Draw();

  auto IsFull() const -> bool;
  auto Insert(Value k, RecordID v) -> bool;
  auto TooShort() const -> bool;
  auto TooShortBefore() const -> bool;
  auto Remove(Value k) -> bool;
  auto GetPointer(int pos) -> std::pair<Value, RecordID> &;

 private:
  PageID next_page_id_;
  // Flexible array member for page data.
  std::pair<Value, RecordID> array_[1];
};
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 */
void BTreeLeafPage::Init(PageID page_id, PageID parent_id, int max_size) {
  SetPageType(BTreePageType::LEAF_PAGE);
  SetSize(0);
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetMaxSize(max_size);
  SetNextPageId(INVALID_PAGE_ID);
}

/**
 * Helper methods to set/get next page id
 */
PageID BTreeLeafPage::GetNextPageId() const { return next_page_id_; }

void BTreeLeafPage::SetNextPageId(PageID next_page_id) { next_page_id_ = next_page_id; }

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
auto BTreeLeafPage::KeyAt(int index) const -> Value {
  return array_[index].first;
}

void BTreeLeafPage::SetKeyAt(int index, Value k) {
  array_[index].first = k;
}

RecordID BTreeLeafPage::ValAt(int index) const {
  return array_[index].second;
}

void BTreeLeafPage::SetValAt(int index, RecordID v) {
  array_[index].second = v;
}

bool BTreeLeafPage::IsFull() const { return GetSize() + 1 == GetMaxSize(); }

bool BTreeLeafPage::TooShort() const {
  if (IsRootPage()) {
    return GetSize() <= 1;
  }
  float tmp = GetMaxSize() - 1;
  tmp /= 2;
  tmp = std::ceil(tmp);
  return GetSize() < tmp;
}

bool BTreeLeafPage::TooShortBefore() const {
  if (IsRootPage()) {
    return GetSize() - 1 <= 1;
  }
  return (GetSize() - 1) < std::ceil(static_cast<float>(GetMaxSize() - 1) / 2);
}

bool BTreeLeafPage::GetValue(Value k, std::vector<RecordID> *result) const {
  int size = GetSize();
  int mid;
  int low = 0;
  int high = size;
  while (low < high) {
    mid = low + (high - low) / 2;

    if (!(k > array_[mid].first)) {
      high = mid;
    } else {
      low = mid + 1;
    }
  }
  if (low < size && array_[low].first < k) {
    low++;
  }
  int cur = low;

  if (KeyAt(cur) == k && cur < size) {
    *result = std::vector<RecordID>();
    result->push_back(ValAt(cur));
    return true;
  }
  return false;
}

int BTreeLeafPage::GetPos(Value k) const  {
  int size = GetSize();
  int mid;
  int low = 0;
  int high = size;
  while (low < high) {
    mid = low + (high - low) / 2;

    if (!(k > array_[mid].first)) {
      high = mid;
    } else {
      low = mid + 1;
    }
  }
  if (low < size && array_[low].first < k) {
    low++;
  }
  return low;
}

bool BTreeLeafPage::Insert(Value k, RecordID v){
  int size = GetSize();
  int mid;
  int low = 0;
  int high = size;
  while (low < high) {
    mid = low + (high - low) / 2;

    if (!(k > array_[mid].first)) {
      high = mid;
    } else {
      low = mid + 1;
    }
  }
  if (low < size && array_[low].first < k) {
    low++;
  }
  int cur = low;

  if (cur < size && KeyAt(cur) == k) {
    return false;
  }
  array_[size] = {k,v};
  int tmp_sz = size;
  while (cur < tmp_sz) {
    auto tmp = array_[tmp_sz - 1];
    array_[tmp_sz - 1] = array_[tmp_sz];
    array_[tmp_sz] = tmp;
    tmp_sz--;
  }
  IncreaseSize(1);
  return true;
}

auto BTreeLeafPage::Remove(Value k) -> bool {
  int size = GetSize();
  if (size == 0) {
    return false;
  }
  int mid;
  int low = 0;
  int high = size;
  while (low < high) {
    mid = low + (high - low) / 2;

    if (!(k > array_[mid].first)) {
      high = mid;
    } else {
      low = mid + 1;
    }
  }
  if (low < size && array_[low].first < k) {
    low++;
  }
  int cur = low;

  if (!(KeyAt(cur) == k) || cur >= size) {
    return false;
  }
  while (cur + 1 < size) {
    array_[cur] = array_[cur + 1];
    cur++;
  }
  IncreaseSize(-1);
  return true;
}

void BTreeLeafPage::Draw() {
    // TODO: provide drawing support for varius type.
    /*
  for (int i = 0; i < GetSize(); i++) {
    std::cout << "key: " << array_[i].first << " value: " << array_[i].second << std::endl;
  }*/
}

auto BTreeLeafPage::GetPointer(int pos) -> std::pair<Value, RecordID> & {
  auto ptr = &array_[pos];
  return *ptr;
}
