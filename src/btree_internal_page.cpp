#pragma once
#include "./btree_page.cpp"
#include "math.h"
#include "page.cpp"

struct IndexKey;

#define INTERNAL_PAGE_HEADER_SIZE 24
#define INTERNAL_PAGE_SIZE ((PAGE_SIZE - INTERNAL_PAGE_HEADER_SIZE) / (sizeof(std::pair<IndexKey, PageID>)))
/**
 * Store n indexed keys and n+1 child pointers (page_id) within internal page.
 * Pointer PAGE_ID(i) points to a subtree in which all keys K satisfy:
 * K(i) <= K < K(i+1).
 * NOTE: since the number of keys does not equal to number of child pointers,
 * the first key always remains invalid. That is to say, any search/lookup
 * should ignore the first key.
 *
 * Internal page format (keys are stored in increasing order):
 *  --------------------------------------------------------------------------
 * | HEADER | KEY(1)+PAGE_ID(1) | KEY(2)+PAGE_ID(2) | ... | KEY(n)+PAGE_ID(n) |
 *  --------------------------------------------------------------------------
 */
class BTreeInternalPage : public BTreePage {
 public:
  void Init(PageID page_id, PageID parent_id = INVALID_PAGE_ID, int max_size = INTERNAL_PAGE_SIZE);

  IndexKey KeyAt(int index);
  void SetKeyAt(int index, IndexKey key);
  void SetValAt(int index, const PageID &v);
  PageID ValueAt(int index) const;
  PageID NextPage(IndexKey key);

  bool IsFull() const;
  bool Insert(IndexKey k, PageID v);

  int InsertionPosition(IndexKey k);
  int NextPageOffset(IndexKey k);
  int PrevPageOffset(IndexKey k);

  bool TooShort() const;
  bool TooShortBefore() const;
  void InsertKeyAtStart(IndexKey k, PageID start_val);
  void RemoveFromStart();

 private:
  std::pair<IndexKey, PageID> array_[1];
};

void BTreeInternalPage::Init(PageID page_id, PageID parent_id, int max_size) {
  SetPageType(BTreePageType::INTERNAL_PAGE);
  SetSize(1);
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetMaxSize(max_size);
}

IndexKey BTreeInternalPage::KeyAt(int index) {
  return array_[index].first;
}


void BTreeInternalPage::SetKeyAt(int index, IndexKey key) { array_[index].first = key; }


auto BTreeInternalPage::IsFull() const -> bool { return GetSize() >= GetMaxSize(); }


auto BTreeInternalPage::TooShort() const -> bool {
  // root page is special.
  if (IsRootPage()) {
    return GetSize() <= 1;
  }
  return GetSize() < std::ceil(static_cast<float>(GetMaxSize()) / 2);
}


auto BTreeInternalPage::TooShortBefore() const -> bool {
  if (IsRootPage()) {
    return GetSize() - 1 <= 1;
  }
  return (GetSize() - 1) < std::ceil(static_cast<float>(GetMaxSize()) / 2);
}


void BTreeInternalPage::SetValAt(int index, const PageID &v) { array_[index].second = v; }

PageID BTreeInternalPage::ValueAt(int index) const  { return array_[index].second; }


PageID BTreeInternalPage::NextPage(IndexKey key){
  int size = GetSize();
  int mid;
  int low = 1;
  int high = size;
  while (low < high) {
    mid = low + (high - low) / 2;

    if (!(key > array_[mid].first)) {
      high = mid;
    } else {
      low = mid + 1;
    }
  }
  if (low < size && array_[low].first < key) {
    low++;
  }
  int cur = low;

  if (cur != 0) {
    cur--;
  }
  return array_[cur].second;
}


int BTreeInternalPage::InsertionPosition(IndexKey k) {
  int size = GetSize();
  int mid;
  int low = 1;
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


int BTreeInternalPage::NextPageOffset(IndexKey k) {
  int size = GetSize();
  int mid;
  int low = 1;
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
  if (cur != 0) {
    cur--;
  }
  return cur + 1;
}


int BTreeInternalPage::PrevPageOffset(IndexKey k) {
  int size = GetSize();
  int mid;
  int low = 1;
  int high = size;
  while (low < high) {
    mid = low + (high - low) / 2;

    if (!(k > array_[mid].first)) {
      high = mid;
    } else {
      low = mid + 1;
    }
  }
  if (low < size && array_[low].first <  k) {
    low++;
  }
  int cur = low;
  if (cur != 0) {
    cur--;
  }
  return cur - 1;
}


void BTreeInternalPage::InsertKeyAtStart(IndexKey k, PageID start_val) {
  int sz = GetSize();
  for (int i = sz; i > 0; i--) {
    SetKeyAt(i, KeyAt(i - 1));
    SetValAt(i, ValueAt(i - 1));
  }
  SetKeyAt(1, k);
  SetValAt(0, start_val);
  IncreaseSize(1);
}

void BTreeInternalPage::RemoveFromStart() {
  for (int i = 1; i < GetSize(); i++) {
    SetKeyAt(i - 1, KeyAt(i));
    SetValAt(i - 1, ValueAt(i));
  }
  IncreaseSize(-1);
}


bool BTreeInternalPage::Insert(IndexKey k, PageID v){
  int size = GetSize();
  int mid;
  int low = 1;
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
  array_[size] = {k, v};
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


