#pragma once
#include <vector>
#include <math.h>

#include "btree_page.cpp"
#include "record.cpp"

class BTreeLeafPage : public BTreePage {
 public:
  void Init(PageID page_id, PageID parent_id = INVALID_PAGE_ID);

  PageID GetNextPageId(FileID fid);
  void SetNextPageId(PageID next_page_id);
  PageNum get_next_page_number();
  void set_next_page_number(PageNum next_page_num);


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

void BTreeLeafPage::Init(PageID page_id, PageID parent_id) {
  SetPageType(BTreePageType::LEAF_PAGE);
  SetPageId(page_id);
  SetParentPageId(parent_id);
  set_next_page_number(INVALID_PAGE_NUM);
  set_free_space_offset_ptr(PAGE_SIZE - 1);
}


PageID BTreeLeafPage::GetNextPageId(FileID fid) { 
  return {
    .fid_ = fid,
    .page_num_ = get_next_page_number(),
  };
}

void BTreeLeafPage::SetNextPageId(PageID pid) {
  set_next_page_number(pid.page_num_);
}

PageNum BTreeLeafPage::get_next_page_number() { 
  return *(PageNum*)get_ptr_to(NEXT_PAGE_NUMBER_OFFSET_); 
}

void BTreeLeafPage::set_next_page_number(PageNum next_page_num) { 
  auto ptr = (PageNum*)get_ptr_to(NEXT_PAGE_NUMBER_OFFSET_);
  *ptr = next_page_num;
}


RecordID BTreeLeafPage::ValAt(int index){
  return *(RecordID*)get_val_ptr(index);
}

void BTreeLeafPage::SetValAt(int index, RecordID v) {
  *(RecordID*)get_val_ptr(index) = v;
}


bool BTreeLeafPage::GetValue(IndexKey k, std::vector<RecordID> *result) {
  int size = get_num_of_slots();
  int mid;
  int low = 0;
  int high = size;
  while (low < high) {
    mid = low + (high - low) / 2;

    //if (!(k > array_[mid].first)) {
    if (!(k > KeyAt(mid))) {
      high = mid;
    } else {
      low = mid + 1;
    }
  }
  //if (low < size && array_[low].first < k) {
  if (low < size && KeyAt(low) < k) {
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

int BTreeLeafPage::GetPos(IndexKey k) {
  int size = get_num_of_slots();
  int mid;
  int low = 0;
  int high = size;
  while (low < high) {
    mid = low + (high - low) / 2;

    //if (!(k > array_[mid].first)) {
    if (!(k > KeyAt(mid))) {
      high = mid;
    } else {
      low = mid + 1;
    }
  }
  //if (low < size && array_[low].first < k) {
  if (low < size && KeyAt(low) < k) {
    low++;
  }
  return low;
}

bool BTreeLeafPage::Insert(IndexKey k, RecordID v){
  int size = get_num_of_slots();
  int entry_sz = LEAF_SLOT_ENTRY_SIZE_;
  if(entry_sz + k.size_ > get_free_space_size()) return false; // no space.


  int mid;
  int low = 0;
  int high = size;
  while (low < high) {
    mid = low + (high - low) / 2;

    //if (!(k > array_[mid].first)) {
    if (!(k > KeyAt(mid))) {
      high = mid;
    } else {
      low = mid + 1;
    }
  }
  //if (low < size && array_[low].first < k) {
  if (low < size && KeyAt(low) < k) {
    low++;
  }
  int cur = low;

  if (cur < size && KeyAt(cur) == k) {
    return false;
  }
  memmove(get_ptr_to(SLOT_ARRAY_OFFSET_) + 
          (entry_sz * cur) + entry_sz, 
          get_ptr_to(SLOT_ARRAY_OFFSET_) +
          (entry_sz * cur), 
          (1+size-cur)*entry_sz);
  increase_size(1);
  SetKeyAt(cur, k);
  SetValAt(cur, v);
  /*
  array_[size] = {k,v};
  int tmp_sz = size;
  while (cur < tmp_sz) {
    auto tmp = array_[tmp_sz - 1];
    array_[tmp_sz - 1] = array_[tmp_sz];
    array_[tmp_sz] = tmp;
    tmp_sz--;
  }
  IncreaseSize(1);
  */
  return true;
}

bool BTreeLeafPage::Remove(IndexKey k){
  int size = get_num_of_slots();
  int entry_sz = LEAF_SLOT_ENTRY_SIZE_;

  if (size == 0) {
    return false;
  }
  int mid;
  int low = 0;
  int high = size;
  while (low < high) {
    mid = low + (high - low) / 2;

    //if (!(k > array_[mid].first)) {
    if (!(k > KeyAt(mid))) {
      high = mid;
    } else {
      low = mid + 1;
    }
  }
  // if (low < size && array_[low].first < k) {
  if (low < size && KeyAt(low) < k) {
    low++;
  }
  int cur = low;

  if (!(KeyAt(cur) == k) || cur >= size) {
    return false;
  }

  memmove(get_ptr_to(SLOT_ARRAY_OFFSET_) + 
          (entry_sz * cur), 
          get_ptr_to(SLOT_ARRAY_OFFSET_) +
          (entry_sz * cur) + entry_sz, 
          (size-cur)*entry_sz);
  increase_size(-1);
  /*
  while (cur + 1 < size) {
    array_[cur] = array_[cur + 1];
    cur++;
  }
  IncreaseSize(-1);
  */
  /* TODO: clean up key payloads.*/
  return true;
}

void BTreeLeafPage::Draw() {
    // TODO: provide drawing support for varius type.
    /*
  for (int i = 0; i < GetSize(); i++) {
    std::cout << "key: " << array_[i].first << " value: " << array_[i].second << std::endl;
  }*/
}

std::pair<IndexKey, RecordID> BTreeLeafPage::getPointer(int pos) {
    if(pos >= get_num_of_slots()) return {};
    return {KeyAt(pos), ValAt(pos)};
}
