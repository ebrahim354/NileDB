#pragma once
#include "btree_page.cpp"
#include "math.h"
#include "page.cpp"
#include "btree_internal_page.h"



bool BTreeInternalPage::IsFull(IndexKey k) { 
    return (INTERNAL_SLOT_ENTRY_SIZE_ + k.size_ >= get_free_space_size());
}

void BTreeInternalPage::Init(PageID page_id, PageID parent_id) {
  SetPageType(BTreePageType::INTERNAL_PAGE);
  SetPageId(page_id);
  SetParentPageId(parent_id);
  set_free_space_offset(PAGE_SIZE - 1);
  increase_size(1);
  //SetMaxSize(max_size);
}

/*
auto BTreeInternalPage::TooShortBefore() const -> bool {
  if (IsRootPage()) {
    return get_num_of_slots() - 1 <= 1;
  }
  return (GetSize() - 1) < std::ceil(static_cast<float>(GetMaxSize()) / 2);
}*/


void BTreeInternalPage::SetValAt(int index, const PageID &v) { 
  char* ptr = get_val_ptr(index);
  if(!ptr) return;
  memcpy(ptr, &v.page_num_, sizeof(v.page_num_));
}

PageID BTreeInternalPage::ValueAt(int index, FileID fid) { 
  char* ptr = get_val_ptr(index);
  if(!ptr) return INVALID_PAGE_ID;
  return {
    .fid_ = fid,
    .page_num_ = *(PageNum*)ptr,
  };
}


PageID BTreeInternalPage::NextPage(IndexKey key, FileID fid){
  int size = get_num_of_slots();
  int mid;
  int low = 1;
  int high = size;
  while (low < high) {
    mid = low + (high - low) / 2;

    //if (!(key > array_[mid].first)) {
    if (!(key > KeyAt(mid))) {
      high = mid;
    } else {
      low = mid + 1;
    }
  }
  //if (low < size && array_[low].first < key) {
  if (low < size && KeyAt(low) < key) {
    low++;
  }
  int cur = low;

  if (cur != 0) {
    cur--;
  }
  //return array_[cur].second;
  return ValueAt(cur, fid);
}


int BTreeInternalPage::InsertionPosition(IndexKey k) {
  int size = get_num_of_slots();
  int mid;
  int low = 1;
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


int BTreeInternalPage::NextPageOffset(IndexKey k) {
  int size = get_num_of_slots();
  int mid;
  int low = 1;
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
  if (cur != 0) {
    cur--;
  }
  return cur + 1;
}


int BTreeInternalPage::PrevPageOffset(IndexKey k) {
  int size = get_num_of_slots();
  int mid;
  int low = 1;
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
  // if (low < size && array_[low].first <  k) {
  if (low < size && KeyAt(low) <  k) {
    low++;
  }
  int cur = low;
  if (cur != 0) {
    cur--;
  }
  return cur - 1;
}

/*

void BTreeInternalPage::InsertKeyAtStart(IndexKey k, PageID start_val) {
  int sz = get_num_of_slots();
  for (int i = sz; i > 0; i--) {
    SetKeyAt(i, KeyAt(i - 1));
    SetValAt(i, ValueAt(i - 1));
  }
  SetKeyAt(1, k);
  SetValAt(0, start_val);
  increase_size(1);
} */

void BTreeInternalPage::insert_key_at_start(Arena* arena, IndexKey key, PageID start_val) {
  int sz = get_num_of_slots();
  int entry_sz = INTERNAL_SLOT_ENTRY_SIZE_;
  // move the slot array by 1 slot.
  if(entry_sz + key.size_ > get_free_space_size()) return; // no space.
  memmove(get_ptr_to(SLOT_ARRAY_OFFSET_) + entry_sz, get_ptr_to(SLOT_ARRAY_OFFSET_), sz*entry_sz);
  increase_size(1);
  SetKeyAt(1, key);
  if(key.data_)
    SetKeyAt(0, null_index_key(arena, *(key.data_)));
  SetValAt(0, start_val);
}

void BTreeInternalPage::remove_from_start() {
  int sz = get_num_of_slots();
  int entry_sz = INTERNAL_SLOT_ENTRY_SIZE_;
  if(sz <= 1) return;

  // move the slot array back by 1 slot.
  memmove(get_ptr_to(SLOT_ARRAY_OFFSET_), get_ptr_to(SLOT_ARRAY_OFFSET_)+entry_sz, (sz-1)*entry_sz);
  increase_size(-1);
}


/*
void BTreeInternalPage::RemoveFromStart() {
  int sz = get_num_of_slots();
  if(sz <= 1) return;
  for (int i = 1; i < sz; i++) {
    SetKeyAt(i - 1, KeyAt(i));
    SetValAt(i - 1, ValueAt(i));
  }
  increase_size(-1);
}*/


bool BTreeInternalPage::Insert(IndexKey k, PageID v){
  int size = get_num_of_slots();
  int entry_sz = INTERNAL_SLOT_ENTRY_SIZE_;
  if(entry_sz + k.size_ > get_free_space_size()) return false; // no space.


  int mid;
  int low = 1;
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

  if(cur < size){
    memmove(get_ptr_to(SLOT_ARRAY_OFFSET_) + 
        (entry_sz * cur) + entry_sz, 
        get_ptr_to(SLOT_ARRAY_OFFSET_) +
        (entry_sz * cur), 
        (size-cur)*entry_sz);
  }
  increase_size(1);
  SetKeyAt(cur, k);
  SetValAt(cur, v);
  /*
  array_[size] = {k, v};
  int tmp_sz = size;
  while (cur < tmp_sz) {
    auto tmp = array_[tmp_sz - 1];
    array_[tmp_sz - 1] = array_[tmp_sz];
    array_[tmp_sz] = tmp;
    tmp_sz--;
  }
  increase_size(1);
  */
  return true;
}


