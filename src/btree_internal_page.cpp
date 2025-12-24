#pragma once
#include "btree_page.cpp"
#include "math.h"
#include "page.cpp"
#include "btree_internal_page.h"



bool BTreeInternalPage::IsFull(IndexKey k) { 
    u64 ksz = normalize_index_key_size(k);
    // 9 for worst case varint + 4 for the overflow page number
    if(ksz != k.size_)
        ksz += 9 + 4; 
    return (INTERNAL_SLOT_ENTRY_SIZE_ + ksz >= get_free_space_size());
}

void BTreeInternalPage::Init(PageID page_id, PageID parent_id) {
  SetPageType(BTreePageType::INTERNAL_PAGE);
  SetPageId(page_id);
  //SetParentPageId(parent_id);
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

        if (!(key > KeyAt(mid))) {
            high = mid;
        } else {
            low = mid + 1;
        }
    }
    if (low < size && KeyAt(low) < key) {
        low++;
    }
    int cur = low;

    if (cur != 0) {
        cur--;
    }
    return ValueAt(cur, fid);
}


int BTreeInternalPage::InsertionPosition(IndexKey k) {
    int size = get_num_of_slots();
    int mid;
    int low = 1;
    int high = size;
    while (low < high) {
        mid = low + (high - low) / 2;

        if (!(k > KeyAt(mid))) {
            high = mid;
        } else {
            low = mid + 1;
        }
    }
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

        if (!(k > KeyAt(mid))) {
            high = mid;
        } else {
            low = mid + 1;
        }
    }
    if (low < size && KeyAt(low) < k) {
        low++;
    }
    int cur = low;
    if (cur != 0) {
        cur--;
    }
    if(cur + 1 >= get_num_of_slots()) {
        std::cout << "last pos\n";
        return -1;
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

        if (!(k > KeyAt(mid))) {
            high = mid;
        } else {
            low = mid + 1;
        }
    }
    if (low < size && KeyAt(low) <  k) {
        low++;
    }
    int cur = low;
    if (cur != 0) {
        cur--;
    }
    return cur - 1;
}


void BTreeInternalPage::insert_first_entry(PageID lpage, IndexKey key, PageID rpage) {
    set_num_of_slots(2);
    SetValAt(0, lpage);
    insert_cell_at(1, key);
    SetValAt(1, rpage);
}

void BTreeInternalPage::insert_key_at_start(Arena* arena, IndexKey key, PageID start_val) {
  int sz = get_num_of_slots();
  int entry_sz = INTERNAL_SLOT_ENTRY_SIZE_;
  // move the slot array by 1 slot.
  if(entry_sz + key.size_ > get_free_space_size()) return; // no space.
  memmove(get_ptr_to(SLOT_ARRAY_OFFSET_) + entry_sz, get_ptr_to(SLOT_ARRAY_OFFSET_), sz*entry_sz);
  increase_size(1);
  insert_cell_at(1, key);
  if(key.data_)
    insert_cell_at(0, null_index_key(arena, *(key.data_)));
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

void BTreeInternalPage::remove_entry_at(int pos) {
  int sz = get_num_of_slots();
  if(sz <= 1) return;
  if(pos >= sz || pos <= 0) assert(0);
  int entry_sz = INTERNAL_SLOT_ENTRY_SIZE_;

  char* dest = get_ptr_to(SLOT_ARRAY_OFFSET_)+(pos*entry_sz);
  char* src  = dest+entry_sz;
  int amount = (sz-(pos + 1)) * entry_sz;
  assert(amount >= 0);
  memmove(dest, src, amount);
  increase_size(-1);
}


bool BTreeInternalPage::Insert(IndexKey k, PageID v){
    int size = get_num_of_slots();
    int entry_sz = INTERNAL_SLOT_ENTRY_SIZE_;
    if(entry_sz + k.size_ > get_free_space_size()) return false; // no space.


    int mid;
    int low = 1;
    int high = size;
    while (low < high) {
        mid = low + (high - low) / 2;

        if (!(k > KeyAt(mid))) {
            high = mid;
        } else {
            low = mid + 1;
        }
    }
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
    insert_cell_at(cur, k);
    SetValAt(cur, v);
    return true;
}


