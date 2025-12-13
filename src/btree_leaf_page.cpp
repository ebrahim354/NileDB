#pragma once
#include <vector>
#include <math.h>

#include "btree_page.cpp"
#include "record.cpp"
#include "btree_leaf_page.h"

bool BTreeLeafPage::split_with_and_insert(Arena* arena, i32 nvals, bool unique_insertion,
        BTreeLeafPage* new_page, IndexKey k) {
    int sz = get_num_of_slots();
    int md = std::ceil(static_cast<float>(sz) / 2);
    md--;
    for (int i = md + 1, j = 0; i < sz; i++, j++) {
        new_page->increase_size(1);
        new_page->SetKeyAt(j, KeyAt(i));
        //new_page->SetValAt(j, ValAt(i));
    }
    sz = md+1;
    set_num_of_slots(sz);
    assert(sz > 0 && "Key couldn't fit in an empty page");
    auto last_key = KeyAt(md);
    if(k <= last_key && !IsFull(k)) 
        //return Insert(k, v);
        return Insert(arena, k, nvals, unique_insertion);
    else if(!new_page->IsFull(k))
        return new_page->Insert(arena, k, nvals, unique_insertion);
        //return new_page->Insert(k, v);
    return false;
}

inline IndexKey BTreeLeafPage::get_last_key_cpy(Arena* arena, int elements_to_chop) {
    auto k = KeyAt(get_num_of_slots() - 1);
    char* data = (char*)arena->alloc(k.size_);
    memcpy(data, k.data_, k.size_);
    IndexKey tmp = {
        .data_ = data,
        .size_ = k.size_,
    };
    if(elements_to_chop > 0)
        remove_last_n(arena, &tmp, elements_to_chop);

    return tmp;
}
bool BTreeLeafPage::IsFull(IndexKey k) { 
    return ((LEAF_SLOT_ENTRY_SIZE_ + k.size_) >= get_free_space_size());
}

void BTreeLeafPage::Init(PageID page_id, PageID parent_id) {
  SetPageType(BTreePageType::LEAF_PAGE);
  SetPageId(page_id);
  SetParentPageId(parent_id);
  set_next_page_number(INVALID_PAGE_NUM);
  set_free_space_offset(PAGE_SIZE - 1);
}


PageID BTreeLeafPage::GetNextPageId(FileID fid) { 
    PageNum pg = get_next_page_number();
    if(pg == INVALID_PAGE_NUM) return INVALID_PAGE_ID;
  return {
    .fid_ = fid,
    .page_num_ = pg,
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


/*
RecordID BTreeLeafPage::ValAt(int index){
  return *(RecordID*)get_val_ptr(index);
}

void BTreeLeafPage::SetValAt(int index, RecordID v) {
  char* ptr = get_val_ptr(index);
  if(!ptr) return;
  *(RecordID*)ptr = v;
}


bool BTreeLeafPage::GetValue(IndexKey k, Vector<RecordID> *result) {
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
    *result = Vector<RecordID>();
    result->push_back(ValAt(cur));
    return true;
  }
  return false;
} */

int BTreeLeafPage::GetPos(IndexKey k) {
    int size = get_num_of_slots();
    int mid;
    int low = 0;
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

bool BTreeLeafPage::Insert(Arena* arena, IndexKey input_k, i32 nvals, bool is_unique) {
    int size = get_num_of_slots();
    int entry_sz = LEAF_SLOT_ENTRY_SIZE_;
    IndexKey k = input_k;

    if(entry_sz + k.size_ > get_free_space_size()) assert(0); // no space.

    ArenaTemp tmp_arena = arena->start_temp_arena();
    if(is_unique){
        assert(nvals > 0);
        k = remove_last_n_cpy(arena, input_k, nvals);
    }


    int mid;
    int low = 0;
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

    if (cur < size && KeyAt(cur) == k) {
        arena->clear_temp_arena(tmp_arena);
        return false;
    }
    if(cur < size){
        memmove(get_ptr_to(SLOT_ARRAY_OFFSET_) + 
                (entry_sz * cur) + entry_sz, 
                get_ptr_to(SLOT_ARRAY_OFFSET_) +
                (entry_sz * cur), 
                (size-cur)*entry_sz);
    }
    increase_size(1);
    SetKeyAt(cur, input_k);
    arena->clear_temp_arena(tmp_arena);
    return true;
}

bool BTreeLeafPage::Remove(IndexKey k) {
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

    if (!(KeyAt(cur) == k) || cur >= size) {
        return false;
    }
    // offset the slot array.
    memmove(get_ptr_to(SLOT_ARRAY_OFFSET_) + 
            (entry_sz * cur), 
            get_ptr_to(SLOT_ARRAY_OFFSET_) +
            (entry_sz * cur) + entry_sz, 
            (size-(cur+1))*entry_sz);
    increase_size(-1);
    return true;
}

void BTreeLeafPage::Draw() {
    // TODO: provide drawing support for varius type.
    std::cout << "---leaf_page---";
}


IndexKey BTreeLeafPage::getPointer(int pos) {
    if(pos >= get_num_of_slots()) return {};
    return KeyAt(pos);
}
