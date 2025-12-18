#pragma once
#include <set>
#include "page.cpp"
#include "value.cpp"
#include "index_key.h"
#include "btree_page.h"


int BTreePage::get_max_key_size() {
    auto mx = std::max((u32)LEAF_SLOT_ENTRY_SIZE_, (u32)INTERNAL_SLOT_ENTRY_SIZE_);
    return PAGE_SIZE-(BTREE_HEADER_SIZE+mx);
}

uint32_t BTreePage::get_num_of_slots() const {
    return *reinterpret_cast<uint32_t*>(get_ptr_to(NUMBER_OF_SLOTS_OFFSET_));
}

/*
   bool BTreePage::IsFull(IndexKey k) { 
   auto entry_sz = (get_page_type() == 
   BTreePageType::INTERNAL_PAGE) ? INTERNAL_SLOT_ENTRY_SIZE_ : LEAF_SLOT_ENTRY_SIZE_;
   return ((entry_sz + k.size_) >= get_free_space_size());
   }*/

uint32_t BTreePage::get_used_space() {
    return PAGE_SIZE - get_free_space_size(); 
}

bool BTreePage::can_merge_with_me(BTreePage* other) {
    return (get_free_space_size() > other->get_used_space());
}

bool BTreePage::TooShort() {
    // root page is special.
    if (IsRootPage()) {
        return get_num_of_slots() <= 1;
    }
    return get_free_space_size() > get_used_space(); 
}

bool BTreePage::TooShortBefore() {
    // root page is special.
    if (IsRootPage()) {
        return get_num_of_slots() <= 1;
    }
    return get_free_space_size() > (get_used_space()); 
}


uint32_t BTreePage::get_free_space_size() const {
    auto type = get_page_type();
    if(type !=  BTreePageType::LEAF_PAGE && type != BTreePageType::INTERNAL_PAGE) return 0;
    uint32_t end_of_slots_offset = SLOT_ARRAY_OFFSET_ + (get_num_of_slots() * 
            (type == BTreePageType::INTERNAL_PAGE ? INTERNAL_SLOT_ENTRY_SIZE_ : LEAF_SLOT_ENTRY_SIZE_));
    char* end_of_slots_ptr = get_ptr_to(end_of_slots_offset);
    char* free_space_ptr = get_free_space_ptr();
    return free_space_ptr - end_of_slots_ptr;
}

char* BTreePage::get_key_ptr(int idx) {
    auto size = get_num_of_slots(); 
    auto type = get_page_type();
    if(idx >= size) return nullptr;
    if(idx == 0 && type != BTreePageType::LEAF_PAGE) return nullptr;
    // slot array -> [item ptr] -> [real item].
    return get_ptr_to(*(uint16_t*)get_ptr_to(SLOT_ARRAY_OFFSET_ + 
                (idx * (type == BTreePageType::INTERNAL_PAGE ? INTERNAL_SLOT_ENTRY_SIZE_ : LEAF_SLOT_ENTRY_SIZE_))));
}

uint16_t BTreePage::get_key_size(int idx) {
    auto size = get_num_of_slots(); if(idx >= size) return 0;
    auto type = get_page_type();
    // slot array -> [item ptr] -> [real item].
    return *(uint16_t*)get_ptr_to(SLOT_ARRAY_OFFSET_ + (SLOT_ARRAY_KEY_SIZE_/2) +
            (idx * (type == BTreePageType::INTERNAL_PAGE ? INTERNAL_SLOT_ENTRY_SIZE_ : LEAF_SLOT_ENTRY_SIZE_)));
}

IndexKey BTreePage::KeyAtCpy(Arena* arena, int index) {
    char* ptr = get_key_ptr(index);
    unsigned int sz = get_key_size(index);
    if(!ptr) return IndexKey();
    char* k = (char*) arena->alloc(sz);
    memcpy(k, ptr, sz);
    return {
        .data_ = k,
        .size_ = sz, 
    };
}


IndexKey BTreePage::KeyAt(int index) {
    char* ptr = get_key_ptr(index);
    if(!ptr) return IndexKey();
    return {
        .data_ = ptr,
        .size_ = get_key_size(index),
    };
}

void BTreePage::SetKeyAt(int index, IndexKey k) { 
    char* key = k.data_;
    uint16_t size = k.size_;

    assert(index < get_num_of_slots() && size <= get_free_space_size());
    if(!key || size == 0 || index > get_num_of_slots() || size > get_free_space_size()) {
        return;
    }

    uint16_t new_free_space_offset = get_free_space_offset() - size;
    char* key_ptr = get_free_space_ptr() - size;
    auto entry_sz = get_page_type() == BTreePageType::INTERNAL_PAGE ? INTERNAL_SLOT_ENTRY_SIZE_ : LEAF_SLOT_ENTRY_SIZE_;
    memcpy(key_ptr, key, size);
    set_free_space_offset(new_free_space_offset);


    // update the slot array.
    // key offset.
    memcpy(
            get_ptr_to(SLOT_ARRAY_OFFSET_ + (index*entry_sz)),
            &new_free_space_offset,
            sizeof(uint16_t));
    // key size.
    memcpy(
            get_ptr_to(SLOT_ARRAY_OFFSET_ + (index*entry_sz) + (SLOT_ARRAY_KEY_SIZE_/2)),
            &size,
            sizeof(uint16_t));
    // array_[index].first = key; 
}

uint16_t BTreePage::compact(){
    uint32_t sz = get_num_of_slots();
    // sroted slots by the closest key payload to the end of the page(highest key paylod offset).
    //               offset,index
    std::set<std::pair<uint16_t, uint32_t>, std::greater<std::pair<uint16_t, uint32_t>>> sorted_slots;
    auto type = get_page_type();
    uint8_t entry_size = (type == BTreePageType::INTERNAL_PAGE ? INTERNAL_SLOT_ENTRY_SIZE_ : LEAF_SLOT_ENTRY_SIZE_);
    uint32_t idx = (type == BTreePageType::INTERNAL_PAGE ? 1 : 0);
    for(; idx < sz; ++idx){
        uint16_t offset = *(uint16_t*)get_ptr_to(SLOT_ARRAY_OFFSET_ + (idx * entry_size));
        sorted_slots.insert({offset, idx});
    }


    uint16_t new_fso = PAGE_SIZE-1;
    uint16_t fso = get_free_space_offset();

    for(auto it : sorted_slots) {
        uint16_t offset = it.first;
        uint32_t idx = it.second;
        char* kp = get_key_ptr(idx);
        uint16_t ks = get_key_size(idx);
        int32_t hole_size = (new_fso - offset)-ks;
        assert(hole_size >= 0 && "HOLE SIZE CANT BE NEGATIVE!\n");
        if(hole_size > 0) {
            memmove(kp+hole_size, kp, ks);
            *(uint16_t*)get_ptr_to(SLOT_ARRAY_OFFSET_ + (idx * entry_size)) = offset + (uint16_t)hole_size;
        }
        new_fso -= ks;
    }
    assert(new_fso >= fso && "ERROR WHILE COMPACTING SPACE!");
    memset(get_ptr_to(fso), 0, new_fso-fso); // TODO: Disable on release.
    set_free_space_offset(new_fso);
    return new_fso-fso;
}


char* BTreePage::get_val_ptr(int idx) {
    auto size = get_num_of_slots(); if(idx >= size) return nullptr;
    auto type = get_page_type();
    return get_ptr_to(SLOT_ARRAY_KEY_SIZE_ + SLOT_ARRAY_OFFSET_ + 
            (idx * (type == BTreePageType::INTERNAL_PAGE ? INTERNAL_SLOT_ENTRY_SIZE_ : LEAF_SLOT_ENTRY_SIZE_)));
}


//private:

inline char*    BTreePage::get_ptr_to(size_t offset) const {
    return (char*)this+offset;
}

inline BTreePageType BTreePage::get_page_type() const {
    return (BTreePageType)*get_ptr_to(PAGE_TYPE_OFFSET_);
}

inline PageNum BTreePage::get_page_number() const {
    return *(PageNum*)get_ptr_to(PAGE_NUMBER_OFFSET_);
}

inline  PageNum BTreePage::get_parent_page_number() const {
    return *(PageNum*)get_ptr_to(PARENT_PAGE_NUMBER_OFFSET_);
}

inline void BTreePage::set_page_type(BTreePageType t) {
    char* ptr = get_ptr_to(PAGE_TYPE_OFFSET_);
    *ptr = (char) t;
}

inline void BTreePage::set_page_number(PageNum page_num) {
    PageNum* ptr = (PageNum*)get_ptr_to(PAGE_NUMBER_OFFSET_);
    *ptr = page_num;
}

inline void BTreePage::set_parent_page_number(PageNum page_num) {
    PageNum* ptr = (PageNum*)get_ptr_to(PARENT_PAGE_NUMBER_OFFSET_);
    *ptr = page_num;
}


void BTreePage::set_free_space_offset(uint32_t free_space_ptr){
    memcpy(get_ptr_to(FREE_SPACE_PTR_OFFSET_), &free_space_ptr, sizeof(free_space_ptr)); 
}

uint32_t BTreePage::get_free_space_offset() const {
    return *reinterpret_cast<uint32_t*>(get_ptr_to(FREE_SPACE_PTR_OFFSET_));
}

char* BTreePage::get_free_space_ptr() const {
    return get_ptr_to(*reinterpret_cast<uint32_t*>(get_ptr_to(FREE_SPACE_PTR_OFFSET_)));
}

bool BTreePage::IsLeafPage() const { return get_page_type() == BTreePageType::LEAF_PAGE; }
bool BTreePage::IsRootPage() const { return get_parent_page_number() == INVALID_PAGE_NUM; }
void BTreePage::SetPageType(BTreePageType page_type) { set_page_type(page_type); }


void BTreePage::set_num_of_slots(uint32_t n) { 
  bool  shrinking = get_num_of_slots() > n;
  *(uint32_t*)(get_ptr_to(NUMBER_OF_SLOTS_OFFSET_)) = n;

  if(shrinking) compact();
}
/*
uint32_t BTreePage::GetMinSize() const { return GetMaxSize() / 2;      }


void BTreePage::SetMaxSize(int size) { max_size_ = size; }

*/
void BTreePage::increase_size(int amount) { 
  uint32_t* size_ptr = (uint32_t*)get_ptr_to(NUMBER_OF_SLOTS_OFFSET_);
  if(*size_ptr == 0 && amount < 1) return;
  *size_ptr += amount;

  if(amount < 0) compact();
}

PageID BTreePage::GetParentPageId(FileID parent_fid) const { 
    PageNum pg_num = get_parent_page_number();
    if(pg_num == INVALID_PAGE_NUM) return INVALID_PAGE_ID;
    return {
        .fid_ = parent_fid,
            .page_num_ = pg_num,
    };
}

void BTreePage::SetParentPageId(PageID parent_page_id) {
  set_parent_page_number(parent_page_id.page_num_);
}

PageID BTreePage::GetPageId(FileID fid) const {
    PageNum pg_num = get_page_number();
    if(pg_num == INVALID_PAGE_NUM) return INVALID_PAGE_ID;
    return {
        .fid_ = fid,
        .page_num_ = pg_num,
    };
}

void BTreePage::SetPageId(PageID page_id) { 
  set_page_number(page_id.page_num_);
}

