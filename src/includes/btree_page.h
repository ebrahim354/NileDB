#ifndef BTREE_PAGE_H
#define BTREE_PAGE_H

#include <set>
#include "page.h"
#include "value.h"
#include "index_key.h"


#define BTREE_HEADER_SIZE 21
// NOTE: these constats are used by sqlite to decide if
// the key should be stored in an overflow page or not,
// and they are picked in way that gives a good fanout to the overall btree page,
// and to keep the tree more balanced while keeping enough data of the key local.
// the '23' at the end is a fudge factor to make sure no out of bounds errors happen.
// https://www.sqlite.org/fileformat2.html#cellformat
//
#define BTREE_U_CONST (PAGE_SIZE - BTREE_HEADER_SIZE)
#define BTREE_X_CONST (((BTREE_U_CONST-BTREE_HEADER_SIZE)*64/255)-23)
#define BTREE_M_CONST (((BTREE_U_CONST-BTREE_HEADER_SIZE)*32/255)-23)
// TODO: provide compile time assertions for positive values.

u64 normalize_index_key_size(const IndexKey& key) {
    if(key.size_ <= BTREE_X_CONST) return key.size_;
	u64 k =  BTREE_M_CONST+((key.size_-BTREE_M_CONST)%(BTREE_U_CONST-4));
    if(k <= BTREE_X_CONST) return k;
    return BTREE_M_CONST; 
}

enum class BTreePageType: u8 { 
  INVALID_PAGE  = '0',
  LEAF_PAGE     = '1',
  INTERNAL_PAGE = '2'
};

class BTreePage {
 public:
  bool IsLeafPage() const;
  void SetPageType(BTreePageType page_type);

  static int get_max_key_size();

  uint32_t get_num_of_slots() const;

  /*
  bool IsFull(IndexKey k);*/
  void set_num_of_slots(uint32_t n);
  uint32_t get_used_space();
  bool can_merge_with_me(BTreePage* other);

  bool TooShort(bool is_root_page);
  bool TooShortBefore(bool is_root_page);
  /*
  uint32_t GetMinSize() const;
  void SetMaxSize(int max_size);
  */
  void increase_size(int amount);

  PageID GetPageId(FileID fid) const;
  void SetPageId(PageID page_id);

  uint32_t get_free_space_size() const;

  char* get_key_ptr(int idx);
  uint16_t get_key_size(int idx);
  IndexKey KeyAtCpy(Arena* arena, int index);
  IndexKey KeyAt(int index);

  void insert_cell_at(int index, IndexKey k);
  uint16_t compact();

  char* get_val_ptr(int idx);

 //private:

  inline char*    get_ptr_to(size_t offset) const;

  inline BTreePageType get_page_type() const;

  inline PageNum get_page_number() const;


  inline void set_page_type(BTreePageType t);

  inline void set_page_number(PageNum page_num);

  void set_free_space_offset(uint32_t free_space_ptr);

  uint32_t get_free_space_offset() const;

  char* get_free_space_ptr() const;


  // TODO: Remove unnecessary data such as parent page number and page number,
  // also shrink the sized of other meta data entries to only 2 bytes / entry.
  static const size_t PAGE_TYPE_OFFSET_ = 0  ;          //  1 byte .
  static const size_t PAGE_NUMBER_OFFSET_ = 1;          //  4 bytes.
  static const size_t NEXT_PAGE_NUMBER_OFFSET_ = 5;     //  4 bytes (only used for leaf pages).
  static const size_t FREE_SPACE_PTR_OFFSET_ = 9;      //  4 bytes.
  static const size_t NUMBER_OF_SLOTS_OFFSET_ = 13;     //  4 bytes.
  static const size_t SLOT_ARRAY_OFFSET_ = 17;          //  4 bytes.
  static const size_t SLOT_ARRAY_KEY_SIZE_ = 4;         //  2 bytes(offset) + 2 bytes(size).
  static const size_t INTERNAL_SLOT_ENTRY_SIZE_ = SLOT_ARRAY_KEY_SIZE_  + 4;//  4 bytes key + 4  bytes page number.
  static const size_t LEAF_SLOT_ENTRY_SIZE_ = SLOT_ARRAY_KEY_SIZE_;         //  4 bytes key.
  static const size_t HEADER_SIZE_ = BTREE_HEADER_SIZE; // 25 bytes are used for storing header data.
};

#endif //BTREE_PAGE_H
