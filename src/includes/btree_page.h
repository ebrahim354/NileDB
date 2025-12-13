#ifndef BTREE_PAGE_H
#define BTREE_PAGE_H

#include <set>
#include "page.h"
#include "value.h"
#include "index_key.h"


#define BTREE_HEADER_SIZE 25

enum class BTreePageType: u8 { 
  INVALID_PAGE  = '0',
  LEAF_PAGE     = '1',
  INTERNAL_PAGE = '2'
};

class BTreePage {
 public:
  bool IsLeafPage() const;
  bool IsRootPage() const;
  void SetPageType(BTreePageType page_type);

  static int get_max_key_size();

  uint32_t get_num_of_slots() const;

  /*
  bool IsFull(IndexKey k);*/
  void set_num_of_slots(uint32_t n);
  uint32_t get_used_space();
  bool can_merge_with_me(BTreePage* other);

  bool TooShort();
  bool TooShortBefore();
  /*
  uint32_t GetMinSize() const;
  void SetMaxSize(int max_size);
  */
  void increase_size(int amount);

  PageID GetParentPageId(FileID parent_fid) const;
  void SetParentPageId(PageID parent_page_id);

  PageID GetPageId(FileID fid) const;
  void SetPageId(PageID page_id);

  uint32_t get_free_space_size() const;

  char* get_key_ptr(int idx);
  uint16_t get_key_size(int idx);
  IndexKey KeyAtCpy(Arena* arena, int index);
  IndexKey KeyAt(int index);

  void SetKeyAt(int index, IndexKey k);
  uint16_t compact();

  char* get_val_ptr(int idx);

 //private:

  inline char*    get_ptr_to(size_t offset) const;

  inline BTreePageType get_page_type() const;

  inline PageNum get_page_number() const;

  inline  PageNum get_parent_page_number() const;

  inline void set_page_type(BTreePageType t);

  inline void set_page_number(PageNum page_num);

  inline void set_parent_page_number(PageNum page_num);

  void set_free_space_offset(uint32_t free_space_ptr);

  uint32_t get_free_space_offset() const;

  char* get_free_space_ptr() const;


  static const size_t PAGE_TYPE_OFFSET_ = 0  ;          //  1 byte .
  static const size_t PAGE_NUMBER_OFFSET_ = 1;          //  4 bytes.
  static const size_t PARENT_PAGE_NUMBER_OFFSET_ = 5;   //  4 bytes.
  static const size_t NEXT_PAGE_NUMBER_OFFSET_ = 9;     //  4 bytes (only used for leaf pages).
  static const size_t FREE_SPACE_PTR_OFFSET_ = 13;      //  4 bytes.
  static const size_t NUMBER_OF_SLOTS_OFFSET_ = 17;     //  4 bytes.
  static const size_t SLOT_ARRAY_OFFSET_ = 21;          //  4 bytes.
  static const size_t SLOT_ARRAY_KEY_SIZE_ = 4;         //  2 bytes(offset) + 2 bytes(size).
  static const size_t INTERNAL_SLOT_ENTRY_SIZE_ = SLOT_ARRAY_KEY_SIZE_  + 4;//  4 bytes key + 4  bytes page number.
  static const size_t LEAF_SLOT_ENTRY_SIZE_ = SLOT_ARRAY_KEY_SIZE_;         //  4 bytes key.
  static const size_t HEADER_SIZE_ = BTREE_HEADER_SIZE; // 25 bytes are used for storing header data.
};

#endif //BTREE_PAGE_H
