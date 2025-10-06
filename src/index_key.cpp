#pragma once

#include <cassert>
#include <cstdint>
#include <cstring>
struct IndexKey;
int index_key_cmp(IndexKey lhs,IndexKey rhs);

/*
* this structure is used to group multiple columns into one key to support multi column indexes for example: 
* create index tmp_index on tmp_table(a,b,c);
* this structure does not own the data it's just a 'view' over the actual key that is stored inside of the index page.
* key/record format used by sqlite : https://www.sqlite.org/fileformat.html#record_format.
* this format is different from the record format that is used for tables pages
* because it stores the schema from within the record rather than using an external schema to decypher the data.
* this format is perfect for a btree because the structure will do direct byte-level comparisons without the need 
* to do any high level conversions using an external schema just to compare two keys,
* however this comes at the cost of having to store a small header for each and every key.
* you can make the argument that either formats are ok to use both in an index or a table, But I don't care too much
* the goal is to try multiple ways and understand the different trade offs.
*/
// TODO: we only support header size of 255 (1 byte) and text fields cannot exceed (255-13) (1 byte).
// until we support the 'varint' type used by sqlite.
// NOTE: we use simpler serial types than the one sqlite uses.
enum class SerialType {
  NIL  = 0, // 0 bytes.
  INT  = 1, // 4 bytes.
  LONG = 2, // 8 bytes.
  FLOAT= 3, // 8 bytes.
  // 13 to avoid confusion with sqlite.
  TEXT = 13, // (N-13) bytes.
};

struct IndexKey {
  // size of the data is stored in the first byte.
  char* data_ = nullptr;

  inline char get_header_size(){
    return (char)*data_;
  }

  inline char* get_payload_ptr(){
    if(!data_) return nullptr;
    return data_+get_header_size();
  }

  inline char* get_header_ptr(){
    return data_+1;
  }

  IndexKey& operator=(const IndexKey& rhs){
    this->data_ = rhs.data_;
    return *this;
  } 
  bool operator==(const IndexKey &rhs) const {
    int cmp = index_key_cmp(*this, rhs);
    return cmp == 0;
  }

  bool operator!=(const IndexKey &rhs) const {
    int cmp = index_key_cmp(*this, rhs);
    return cmp != 0;
  }

  bool operator<(const IndexKey &rhs) const {
    int cmp = index_key_cmp(*this, rhs);
    return cmp < 0;
  }

  bool operator<=(const IndexKey &rhs) const {
    int cmp = index_key_cmp(*this, rhs);
    return cmp <= 0;
  }

  bool operator>(const IndexKey &rhs) const {
    int cmp = index_key_cmp(*this, rhs);
    return cmp > 0;
  }

  bool operator>=(const IndexKey &rhs) const {
    int cmp = index_key_cmp(*this, rhs);
    return cmp >= 0;
  }
};


// -1 ==> lhs < rhs, 0 eq, 1 ==> lhs > rhs
int index_key_cmp(IndexKey lhs,IndexKey rhs) {
  if(!lhs.data_ || !rhs.data_ || lhs.get_header_size() != rhs.get_header_size()) assert(1 && "INVALID COMPARISON");
  char* payload_ptr = lhs.get_payload_ptr();
  char* rhs_payload_ptr = rhs.get_payload_ptr();
  char* header = lhs.get_header_ptr();
  char* rhs_header = rhs.get_header_ptr();
  while(header != lhs.get_payload_ptr()){
    if((*header >= 13 && *rhs_header < 13) || (*header < 13 && *rhs_header >= 13)) assert(1 && "INVALID COMPARISON");
    if(*header < 13 && *header != *rhs_header) assert(1&& "TODO: SUPPORT NUMERIC CASTING.");
    int diff = 0;
    int advance = 0;
    switch(*header){
      case (uint8_t)SerialType::INT:{
        diff = (int32_t) *payload_ptr - (int32_t) *rhs_payload_ptr;
        advance = 4;
        break;
               }
      case (uint8_t)SerialType::LONG:{
        diff = (int64_t) *payload_ptr - (int64_t) *rhs_payload_ptr;
        advance = 8;
        break;
                }
      case (uint8_t)SerialType::FLOAT:{
        diff = (double) *payload_ptr  - (double) *rhs_payload_ptr;
        advance = 8;
        break;
                 }
      case (uint8_t)SerialType::NIL:{
        break;
               }
      case (uint8_t)SerialType::TEXT:
      default:{
        if(*header < 13) return false;
        diff = memcmp(payload_ptr, rhs_payload_ptr, (*header) - 13);
        advance = (*header) - 13;
        break;
              }
    }
    if(diff) return diff;
    payload_ptr += advance;
    rhs_payload_ptr += advance;
    header++;
    rhs_header++;
  }
  return 0;
}
