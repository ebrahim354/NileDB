#ifndef INDEX_KEY_H
#define INDEX_KEY_H


#include <cassert>
#include <cstdint>
#include <cstring>
#include "tuple.h"

struct IndexKey;
class BTreeIndex;
bool is_desc_order(char* bitmap, int idx);
int index_key_cmp(IndexKey lhs,IndexKey rhs);
IndexKey temp_index_key_from_values(Vector<Value>& vals);
#define EPS 1e-6

struct IndexField {
    String name_;
    bool desc_ = false;
};

struct NumberedIndexField {
    int  idx_ = -1;
    bool desc_ = false;

};

struct IndexHeader {
    IndexHeader(){};
    IndexHeader(BTreeIndex* index, String index_name, Vector<NumberedIndexField> fields_numbers = {}):
        index_(index), index_name_(index_name), fields_numbers_(fields_numbers)
    {};
    IndexHeader(Arena* arena):
        index_name_(arena), fields_numbers_(arena)
    {}
    BTreeIndex* index_;
    String index_name_;
    Vector<NumberedIndexField> fields_numbers_;
};



/*
 * this structure is used to group multiple columns into one key to support multi column indexes for example: 
 * create index tmp_index on tmp_table(a,b,c);
 * this structure does not own the data it's just a 'view' over the actual key that is stored inside of the index page.
 * key/record format used by sqlite : https://www.sqlite.org/fileformat.html#record_format.
 * this format is different from the record format that is used for tables pages
 * because it stores the schema from within the record rather than using an external schema to decypher the data.
 * this format is perfect for a btree because the structure will do direct byte-level comparisons without the need 
 * to do any high level conversions using an external schema just to compare two keys,
 * however this comes at the cost of having to store a small header for every key.
 * you can make the argument that either formats are ok to use both in an index or a table, But I don't care too much
 * the goal is to try multiple ways and understand the different trade offs.
 */
// TODO: we only support header size of 255 (1 byte) and text fields cannot exceed (255-13) (1 byte).
// until we support the 'varint' type used by sqlite.
// NOTE: we use simpler serial types than the one sqlite uses.
enum class SerialType : u8 {
    NIL  = 0, // 0 bytes.
    INT  = 1, // 4 bytes.
    LONG = 2, // 8 bytes.
    FLOAT= 3, // 4 bytes.
    // 13 to avoid confusion with sqlite.
    TEXT = 13, // (N-13) bytes.
};


struct IndexKey {
    // size of the data is stored in the first byte.
    char* data_ = nullptr;
    char* sort_order_ = nullptr;
    uint32_t size_ = 0;

    // the user should be sure before using this method that the last 2 entries are in fact a record id.
    RecordID getRID(FileID fid) {
        assert(size_ && data_ && get_header_size() > 2);
        char* header = (get_header_ptr()+get_header_size())-2; // record id is stored at the last two entries.
        char* payload = (data_+size_)-8; // record id is stored at the last 8 bytes.
        assert(    *(SerialType*)header == SerialType::INT 
                && *(SerialType*)(header+1) == SerialType::INT );
        RecordID rid;
        rid.page_id_.fid_ = fid;
        rid.page_id_.page_num_ = (i32)(*(i32*)payload);
        payload += 4;
        rid.slot_number_ = (i32)(*(i32*)payload);
        return rid;
    }

    Value get_val_at(u32 idx) {
        assert(size_ && data_ && idx < get_header_size());
        char* header = get_header_ptr();
        char* payload = get_payload_ptr();
        for(int i = 0; i < idx; ++i, header++){
            switch(*(SerialType*)header){
                case SerialType::INT:
                case SerialType::FLOAT:
                    payload += 4;
                    break;
                case SerialType::LONG:
                    payload += 8;
                case SerialType::NIL:
                    payload += 0;
                default:
                    payload += (*(u8*)header - 13);
            }
        }
        if(*header == 0) return Value(NULL_TYPE);
        if(*header == 1) return Value((i32)(*(i32*)payload));
        if(*header == 2) return Value((i64)(*(i64*)payload));
        if(*header == 3) return Value((f32)(*(f32*)payload));
        return Value(payload, (*(u8*)header - 13));
    }

    void print() {
        if(!data_){
            std::cout << '-';
            return;
        }
        u8 sz = get_header_size();
        std::cout << "IndexKey with " << (int)sz << " elements, Total size: " << size_ << ", elements: ";
        for(int i = 0; i < sz; ++i){
            std::cout << get_val_at(i).toString();
            if(i + 1 < sz)
                std::cout << ", "; 
        }
        std::cout << "\n";
    }

    inline uint8_t get_header_size(){
        if(!data_) return 0;
        return *(uint8_t*)data_;
    }

    inline char* get_payload_ptr(){
        if(!data_) return nullptr;
        return get_header_ptr()+get_header_size();
    }

    inline char* get_header_ptr(){
        return data_+1;
    }

    IndexKey& operator=(const IndexKey& rhs){
        this->data_ = rhs.data_;
        this->size_ = rhs.size_;
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

// removes the last n elements from a key (in-place).
// arena is just used for temporary memory and cleared back.
bool remove_last_n(Arena* arena, IndexKey* k, i32 n){
    int k_header_sz = k->get_header_size();
    assert(n > 0 && n < k_header_sz);
    if(n == 0) false;
    int original_ksize = k->size_;
    char* data = (char*)arena->alloc(original_ksize);
    *data = (*(k->data_))-n;
    auto header = k->get_header_ptr();
    auto payload = k->get_payload_ptr();
    memcpy(data+1, header, k_header_sz - n);
    i32 removed_payload_sz = 0;
    int i = 0;
    for(char* header_ptr = payload - 1; i < n; ++i, header_ptr--){
        switch(*(SerialType*)header_ptr){
            case SerialType::INT:
            case SerialType::FLOAT:
                removed_payload_sz += 4;
                break;
            case SerialType::LONG:
                removed_payload_sz += 8;
            case SerialType::NIL:
                removed_payload_sz += 0;
            default:
                removed_payload_sz += (*(u8*)header_ptr - 13);
        }
    }
    memcpy(data+1+(k_header_sz-n), payload, ((payload+original_ksize)-removed_payload_sz)-payload);
    k->size_ = original_ksize-(n  + removed_payload_sz);
    memcpy(k->data_, data, k->size_);
    arena->dealloc(original_ksize);
    return true;
}

// removes the last n elements from a key and returns a copy.
IndexKey remove_last_n_cpy(Arena* arena, IndexKey k, i32 n){
    int k_header_sz = k.get_header_size();
    assert(n > 0 && n < k_header_sz);
    int original_ksize = k.size_;
    char* data = (char*)arena->alloc(original_ksize);
    *data = (*(k.data_))-n;
    auto header = k.get_header_ptr();
    auto payload = k.get_payload_ptr();
    memcpy(data+1, header, k_header_sz - n);
    i32 removed_payload_sz = 0;
    int i = 0;
    for(char* header_ptr = payload - 1; i < n; ++i, header_ptr--){
        switch(*(SerialType*)header_ptr){
            case SerialType::INT:
            case SerialType::FLOAT:
                removed_payload_sz += 4;
                break;
            case SerialType::LONG:
                removed_payload_sz += 8;
            case SerialType::NIL:
                removed_payload_sz += 0;
            default:
                removed_payload_sz += (*(u8*)header_ptr - 13);
        }
    }
    memcpy(data+1+(k_header_sz-n), payload, ((payload+original_ksize)-removed_payload_sz)-payload);
    u32 new_size = original_ksize-(n  + removed_payload_sz);
    return {
        .data_ = data,
        .size_ = new_size,
    };
}


// -1 ==> lhs < rhs, 0 eq, 1 ==> lhs > rhs
int index_key_cmp(IndexKey lhs,IndexKey rhs) {
    assert(lhs.sort_order_ != nullptr || rhs.sort_order_ != nullptr); // at least one key should know the order
    if(lhs.data_  && !rhs.data_ ) return 1;
    if(rhs.data_  && !lhs.data_ ) return -1;
    if(!rhs.data_ && !lhs.data_ ) return 0;

    if(lhs.get_header_size() != rhs.get_header_size()){
        //assert(0 && "INVALID COMPARISON");
    }
    char* payload_ptr = lhs.get_payload_ptr();
    char* rhs_payload_ptr = rhs.get_payload_ptr();
    char* header = lhs.get_header_ptr();
    char* rhs_header = rhs.get_header_ptr();
    while(header != lhs.get_payload_ptr() && rhs_header != rhs.get_payload_ptr()){
        if(*header != 0 && *rhs_header == 0) // rhs is null.
            return 1;
        if(*header == 0 && *rhs_header != 0) // lhs is null.
            return -1;
        if(*header == 0 && *rhs_header == 0) {// both are null.
            header++;
            rhs_header++;
            continue; 
        }

        if((*header >= 13 && *rhs_header < 13) || (*header < 13 && *rhs_header >= 13)) {
            assert(0 && "INVALID COMPARISON");
        }
        if(*header < 13 && *header != *rhs_header) {
            assert(0 && "TODO: SUPPORT NUMERIC CASTING.");
        }
        int diff = 0;
        int advance = 0;
        switch(*header){
            case (uint8_t)SerialType::INT:{
                                              diff = *(int32_t*) payload_ptr - *(int32_t*) rhs_payload_ptr;
                                              advance = 4;
                                              break;
                                          }
            case (uint8_t)SerialType::LONG:{
                                               diff = *(int64_t*) payload_ptr - *(int64_t*) rhs_payload_ptr;
                                               advance = 8;
                                               break;
                                           }
            case (uint8_t)SerialType::FLOAT:{
                                                float res = *(float*) payload_ptr  - *(float*) rhs_payload_ptr;
                                                if(fabsf(res) > EPS){
                                                    if(res < 0.0) diff = -1;
                                                    else diff = 1;
                                                }
                                                advance = 4;
                                                break;
                                            }
            case (uint8_t)SerialType::NIL:{
                                              break;
                                          }
            case (uint8_t)SerialType::TEXT:
            default:{
                        if(*header < 13) assert(0 && "TYPE NOT SUPPORTED FOR COMPARISON!");
                        diff = memcmp(payload_ptr, rhs_payload_ptr, (*header) - 13);
                        advance = (*header) - 13;
                        break;
                    }
        }
        if(diff) {
            if(is_desc_order(lhs.sort_order_, header-lhs.get_header_ptr()) 
                    || is_desc_order(rhs.sort_order_, rhs_header-rhs.get_header_ptr())) 
                diff *= -1;
            return diff;
        }
        payload_ptr += advance;
        rhs_payload_ptr += advance;
        header++;
        rhs_header++;
    }
    return 0;
}

// user has ownership over the return value.
IndexKey temp_index_key_from_values(Arena* arena, Vector<Value>& vals) {
    int buf_size = 128; // TODO: test small value.
    if(vals.size() >= buf_size || vals.size() > 255) assert(0 && "TOO MANY VALUES FOR AN INDEX KEY!");
    if(vals.size() < 1) return {};

    char* buf = (char*)arena->alloc(buf_size);
    char* bound = buf+buf_size;

    char* header_ptr = buf;
    *header_ptr = (uint8_t)vals.size();
    header_ptr++;
    char* payload_ptr = header_ptr+vals.size();
    for(int i = 0; i < vals.size(); ++i, header_ptr++){
        auto val_type = vals[i].type_;
        uint8_t val_size = (uint8_t) vals[i].size_;
        switch(val_type){
            case NULL_TYPE:
                *header_ptr = (char)SerialType::NIL;
                break;
            case INT:
                *header_ptr = (char)SerialType::INT;
                break;
            case BIGINT:
                *header_ptr = (char)SerialType::LONG;
                break;
            case FLOAT:
                *header_ptr = (char)SerialType::FLOAT;
                break;
            case VARCHAR:
                *header_ptr = val_size + (char)SerialType::TEXT;
                break;
            default:
                assert(0 && "NOT SUPPORTED INDEX KEY TYPE!\n");
        }
        while(payload_ptr + val_size >= bound) {
            int payload_offset = payload_ptr - buf;
            int header_offset  = header_ptr - buf;

            char* new_buf = (char*)arena->alloc(buf_size * 2);
            memcpy(new_buf, buf, buf_size);
            buf_size *= 2; 
            buf = new_buf;

            payload_ptr = buf + payload_offset;
            header_ptr = buf + header_offset;
            bound = buf + buf_size;
        }
        //memcpy(payload_ptr, (char*)vals[i].content_, val_size);
        memcpy(payload_ptr, vals[i].get_ptr(), val_size);
        payload_ptr += val_size;
    }
    return {
        .data_ = buf,
        .size_ = (uint32_t)(payload_ptr - buf),
    };
}

// user has ownership over the return value.
IndexKey null_index_key (Arena* arena, uint8_t size) {
    char* buf = (char*)arena->alloc(size+1);
    memset(buf, (char)SerialType::NIL, size+1);
    *buf = size;
    return {
        .data_ = buf,
        .size_ = (uint32_t)size+1, 
    };
}

bool is_desc_order(char* bitmap, int idx) {
    if(!bitmap) return 0;
    char* cur_byte = bitmap+(idx/8);  
    return *cur_byte & (1 << (idx%8));
}
char* create_sort_order_bitmap(Arena* arena, Vector<NumberedIndexField>& fields) { 
    int size = (fields.size() / 8) + (fields.size() % 8);
    char* bitmap = (char*) arena->alloc(size);
    for(int i = 0; i < fields.size(); ++i){
        if(fields[i].desc_) {
            char* cur_byte = bitmap+(i/8);  
            *cur_byte = *cur_byte | (1 << (i%8));
        }
    }
    return bitmap;
}

IndexKey getIndexKeyFromTuple(Arena* arena, Vector<NumberedIndexField>& fields, Vector<Value>& values, const RecordID rid) {
    Vector<Value> keys;
    for(int i = 0; i < fields.size(); ++i){
        if(fields[i].idx_ >= values.size()) 
            return {};
        keys.push_back(values[fields[i].idx_]);
    }
    assert(keys.size() != 0);
    if(rid.page_id_ != INVALID_PAGE_ID){
        keys.push_back(Value(rid.page_id_.page_num_));
        keys.push_back(Value((int)rid.slot_number_));
    }

    IndexKey res = temp_index_key_from_values(arena, keys);
    res.sort_order_ = create_sort_order_bitmap(arena, fields);
    return res;
}

IndexKey getIndexKeyFromTuple(Arena* arena, Vector<NumberedIndexField>& fields, const Tuple& tuple, const RecordID rid) {
    Vector<Value> keys;
    for(int i = 0; i < fields.size(); ++i){
        if(fields[i].idx_ >= tuple.size()) 
            return {};
        keys.push_back(tuple.get_val_at(fields[i].idx_));
    }
    assert(keys.size() != 0);
    if(rid.page_id_ != INVALID_PAGE_ID){
        keys.push_back(Value(rid.page_id_.page_num_));
        keys.push_back(Value((int)rid.slot_number_));
    }
    IndexKey res = temp_index_key_from_values(arena, keys);
    res.sort_order_ = create_sort_order_bitmap(arena, fields);
    return res;
}

#endif //INDEX_KEY_H
