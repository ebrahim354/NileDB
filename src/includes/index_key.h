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

// https://www.sqlite.org/fileformat2.html#varint
// return : the number of consumed bytes.
// passing bytes as null will only return the size that this number would take.
u8 varint_encode(u8* bytes, u64 number) {
    u8 sz = 0;
    u8 reserved_bytes[9]{0};
    bool handled = false;
    while(number) {
        if( !handled && number > MAX_8B_VARINT){
            reserved_bytes[sz++] = number & MAX_U8;
            number >>=  8;
            handled = true;
        } else {
            reserved_bytes[sz++] = (number & MAX_I8) | (1 << 7);
            number >>=  7;
        }
    }
    if(sz == 0) sz++;

    if(bytes){
        for(u8 i = 0; i < sz; ++i){
            bytes[i] = reserved_bytes[(sz-(i+1))];
            if(i == sz - 1 && sz != 9)
                bytes[i] = bytes[i]&(~(1<<7));
        }
    }
    return sz;
}

// return : the number of consumed bytes.
u8 varint_decode(u8* bytes, u64* output) {
    u64 val = 0;
    u8 i = 0;
    while(true) {
        if(i == 8){
            val = (val << 8) | (bytes[i++] & MAX_U8);
            break;
        } else {
            val = (val << 7) | (bytes[i++] & MAX_I8);
            bool is_last_byte = ((bytes[i-1]&(1<<7)) == 0);
            if(is_last_byte)
                break;
        }
    }
    if(output)
        *output = val;
    return i;
}


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
 * cell and key/record format used by sqlite : https://www.sqlite.org/fileformat.html#record_format.
 * this format is different from the record format that is used for our tables pages implementation,
 * because it stores the schema from within the record rather than using an external schema to decypher the data.
 * this format is perfect for a btree because the structure will do direct byte-level comparisons without the need 
 * to do any high level conversions using an external schema just to compare two keys,
 * however this comes at the cost of having to store a small header for every single key.
 * you can make the argument that either formats are ok to use both in an index or a table,
 * But the goal is to try multiple ways and understand the different trade offs.
 */
// NOTE: we only use a subset of the serial types that sqlite uses.
enum class SerialType: u8 {
    NIL           = 0, // 0 bytes.
    BOOL_FALSE    = 1, // 0 bytes.
    BOOL_TRUE     = 2, // 0 bytes.
    INT           = 3, // 4 bytes.
    LONG          = 4, // 8 bytes. (timestamps are stored as a long type).
    FLOAT         = 5, // 4 bytes.
    DOUBLE        = 6, // 4 bytes.
    TEXT          = 7, // 7 >= bytes => is TEXT type with size (N-7).
};


struct IndexKey {
    char* data_ = nullptr;
    // this bitmap is not stored on the btree but provided by the catalog.
    char* sort_order_ = nullptr;
    u32 size_ = 0;

    // the user must be sure before using this method that the last 2 entries are in fact a record id.
    RecordID getRID(FileID fid) {
        RecordID rid;
        rid.page_id_.fid_ = fid;
        rid.page_id_.page_num_ = (i32)(*(i32*)((data_+size_)-8));
        rid.slot_number_ = (i32)(*(i32*)((data_+size_)-4));
        return rid;
    }

    void print(std::ofstream& out) {
        if(!data_){
            out << '-';
            return;
        }
        u64 header_size              = 0;
        u8* header          = (u8*)data_ + varint_decode((u8*)data_, &header_size);
        u8* payload_ptr     = (u8*)data_ + header_size; 

        u64 header_val  = 0;
        header_size     -= (header     - (u8*)data_);
        while(header_size) {
            header_size     -= varint_decode(header, &header_val);
            switch(header_val) {
                case (u8)SerialType::NIL:
                    {
                        out << "NIL";
                        header++;
                        break;
                    }
                case (u8)SerialType::BOOL_TRUE:
                    {
                        out << "TRUE";
                        header++;
                        break;
                    }
                case (u8)SerialType::BOOL_FALSE:
                    {
                        out << "FALSE";
                        header++;
                        break;
                    }
                case (u8)SerialType::INT:
                    {
                        out << *(i32*) payload_ptr ;
                        header++;
                        payload_ptr     += 4;
                        break;
                    }
                case (u8)SerialType::FLOAT:
                    {
                        out << *(f32*) payload_ptr ;
                        header++;
                        payload_ptr     += 4;
                        break;
                    }
                case (u8)SerialType::LONG:
                    {
                        out << *(i64*) payload_ptr ;
                        header++;
                        payload_ptr     += 8;
                        break;
                    }
                case (u8)SerialType::DOUBLE:
                    {
                        out << *(f64*) payload_ptr;
                        header++;
                        payload_ptr     += 8;
                        break;
                    }
                case (u8)SerialType::TEXT:
                    {
                        u8 bytes_read = varint_decode(nullptr, &header_val);
                        out << std::string((char*)payload_ptr, header_val - (u8)SerialType::TEXT);
                        header          += bytes_read;
                        payload_ptr     += header_val     - (u8)SerialType::TEXT;
                        break;
                    }
                default:
                    assert(0 && "TYPE NOT SUPPORTED!");
            }
            if(header_size) out << ",";
        }
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

// resize the key to only contain the first n columns(in-place).
// arena is just used for temporary memory and cleared back.
bool index_key_resize(Arena* arena, IndexKey* k, u32 n) {
    assert(k->data_ && k->size_ && n);

    u64 header_size              = 0;
    u8* starting_header          = (u8*)k->data_ + varint_decode((u8*)k->data_, &header_size);
    u8* starting_payload_ptr     = (u8*)k->data_ + header_size; 

    u8* header      = starting_header;
    u8* payload_ptr = starting_payload_ptr;

    u64 header_val  = 0;
    header_size     -= (header     - (u8*)k->data_);
    while(n && header_size) {
        header_size     -= varint_decode(header, &header_val);
        switch(header_val) {
            case (u8)SerialType::NIL:
            case (u8)SerialType::BOOL_TRUE:
            case (u8)SerialType::BOOL_FALSE:
                {
                    header++;
                    break;
                }
            case (u8)SerialType::INT:
            case (u8)SerialType::FLOAT:
                {
                    header++;
                    payload_ptr     += 4;
                    break;
                }
            case (u8)SerialType::LONG:
            case (u8)SerialType::DOUBLE:
                {
                    header++;
                    payload_ptr     += 8;
                    break;
                }
            case (u8)SerialType::TEXT:
                {
                    u8 bytes_read = varint_decode(nullptr, &header_val);
                    header          += bytes_read;
                    payload_ptr     += header_val     - (u8)SerialType::TEXT;
                    break;
                }
            default:
                assert(0 && "TYPE NOT SUPPORTED!");
        }
        n--;
    }

    u32 new_header_size   = header-starting_header;
    u32 new_payload_size  = payload_ptr-starting_payload_ptr;

    u8* new_data_ref = (u8*)arena->alloc(k->size_);
    u8* new_data  = new_data_ref + varint_encode(new_data_ref, (header-(u8*)k->data_));

    memcpy(new_data, starting_header, new_header_size);
    new_data += new_header_size;

    memcpy(new_data, starting_payload_ptr, new_payload_size);
    new_data += new_payload_size;

    u32 new_size = new_data-new_data_ref;
    memcpy(k->data_, new_data_ref, new_size);

    arena->dealloc(k->size_);
    k->size_ = new_size; 
    return true;
}

// same as resize but returns a copy.
IndexKey index_key_resize_cpy(Arena* arena, IndexKey k, i32 n){
    assert(k.data_ && k.size_ && n);

    u64 header_size              = 0;
    u8* starting_header          = (u8*)k.data_ + varint_decode((u8*)k.data_, &header_size);
    u8* starting_payload_ptr     = (u8*)k.data_ + header_size; 

    u8* header      = starting_header;
    u8* payload_ptr = starting_payload_ptr;


    u64 header_val  = 0;
    header_size     -= (header     - (u8*)k.data_);
    while(n && header_size) {
        header_size     -= varint_decode(header, &header_val);
        switch(header_val) {
            case (u8)SerialType::NIL:
            case (u8)SerialType::BOOL_TRUE:
            case (u8)SerialType::BOOL_FALSE:
                {
                    header++;
                    break;
                }
            case (u8)SerialType::INT:
            case (u8)SerialType::FLOAT:
                {
                    header++;
                    payload_ptr     += 4;
                    break;
                }
            case (u8)SerialType::LONG:
            case (u8)SerialType::DOUBLE:
                {
                    header++;
                    payload_ptr     += 8;
                    break;
                }
            case (u8)SerialType::TEXT:
                {
                    u8 bytes_read = varint_decode(nullptr, &header_val);
                    header          += bytes_read;
                    payload_ptr     += header_val     - (u8)SerialType::TEXT;
                    break;
                }
            default:
                assert(0 && "TYPE NOT SUPPORTED!");
        }
        n--;
    }
    u32 new_header_size   = header-starting_header;
    u32 new_payload_size  = payload_ptr-starting_payload_ptr;

    u8* new_data_ref = (u8*)arena->alloc(k.size_);
    u8* new_data  = new_data_ref + varint_encode(new_data_ref, (header-(u8*)k.data_));

    memcpy(new_data, starting_header, new_header_size);
    new_data += new_header_size;

    memcpy(new_data, starting_payload_ptr, new_payload_size);
    new_data += new_payload_size;
    return {
        .data_ = (char*)new_data_ref,
        .size_ = (u32)(new_data-new_data_ref),
    };
}

// -1 ==> lhs < rhs, 0 eq, 1 ==> lhs > rhs
int index_key_cmp(IndexKey lhs, IndexKey rhs) {
    assert(lhs.sort_order_ != nullptr || rhs.sort_order_ != nullptr); // at least one key should know the order
    if(lhs.data_  && !rhs.data_ ) return 1;
    if(rhs.data_  && !lhs.data_ ) return -1;
    if(!rhs.data_ && !lhs.data_ ) return 0;


    u64 header_size     = 0;
    u64 rhs_header_size = 0;

    u8* header     = (u8*)lhs.data_ + varint_decode((u8*)lhs.data_, &header_size);
    u8* rhs_header = (u8*)rhs.data_ + varint_decode((u8*)rhs.data_, &rhs_header_size);

    u8* payload_ptr     = (u8*)lhs.data_ + header_size; 
    u8* rhs_payload_ptr = (u8*)rhs.data_ + rhs_header_size;

    // subtract the bytes that store the header size itself and start iterating over columns.
    header_size     -= (header     - (u8*)lhs.data_);
    rhs_header_size -= (rhs_header - (u8*)rhs.data_);

    i32 idx = 0;
    while(header_size  && rhs_header_size) {
        u64 header_val = 0;
        u64 rhs_header_val = 0;
        header_size     -= varint_decode(header, &header_val);
        rhs_header_size -= varint_decode(rhs_header, &rhs_header_val);

        if(header_val != (u8)SerialType::NIL && rhs_header_val == (u8)SerialType::NIL)   // rhs is null.
            return 1;
        if(header_val == (u8)SerialType::NIL && rhs_header_val != (u8)SerialType::NIL)   // lhs is null.
            return -1;
        if(header_val == (u8)SerialType::NIL && rhs_header_val == (u8)SerialType::NIL) { // both are null.
            header++;
            rhs_header++;
            continue; 
        }

        if(        (header_val >= (u8)SerialType::TEXT        && rhs_header_val <  (u8)SerialType::TEXT) 
                || (header_val <  (u8)SerialType::TEXT        && rhs_header_val >= (u8)SerialType::TEXT)
          ) {
            assert(0 && "INVALID COMPARISON");
        }
        if(header_val < (u8)SerialType::TEXT && header_val > (u8)SerialType::BOOL_TRUE 
                && header_val != rhs_header_val) {
            assert(0 && "TODO: SUPPORT NUMERIC CASTING.");
        }

        i32 diff = 0;
        switch(header_val){
            case (u8)SerialType::BOOL_TRUE:
            case (u8)SerialType::BOOL_FALSE:
                {
                    assert(rhs_header_val <= (u8)SerialType::BOOL_TRUE);
                    diff = header_val -  rhs_header_val;
                    header++;
                    rhs_header++;
                    break;
                }
            case (u8)SerialType::INT:
                {
                    diff = *(i32*) payload_ptr - *(i32*) rhs_payload_ptr;
                    header++;
                    rhs_header++;
                    payload_ptr     += 4;
                    rhs_payload_ptr += 4;
                    break;
                }
            case (u8)SerialType::LONG:
                {
                    diff = *(i64*) payload_ptr - *(i64*) rhs_payload_ptr;
                    header++;
                    rhs_header++;
                    payload_ptr     += 8;
                    rhs_payload_ptr += 8;
                    break;
                }
            case (u8)SerialType::FLOAT:
                {
                    f32 res = *(f32*) payload_ptr  - *(f32*) rhs_payload_ptr;
                    if(fabsf(res) > EPS){
                        if(res < 0.0) diff = -1;
                        else diff = 1;
                    }
                    header++;
                    rhs_header++;
                    payload_ptr     += 4;
                    rhs_payload_ptr += 4;
                    break;
                }
            case (u8)SerialType::DOUBLE:
                {
                    f64 res = *(f64*) payload_ptr  - *(f64*) rhs_payload_ptr;
                    if(fabs(res) > EPS){
                        if(res < 0.0) diff = -1;
                        else diff = 1;
                    }
                    header++;
                    rhs_header++;
                    payload_ptr     += 8;
                    rhs_payload_ptr += 8;
                    break;
                }
            case (u8)SerialType::NIL:
                {
                    assert(0); // Unreachable.
                    break;
                }
            case (u8)SerialType::TEXT:
                {
                    if(header_val < (u8)SerialType::TEXT || rhs_header_val < (u8)SerialType::TEXT)
                        assert(0 && "TYPE NOT SUPPORTED FOR COMPARISON!");

                    u8 lhs_bytes_read = varint_decode(nullptr, &header_val);
                    u8 rhs_bytes_read = varint_decode(nullptr, &rhs_header_val);

                    diff = memcmp(payload_ptr, rhs_payload_ptr, 
                            std::min(header_val, rhs_header_val)-(u8)SerialType::TEXT);

                    payload_ptr     += header_val     - (u8)SerialType::TEXT;
                    rhs_payload_ptr += rhs_header_val - (u8)SerialType::TEXT;

                    header     += lhs_bytes_read;
                    rhs_header += rhs_bytes_read;
                    break;
                }
            default:
                assert(0 && "TYPE NOT SUPPORTED!");
        }
        if(diff) {
            if(is_desc_order(lhs.sort_order_, idx) 
                    || is_desc_order(rhs.sort_order_, idx)) 
                diff *= -1;
            return diff;
        }
        ++idx;
    }
    return 0;
}

IndexKey temp_index_key_from_values(Arena* arena, const Vector<Value>& vals) {
    if(vals.size() < 1) {
        assert(0);
        return {};
    }
    u32 header_size = 0;
    u32 body_size = 0;
    for(u32 i = 0; i < vals.size(); ++i) {
        switch(vals[i].type_){
            case BOOLEAN  : 
            case NULL_TYPE:
                header_size++;
                break;
            case INT:
            case FLOAT:
                header_size++;
                body_size += 4;
                break;
            case TIMESTAMP:
            case BIGINT:
            case DOUBLE:
                header_size++;
                body_size += 8;
                break;
            case VARCHAR:
                header_size += varint_encode(nullptr, vals[i].size_ + (u8)SerialType::TEXT);
                body_size   += vals[i].size_;
                break;
            case INVALID:
            case OVERFLOW_ITERATOR:
            case EXECUTOR_ID:
            default:
                assert(0 && "Not suported index key types!");
        }
    }
    u8 byte_cnt = varint_encode(nullptr, header_size);
    header_size += byte_cnt;

    // in case of adding the byte_cnt would change the header_size for example: 
    // header_size is 127, byte count is 1 after adding it to the total
    // the byte_cnt of the byte_cnt changes to 2 then we need an extra byte.
    if(varint_encode(nullptr, header_size) != byte_cnt)
        header_size++;

    u32 total_size = header_size + body_size;
    u8* buf = (u8*)arena->alloc(total_size);

    // store the header size.
    u8* header_ptr = buf;
    u8 used_bytes = varint_encode(header_ptr, header_size);

    u8* payload_ptr = header_ptr + header_size; 
    header_ptr     += used_bytes;

    for(int i = 0; i < vals.size(); ++i) {
        Type val_type = vals[i].type_;
        u32 val_size = (u32) vals[i].size_;
        switch(val_type){
            case BOOLEAN  : 
                *header_ptr = vals[i].getBoolVal() ? (u8)SerialType::BOOL_TRUE : (u8)SerialType::BOOL_FALSE;
                header_ptr++;
                break;
            case NULL_TYPE:
                *header_ptr = (u8)SerialType::NIL;
                header_ptr++;
                break;
            case INT:
                *header_ptr = (u8)SerialType::INT;
                header_ptr++;
                break;
            case FLOAT:
                *header_ptr = (u8)SerialType::FLOAT;
                header_ptr++;
                break;
            case TIMESTAMP:
            case BIGINT:
                *header_ptr = (u8)SerialType::LONG;
                header_ptr++;
                break;
            case DOUBLE:
                *header_ptr = (u8)SerialType::DOUBLE;
                header_ptr++;
                break;
            case VARCHAR:
                header_ptr  += varint_encode(header_ptr, vals[i].size_ + (u8)SerialType::TEXT);
                break;
            case INVALID:
            case OVERFLOW_ITERATOR:
            case EXECUTOR_ID:
            default:
                assert(0 && "Not suported index key types!");
        }
        if(val_type != BOOLEAN && val_type != NULL_TYPE) {
            memcpy(payload_ptr, vals[i].get_ptr(), val_size);
            payload_ptr += val_size;
        }
    }
    assert(payload_ptr-buf == total_size);
    return {
        .data_ = (char*) buf,
        .size_ = total_size,
    };
}

IndexKey null_index_key (Arena* arena, uint8_t size) {
    char* buf = (char*)arena->alloc(size+1);
    memset(buf, (char)SerialType::NIL, size+1);
    *buf = size;
    return {
        .data_ = buf,
        .size_ = (u32)size+1, 
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
        keys.push_back(Value((i32)rid.slot_number_));
    }
    IndexKey res = temp_index_key_from_values(arena, keys);
    res.sort_order_ = create_sort_order_bitmap(arena, fields);
    return res;
}

#endif //INDEX_KEY_H
