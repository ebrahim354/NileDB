#ifndef DATA_STRUCTURES_H
#define DATA_STRUCTURES_H

#include "defines.h"
#include "arena.h"
#include <assert.h>
#include <string.h>
#include <memory_resource>

// string types and helpers.

struct String8 {
    u8* str_;
    u64 size_;

    const bool operator==(const String8& other) const {
        if(size_ != other.size_) return false;
        return memcmp(str_, other.str_, size_) == 0;
    }

    const bool operator<(const String8& other) const {
        u64 min_sz = size_;
        if(other.size_ < size_) min_sz = other.size_;
        int res = strncmp((char*)str_, (char*)other.str_, min_sz);
        return res < 0;
    }

    const u8& operator[](u64 idx) const {
        assert(idx < size_);
        return str_[idx];
    }

    const u8& last_char() const {
        assert(size_ > 0);
        return str_[size_-1];
    }
};

const String8 NULL_STRING8 = {
    .str_ = 0,
    .size_ = 0,
};


std::pmr::string to_string(String8 str){
    return std::pmr::string((char*)str.str_, str.size_);
}

String8 str_alloc(Arena* arena, u64 size) {
    String8 s = {};
    s.str_ = (u8*)arena->alloc(size);
    s.size_ = size;
    return s;
}

String8 str_copy(Arena* arena, String8 other) {
    String8 s = {};
    s.str_ = (u8*)arena->alloc(other.size_);
    s.size_ = other.size_;
    memcpy(s.str_, other.str_, other.size_);
    return s;
}




#define str_lit(s) (String8) { .str_ = (u8*)(s), .size_ = sizeof(s) - 1 }


// source: 
// https://en.wikipedia.org/wiki/Fowler%E2%80%93Noll%E2%80%93Vo_hash_function
#define FNV_offset_basis ((u64)0xcbf29ce484222325)
#define FNV_prime ((u64)0x100000001b3) 


// string hash(case insensetive)
struct String_ihash {
    u64 operator()(const String8 str) const {
        u64 hash = FNV_offset_basis;
        for (int i = 0; i < str.size_; i++) {
            hash *= FNV_prime;
            hash ^= toupper(str[i]);
        }
        return hash;
    }
};

// string hash(case sensetive)
struct String_hash {
    u64 operator()(const String8 str) const {
        u64 hash = FNV_offset_basis;
        for (int i = 0; i < str.size_; i++) {
            hash *= FNV_prime;
            hash ^= str[i];
        }
        return hash;
    }
};

// string comparetor(case insensetive).
struct String_ieq {
    bool operator()(const String8& lhs, const String8& rhs) const
    {
        if(lhs.size_ != rhs.size_) return false;
        u64 cnt = 0;
        while(cnt < lhs.size_ && toupper(lhs[cnt]) == toupper(rhs[cnt])){
            cnt++;
        }
        return cnt == lhs.size_;
    }
};

// string comparetor(case sensetive).
struct String_eq {
    bool operator()(const String8& lhs, const String8& rhs) const
    {
        if(lhs.size_ != rhs.size_) return false;
        return memcmp(lhs.str_, rhs.str_, lhs.size_) == 0;
    }
};

bool char_is_digit(u8 c) {
    return (c <= '9' && c >= '0');
}


#endif //DATA_STRUCTURES_H
