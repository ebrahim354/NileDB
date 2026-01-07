#ifndef DATA_STRUCTURES_H
#define DATA_STRUCTURES_H

#include <cassert>
#include <cstring>
#include <memory_resource>

#include "defines.h"
#include "arena.h"

// string types and helpers.

struct String8 {
    u8* str_;
    u64 size_;

    const bool operator==(const String8& other) const {
        if(size_ != other.size_) return false;
        return std::memcmp(str_, other.str_, size_) == 0;
    }

    const bool operator<(const String8& other) const {
        u64 min_sz = size_;
        if(other.size_ < size_) min_sz = other.size_;
        int res = std::strncmp((char*)str_, (char*)other.str_, min_sz);
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

#define str_lit(s) (String8) { .str_ = (u8*)(s), .size_ = sizeof(s) - 1 }
#define str_lit_null(s) (String8) { .str_ = (u8*)(s), .size_ = sizeof(s) }

const String8 NULL_STRING8 = {
    .str_ = 0,
    .size_ = 0,
};

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
    std::memcpy(s.str_, other.str_, other.size_);
    return s;
}

String8 str_cat(Arena* arena, String8 a, String8 b, bool null_terminated = false) {
    String8 res = {};
    res.size_ = a.size_ + b.size_ + null_terminated;
    res.str_ = (u8*)arena->alloc(res.size_);
    std::memcpy(res.str_, a.str_, a.size_);
    std::memcpy(res.str_ + a.size_, b.str_, b.size_);
    if(null_terminated) res.str_[res.size_ - 1] = 0;
    return res;
}

bool str_ends_with(String8 a, String8 b, bool skip_null = true) {
    if(skip_null && b.last_char() == 0) b.size_--;
    if(skip_null && a.last_char() == 0) a.size_--;
    if(b.size_ > a.size_) return false;
    u64 right_a_padding = a.size_ - b.size_;
    a.str_  += right_a_padding;
    a.size_ -= right_a_padding;
    return (a == b);
}

bool str_starts_with(String8 a, String8 b) {
    if(b.size_ > a.size_) return false;
    a.size_ = b.size_;
    return (a == b);
}

std::vector<String8> str_split(String8 s, String8 needle)
{
    std::vector<String8> arr;
    arr.reserve(4);
    u8* last_ptr = s.str_;
    while(s.size_){
        if(str_starts_with(s, needle)) {
            arr.push_back({ .str_ = last_ptr, .size_ = (u64)(s.str_ - last_ptr) });
            s.str_  += needle.size_;
            s.size_ -= needle.size_;
            last_ptr = s.str_;
            continue;
        }
        s.str_++;
        s.size_--;
    }
    if(s.str_ - last_ptr > 0) arr.push_back({ .str_ = last_ptr, .size_ = (u64)(s.str_ - last_ptr) });
    return arr;
}


// TODO: write a better implementation for these functions.
i64 str_to_i64(String8 s) {
    char str[64];
    if (s.size_ > 63)
    {
        s.size_ = 63;
    }
    memcpy(str, s.str_, s.size_);
    str[s.size_] = 0;
    return strtoll(str, NULL, 10);
}

float str_to_f64(String8 s) {
    char str[64];
    if (s.size_ > 63)
    {
        s.size_ = 63;
    }
    memcpy(str, s.str_, s.size_);
    str[s.size_] = 0;
    return atof(str);
}

String8 i64_to_str(Arena* arena, i64 t){
    if(t == 0) return str_lit("0");

    char temp_str[64];
    u8 idx = 0;
    if(t < 0)  {
        temp_str[idx++] = '-';
        t *= -1;
    }
    while(t > 0){
        temp_str[idx++] = ((t%10) + '0');
        t /= 10;
    }
    u8* ptr = (u8*)arena->alloc(idx);
    u64 size = idx;
    while(idx > 0) {
        ptr[size-idx] = temp_str[idx];
        idx--;
    }
    return {.str_ = ptr, .size_ = size};
}




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
        return std::memcmp(lhs.str_, rhs.str_, lhs.size_) == 0;
    }
};

bool char_is_digit(u8 c) {
    return (c <= '9' && c >= '0');
}


#endif //DATA_STRUCTURES_H
