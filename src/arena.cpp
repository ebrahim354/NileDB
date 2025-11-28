#pragma once

#include "defines.h"
#include "arena.h"
#include <sys/mman.h>

// used resources: 
// https://www.gingerbill.org/article/2019/02/01/memory-allocation-strategies-001
// https://github.com/PixelRifts/c-codebase

inline bool is_power_of_two(uintptr_t x) {
    return (x & (x-1)) == 0;
}

u64 align_forward_u64(u64 ptr, u64 align) {
    u64 p, a, modulo;

    assert(is_power_of_two(align));

    p = ptr;
    a = (u64)align;
    // Same as (p % a) but faster as 'a' is a power of two
    modulo = p & (a-1);
    if (modulo != 0) {
        // If 'p' address is not aligned, push the address to the
        // next value which is aligned
        p += a - modulo;
    }
    return p;
}

void* memory_reserve(u64 size) {
    return mmap(nullptr, size, PROT_NONE, MAP_PRIVATE | MAP_ANON, -1, 0); 
}

void memory_commit(void* memory, u64 size) {
    mprotect(memory, size, PROT_READ | PROT_WRITE);
}

void memory_decommit(void* memory, u64 size) {
    mprotect(memory, size, PROT_NONE);
}

void memory_release(void* memory, u64 size) {
    munmap(memory, size);
}



Arena::Arena() {}
Arena::~Arena() { destroy(); }

void Arena::init() {
    max_ = ARENA_MAX;
    buffer_ = (u8*) memory_reserve(ARENA_MAX);
    assert(buffer_ != nullptr);
}

void Arena::destroy() {
    if(!buffer_) return;
    memory_release(buffer_, max_);
    u8* buffer_      = nullptr;
    u64 alloc_pos_   = 0;
    u64 commit_pos_  = 0;
}

void* Arena::alloc(size_t size) {
    void* memory = nullptr;
    size = align_forward_u64(size, DEFAULT_ALIGNMENT);

    if(alloc_pos_ + size > commit_pos_) {
        u64 commit_size = size;
        commit_size += ARENA_COMMIT_SIZE - 1;
        commit_size -= commit_size % ARENA_COMMIT_SIZE;

        if(commit_pos_ >= max_)
            assert(0 && "Arena is out of memory!\n");
        else {
            memory_commit(buffer_ + commit_pos_, commit_size);
            commit_pos_ += commit_size;
        }
    }
    memory = buffer_ + alloc_pos_;
    alloc_pos_ += size;
    memset(memory, 0, size);
    return memory;
}

void Arena::dealloc(u64 size) {
    if(size > alloc_pos_)
        size = alloc_pos_;
    alloc_pos_ = size;
}

void Arena::dealloc_to(u64 pos) {
    if(pos > max_) pos = max_;
    if(pos < 0) pos = 0;
    alloc_pos_ = pos;
}

void Arena::clear() {
    dealloc(alloc_pos_);
}

ArenaTemp Arena::start_temp_arena() {
    return { 
        .arena_ = this, 
        .pos_   = alloc_pos_
    };
}

void Arena::clear_temp_arena(ArenaTemp temp) {
    dealloc_to(temp.pos_);
}

