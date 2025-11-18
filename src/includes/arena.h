#ifndef ARENA_H
#define ARENA_H

#include "defines.h"
#include <sys/mman.h>

// used resources: 
// https://www.gingerbill.org/article/2019/02/01/memory-allocation-strategies-001
// https://github.com/PixelRifts/c-codebase

#ifndef DEFAULT_ALIGNMENT
#define DEFAULT_ALIGNMENT sizeof(void *)
#endif

#define ARENA_MAX         Megabytes(1)
#define ARENA_COMMIT_SIZE Kilobytes(8)

inline bool is_power_of_two(uintptr_t x);

u64 align_forward_u64(u64 ptr, u64 align);


void* memory_reserve(u64 size);

void memory_commit(void* memory, u64 size);

void memory_decommit(void* memory, u64 size);

void memory_release(void* memory, u64 size);


struct Arena;

struct ArenaTemp {
    Arena* arena_;
    u64 pos_;
};

struct Arena {
    u8* buffer_      = nullptr;
    u64 max_         = 0;
    u64 alloc_pos_   = 0;
    u64 commit_pos_  = 0;

    Arena();
    ~Arena();

    void init();

    void destroy();

    void* alloc(size_t size);

    void dealloc(u64 size);

    void dealloc_to(u64 pos);

    void clear();

    ArenaTemp start_temp_arena();

    void clear_temp_arena(ArenaTemp temp);
};

#endif // ARENA_H
