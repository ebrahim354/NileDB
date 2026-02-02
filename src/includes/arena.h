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

#define ARENA_MAX         Megabytes(512)
#define ARENA_COMMIT_SIZE Kilobytes(512)

inline bool is_power_of_two(uintptr_t x);

u64 align_forward_u64(u64 ptr, u64 align);


void* memory_reserve(u64 size);

void memory_commit(void* memory, u64 size);

void memory_decommit(void* memory, u64 size);

void memory_release(void* memory, u64 size);

#define Vector std::pmr::vector
#define String std::pmr::string

struct Arena;

struct ArenaTemp {
    Arena* arena_;
    u64 pos_;
};

struct Arena : public std::pmr::memory_resource {
    u8* buffer_      = nullptr;
    u64 max_         = 0;
    u64 alloc_pos_   = 0;
    u64 commit_pos_  = 0;

    Arena();
    ~Arena();

    void init();

    void destroy();

    // sometimes calling alloc with alignment = 0
    // (for example appending strings into the arena to concatenate them)
    // could mess with the total alignment of the arena alloc_pos_
    // so this methods makes sure it's realigned correctly
    void realign();

    void* alloc(size_t size, u64 alignment = DEFAULT_ALIGNMENT);

    void dealloc(u64 size);

    void dealloc_to(u64 pos);

    void clear();

    ArenaTemp start_temp_arena();

    void clear_temp_arena(ArenaTemp temp);

    void* do_allocate(size_t bytes, size_t alignment) override;

    void do_deallocate(void* p, size_t bytes, size_t alignment) override;

    bool do_is_equal(const std::pmr::memory_resource& other) const noexcept override;
};

#endif // ARENA_H
