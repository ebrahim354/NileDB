#ifndef DEFINES_H
#define DEFINES_H

// Unsigned int types. 
#define u8 uint8_t 
#define u16 uint16_t
#define u32 uint32_t
#define u64 uint64_t

// Signed int types.
#define i8 int8_t   
#define i16 int16_t 
#define i32 int32_t 
#define i64 int64_t 

// Floating point types
#define f32 float 
#define f64 double

// boolean
#define b8 bool

#define Gigabytes(count) (u64) (count * 1024 * 1024 * 1024)
#define Megabytes(count) (u64) (count * 1024 * 1024)
#define Kilobytes(count) (u64) (count * 1024)

#define ALLOCATE(arena, type) ((type *)((arena).alloc(sizeof(type))))
#define ALLOCATE_INIT(arena, ptr, type, ...) \
    do { \
        ptr = ALLOCATE(arena, type); \
        new(ptr) type(); \
        ((type *)ptr)->init(__VA_ARGS__); \
    } while(0)

#define ALLOCATE_CONSTRUCT(arena, ptr, type, ...) \
    do { \
        ptr = ALLOCATE(arena, type); \
        new(ptr) type(); \
        ((type *)ptr)->construct(__VA_ARGS__); \
    } while(0)



#endif // DEFINES_H
