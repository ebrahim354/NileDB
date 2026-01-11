#ifndef TUPLE_H
#define TUPLE_H

struct Tuple {
    Value* values_ = nullptr;
    Arena* arena_ = nullptr;
    u32 width_ = 0;
    RecordID left_most_rid_;

    Tuple();
    Tuple(Arena* arena);
    void resize(u32 new_size, Value val = Value(NULL_TYPE));

    u32 size() const;
    // the usual copy constructor makes a shallow copy that only lasts for the lifetime of a pull,
    // a pull is the time between two next() calls of the same executor.
    // this function makes a deep copy that can last for a custom lifetime based on the passed allocator,
    Tuple duplicate(Arena* arena) const;
    String build_hash_key(Vector<int>& fields);
    String stringify();
    void put_tuple_at_end(const Tuple* t);
    void put_tuple_at_start(const Tuple* t);
    void nullify(int start, int end);
    void put_val_at(int idx, Value v);
    Value& get_val_at(uint32_t idx) const;
    bool is_empty();
};

#endif //TUPLE_H
