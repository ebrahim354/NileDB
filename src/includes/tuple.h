#ifndef TUPLE_H
#define TUPLE_H

struct Tuple {
    Value* values_ = nullptr;
    Arena* arena_ = nullptr;
    RecordID left_most_rid_;
    TableSchema* schema_ = nullptr;
    /*
    Record* rec_;
    */
    // Delete copy constructor
    //Tuple(const Tuple&) = delete; 

    Tuple();
    Tuple(Arena* arena);
    void setNewSchema(TableSchema* schema, Value val = Value(NULL_TYPE));
//    Tuple() = delete;

    int size() const;
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
    /*
       void init(TableSchema* schema){
       schema_ = schema;
       values_.resize(schema_.numOfCols());
       }*/
};

#endif //TUPLE_H
