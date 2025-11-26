#ifndef TUPLE_H
#define TUPLE_H

struct Tuple {
    std::vector<Value> values_;
    /*
    Record* rec_;
    */
    TableSchema* schema_ = nullptr;
    Tuple();
    Tuple(TableSchema* schema, Value val = Value(NULL_TYPE));
//    Tuple() = delete;

    int size();
    // the usual copy constructor makes a shallow copy that only lasts for the lifetime of a pull,
    // a pull is the time between two next() calls of the same executor.
    // this function makes a deep copy that can last for a custom lifetime based on the passed allocator,
    // TODO: use an allocator instead of malloc.
    Tuple duplicate();
    std::string build_hash_key(std::vector<int>& fields);
    std::string stringify();
    void put_tuple_at_end(const Tuple* t);
    void put_tuple_at_start(const Tuple* t);
    void nullify(int start, int end);
    void put_val_at(int idx, Value v);
    Value& get_val_at(uint32_t idx);
    bool is_empty();
    /*
       void init(TableSchema* schema){
       schema_ = schema;
       values_.resize(schema_.numOfCols());
       }*/
};

#endif //TUPLE_H
