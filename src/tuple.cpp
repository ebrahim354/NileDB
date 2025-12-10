#pragma once
#include "tuple.h"

Tuple::Tuple(){}
Tuple::Tuple(Arena* arena): arena_(arena)
{}
void Tuple::setNewSchema(TableSchema* schema, Value val) {
    assert(schema);
    schema_ = schema;
    u32 size = schema_->numOfCols();
    values_ = (Value*) arena_->alloc(size * sizeof(Value));
    for(int i = 0; i < size; ++i)
        values_[i] = val;
}    

int Tuple::size() const {
    if(!schema_) return 0;
    return schema_->numOfCols();
}
// the usual copy constructor makes a shallow copy that only lasts for the lifetime of a pull,
// a pull is the time between two next() calls of the same executor.
// this function makes a deep copy that can last for a custom lifetime based on the passed allocator.
Tuple Tuple::duplicate(Arena* arena) const {
    if(!schema_) return *this;
    auto t = Tuple(arena);
    t.setNewSchema(schema_);
    for(int i = 0; i < size(); ++i)
        t.values_[i] = values_[i].get_copy(arena);
    return t;
}

String Tuple::build_hash_key(Vector<int>& fields) {
    String key = "";
    for(int i = 0; i < fields.size(); ++i){
        int idx = fields[i]; 
        assert(idx < size());
        key += values_[idx].toString();
    }
    return key;
}

String Tuple::stringify() {
    String str = "";
    for(int i = 0; i < size(); ++i) {
        str+= values_[i].toString(); 
        str+= ",";
    }
    return str;
}

void Tuple::put_tuple_at_end(const Tuple* t) {
    assert(t != nullptr);
    int n = t->size();
    assert(n <= size());
    --n;
    for(int i = size() - 1; i >= 0 && n >= 0; --i, --n)
        values_[i] = t->values_[n];
}

void Tuple::put_tuple_at_start(const Tuple* t) {
    //assert(t->values_.size() <= values_.size());
    left_most_rid_ = t->left_most_rid_;
    int mn = std::min(t->size(), size());
    for(int i = 0; i < mn; ++i)
        values_[i] = t->values_[i];
}

void Tuple::nullify(int start, int end) { // end is exclusive: [start, end[
    if(end < 0) end = size();
    assert(start >= 0 && end <= size());
    while(start < end){
        values_[start] = Value(NULL_TYPE);
        ++start;
    }
}

void Tuple::put_val_at(int idx, Value v) {
    assert(idx < size());
    values_[idx] = v;
}


Value& Tuple::get_val_at(uint32_t idx) const {
    assert(idx < size());
    return values_[idx];
}

bool Tuple::is_empty() {
    return (size() == 0);
}
