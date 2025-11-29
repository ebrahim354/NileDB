#pragma once
#include "tuple.h"
Tuple::Tuple(){}
Tuple::Tuple(TableSchema* schema, Value val) {
    assert(schema);
    schema_ = schema;
    values_.resize(schema_->numOfCols());
    for(int i = 0; i < values_.size(); ++i)
        values_[i] = val;
}
//    Tuple() = delete;

int Tuple::size(){
    if(!schema_) return 0;
    return schema_->numOfCols();
}
// the usual copy constructor makes a shallow copy that only lasts for the lifetime of a pull,
// a pull is the time between two next() calls of the same executor.
// this function makes a deep copy that can last for a custom lifetime based on the passed allocator.
Tuple Tuple::duplicate(Arena* arena){
    if(!schema_) return *this;
    auto t = Tuple(schema_);
    for(int i = 0; i < values_.size(); ++i)
        t.values_[i] = values_[i].get_copy(arena);
    return t;
}

std::string Tuple::build_hash_key(std::vector<int>& fields) {
    std::string key = "";
    for(int i = 0; i < fields.size(); ++i){
        int idx = fields[i]; 
        assert(idx < values_.size());
        key += values_[idx].toString();
    }
    return key;
}

std::string Tuple::stringify() {
    std::string str = "";
    for(int i = 0; i < values_.size(); ++i) {
        str+= values_[i].toString(); 
        str+= ",";
    }
    return str;
}

void Tuple::put_tuple_at_end(const Tuple* t) {
    assert(t != nullptr);
    int n = t->values_.size();
    assert(n <= values_.size());
    --n;
    for(int i = values_.size() - 1; i >= 0 && n >= 0; --i, --n)
        values_[i] = t->values_[n];
}

void Tuple::put_tuple_at_start(const Tuple* t) {
    assert(t->values_.size() <= values_.size());
    for(int i = 0; i < t->values_.size(); ++i)
        values_[i] = t->values_[i];
}

void Tuple::nullify(int start, int end) { // end is exclusive: [start, end[
    if(end < 0) end = values_.size();
    assert(start >= 0 && end <= values_.size());
    while(start < end){
        values_[start] = Value(NULL_TYPE);
        ++start;
    }
}

void Tuple::put_val_at(int idx, Value v) {
    assert(idx < values_.size());
    values_[idx] = v;
}


Value& Tuple::get_val_at(uint32_t idx) {
    assert(idx < values_.size());
    return values_[idx];
}

bool Tuple::is_empty() {
    return (values_.size() == 0);
}
