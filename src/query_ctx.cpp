#pragma once
#include "query_ctx.h"
#include "arena.cpp"

QueryCTX::QueryCTX () {}
/* 
 * use it for debug only
 *
QUERY::QueryCTX (QueryCTX& rhs){
    std::cout << "copy" << std::endl;
}
*/

void QueryCTX::init(int query_string_size) {
    int avg_num_of_tokens = query_string_size / AVG_TOKEN_SIZE;
    tokens_.reserve(avg_num_of_tokens);
    arena_.init();
}

inline bool QueryCTX::matchTokenType(TokenType type){
    return (cursor_ < tokens_.size() && tokens_[cursor_].type_ == type);
}

inline bool QueryCTX::matchAnyTokenType(std::vector<TokenType> types) {
    if(cursor_ >= tokens_.size()) return false;
    for(size_t i = 0; i < types.size(); i++)
        if (tokens_[cursor_].type_ == types[i])
            return true;
    return false;
}

inline bool QueryCTX::matchMultiTokenType(std::vector<TokenType> types) {
    for(size_t i = 0; i < types.size(); i++)
        if (cursor_  + i >= tokens_.size() || tokens_[cursor_ + i].type_ != types[i]) 
            return false;
    return true;
}

// advance the cursor_.
QueryCTX& QueryCTX::operator++() {
    cursor_++;
    return *this;
}

QueryCTX& QueryCTX::operator+=(int val) {
    if(cursor_ + val < tokens_.size() + 1)
        cursor_+= val;
    return *this;
}

// might give out of bounds.
inline Token QueryCTX::getCurrentToken() {
    return tokens_[cursor_];
}

void QueryCTX::clean() {
    for(int i = 0; i < executors_call_stack_.size(); ++i)
        delete executors_call_stack_[i];
    /*
       for(int i = 0; i < operators_call_stack_.size(); ++i)
       delete operators_call_stack_[i];
       for(int i = 0; i < set_operations_.size(); ++i)
       delete set_operations_[i];
       for(int i = 0; i < queries_call_stack_.size(); ++i)
       delete queries_call_stack_[i];*/
    arena_.destroy();
    // TODO: clean specific pointers when dealing with set operations?
}
