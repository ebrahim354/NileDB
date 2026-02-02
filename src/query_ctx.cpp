#pragma once
#include "arena.h"
#include "query_ctx.h"

QueryCTX::QueryCTX () {}

void QueryCTX::init(String8 query) {
    init((char*)query.str_, query.size_);
}

void QueryCTX::init(const char* query, u64 query_size) {
    //query_ = std::string(query, query_size);
    query_.str_ = (u8*)query;
    query_.size_ = query_size;
    int query_string_size = query_size;
    int avg_num_of_tokens = query_string_size / AVG_TOKEN_SIZE;
    tokens_.reserve(avg_num_of_tokens);
    arena_.init();
    temp_arena_.init();
}

inline bool QueryCTX::matchTokenType(TokenType type){
    return (cursor_ < tokens_.size() && tokens_[cursor_].type_ == type);
}

inline bool QueryCTX::matchAnyTokenType(Vector<TokenType> types) {
    if(cursor_ >= tokens_.size()) return false;
    for(size_t i = 0; i < types.size(); i++)
        if (tokens_[cursor_].type_ == types[i])
            return true;
    return false;
}

inline bool QueryCTX::matchMultiTokenType(Vector<TokenType> types) {
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
    for(int i = 0; i < index_handles_.size(); ++i){
        index_handles_[i]->clear();
    }
    for(int i = 0; i < table_handles_.size(); ++i){
        table_handles_[i]->destroy();
    }
    arena_.destroy();
    temp_arena_.destroy();
}
