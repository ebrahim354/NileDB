#pragma once
#include "tokenizer.cpp"
#include "error.cpp"

struct ExpressionNode;
struct QueryData;
struct AlgebraOperation;
struct Executor;

// assuming average token size is 4.
#define AVG_TOKEN_SIZE 4

// each query has its own context that is passed around the system.
struct QueryCTX {
    QueryCTX (int query_string_size) {
        int avg_num_of_tokens = query_string_size / AVG_TOKEN_SIZE;
        tokens_.reserve(avg_num_of_tokens);
    }
    /* use it for debug only
    QueryCTX (QueryCTX& rhs){
        std::cout << "copy" << std::endl;
    }*/

    inline bool matchTokenType(TokenType type){
        return (cursor_ < tokens_.size() && tokens_[cursor_].type_ == type);
    }

    inline bool matchAnyTokenType(std::vector<TokenType> types){
        if(cursor_ >= tokens_.size()) return false;
        for(size_t i = 0; i < types.size(); i++)
            if (tokens_[cursor_].type_ == types[i])
                return true;
        return false;
    }

    inline bool matchMultiTokenType(std::vector<TokenType> types){
        for(size_t i = 0; i < types.size(); i++)
            if (cursor_  + i >= tokens_.size() || tokens_[cursor_ + i].type_ != types[i]) 
                return false;
        return true;
    }
    // advance the cursor_.
    QueryCTX& operator++() {
        cursor_++;
        return *this;
    }
    QueryCTX& operator+=(int val) {
        if(cursor_ + val < tokens_.size() + 1)
            cursor_+= val;
        return *this;
    }
    // might give out of bounds.
    inline Token getCurrentToken(){
        return tokens_[cursor_];
    }

    std::vector<Token> tokens_;
    std::vector<QueryData*> queries_call_stack_ = {};
    std::vector<QueryData*> set_operations_ = {};
    std::vector<AlgebraOperation*> operators_call_stack_ = {};
    std::vector<Executor*> executors_call_stack_ = {};
    uint32_t cursor_ = 0;
    Error error_status_ = Error::NO_ERROR;
    bool direct_execution_ = 0;    // directly execeute without translating to algebra, Usually set to true for DDL.
};
