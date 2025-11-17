#pragma once
#include "tokenizer.cpp"
#include "error.cpp"
#include "executor.cpp"
#include "algebra_operation.cpp"
#include "query_data.cpp"
#include "arena.cpp"

struct ExpressionNode;

// assuming average token size is 4.
#define AVG_TOKEN_SIZE 4

// each query has its own context that is passed around the system.
struct QueryCTX {
    QueryCTX () {}
    /* use it for debug only
    QueryCTX (QueryCTX& rhs){
        std::cout << "copy" << std::endl;
    }*/

    // Delete copy constructor
    QueryCTX(const QueryCTX&) = delete; 

    // Delete copy assignment operator
    QueryCTX& operator=(const QueryCTX&) = delete;

    // Delete move constructor
    QueryCTX(QueryCTX&&) = delete;

    // Delete move assignment operator
    QueryCTX& operator=(QueryCTX&&) = delete;

    void init(int query_string_size) {
        int avg_num_of_tokens = query_string_size / AVG_TOKEN_SIZE;
        tokens_.reserve(avg_num_of_tokens);
        arena_.init();
    }

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

    void clean(){
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

    std::vector<Token> tokens_;
    std::vector<QueryData*> queries_call_stack_ = {};
    std::vector<QueryData*> set_operations_ = {};
    std::vector<AlgebraOperation*> operators_call_stack_ = {};
    std::vector<Executor*> executors_call_stack_ = {};
    Arena arena_;
    uint32_t cursor_ = 0;
    Error error_status_ = Error::NO_ERROR;
    bool direct_execution_ = 0;    // directly execeute without translating to algebra, Usually set to true for DDL.
};
