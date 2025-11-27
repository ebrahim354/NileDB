#ifndef QUERY_CTX_H
#define QUERY_CTX_H

#include "tokenizer.h"
#include "error.h"
#include "executor.h"
#include "algebra_operation.h"
#include "query_data.h"
#include "arena.h"

// assuming average token size is 4.
#define AVG_TOKEN_SIZE 4

// each query has its own context that is passed around the system.
struct QueryCTX {
    QueryCTX ();
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

    void init(int query_string_size);
    inline bool matchTokenType(TokenType type);
    inline bool matchAnyTokenType(std::vector<TokenType> types);
    inline bool matchMultiTokenType(std::vector<TokenType> types);
    // advance the cursor_.
    QueryCTX& operator++(); 
    QueryCTX& operator+=(int val);
    // might give out of bounds.
    inline Token getCurrentToken();

    void clean();

    std::vector<Token> tokens_;
    std::vector<QueryData*> queries_call_stack_ = {};
    std::vector<QueryData*> set_operations_ = {};
    std::vector<IndexIterator*> index_handles_ = {};
    std::vector<TableIterator*> table_handles_ = {};
    std::vector<AlgebraOperation*> operators_call_stack_ = {};
    std::vector<Executor*> executors_call_stack_ = {};
    std::vector<Tuple> query_inputs = {};
    Arena arena_;
    uint32_t cursor_ = 0;
    Error error_status_ = Error::NO_ERROR;
    bool direct_execution_ = 0;    // directly execeute without translating to algebra, Usually set to true for DDL.
};

#endif // QUERY_CTX_H
