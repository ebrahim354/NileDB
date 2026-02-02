#ifndef QUERY_CTX_H
#define QUERY_CTX_H

#include "tokenizer.h"
#include "error.h"
#include "executor.h"
#include "algebra_operation.h"
#include "query_data.h"
#include "arena.h"

#define TableID FileID
#define IndexID FileID

// assuming average token size is 4.
#define AVG_TOKEN_SIZE 4

// each query has its own context that is passed around the system.
struct QueryCTX {
    QueryCTX ();
    // Delete copy constructor
    QueryCTX(const QueryCTX&) = delete; 

    // Delete copy assignment operator
    QueryCTX& operator=(const QueryCTX&) = delete;

    // Delete move constructor
    QueryCTX(QueryCTX&&) = delete;

    // Delete move assignment operator
    QueryCTX& operator=(QueryCTX&&) = delete;

    void init(String8 query);
    void init(const char* query, u64 query_size);
    inline bool matchTokenType(TokenType type);
    inline bool matchAnyTokenType(Vector<TokenType> types);
    inline bool matchMultiTokenType(Vector<TokenType> types);
    // advance the cursor_.
    QueryCTX& operator++(); 
    QueryCTX& operator+=(int val);
    // might give out of bounds.
    inline Token getCurrentToken();

    void clean();

    String8 query_;
    Vector<Token> tokens_;
    Vector<QueryData*> queries_call_stack_ = {};
    Vector<QueryData*> set_operations_ = {};
    Vector<IndexIterator*> index_handles_ = {};
    Vector<TableIterator*> table_handles_ = {};
    Vector<AlgebraOperation*> operators_call_stack_ = {};
    Vector<Executor*> executors_call_stack_ = {};
    std::vector<Tuple> query_inputs = {};
    Arena arena_;  // this arena lasts for the entire duration of the query.
    Arena temp_arena_; // this arena gets cleaned up after every call to next().
    uint32_t cursor_ = 0;
    Error error_status_ = Error::NO_ERROR;
    bool direct_execution_ = 0;    // directly execeute without translating to algebra, Usually set to true for DDL.
};

#endif // QUERY_CTX_H
