#ifndef QUERY_DATA_H
#define QUERY_DATA_H

#include "catalog.h"
#include "ast_nodes.h"

class Catalog;


enum QueryType {
    SELECT_DATA = 0,
    CREATE_TABLE_DATA,
    CREATE_INDEX_DATA,
    INSERT_DATA,
    DELETE_DATA,
    UPDATE_DATA,
    // set operations.
    UNION,
    INTERSECT,
    EXCEPT
};



struct QueryData {
    void init(QueryType type, int parent_idx);

    QueryType type_;
    int idx_ = -1;          // every query must have an id starting from 0 even the top level query.
    int parent_idx_ = -1;   // -1 means this query is the top level query.

    std::vector<std::string> tables_      = {};
    std::vector<std::string> table_names_ = {};
    std::vector<JoinedTablesData> joined_tables_ = {};
    ExpressionNode* where_ = nullptr;

    // a mark for subqueries to indicate if they are corelated to their parent or not.
    // the top level query is always not corelated and has a parent_query_idx of -1.
    // TODO: if the top level query is corelated that is an error, indicate that.
    bool is_corelated_ = false; 
};

// SQL statement data wrappers.
struct SelectStatementData : QueryData {
    void init (int parent_idx);

    std::vector<std::string> field_names_ = {};
    std::vector<ExpressionNode*> fields_ = {};
    std::vector<AggregateFuncNode*> aggregates_;  
    std::vector<int> order_by_list_ = {};
    std::vector<ASTNode*> group_by_ = {}; 
    bool has_star_ = false;
    ExpressionNode* having_ = nullptr;
    bool distinct_ = false;

};

struct Intersect : QueryData {
    void init(int parent_idx, QueryData* lhs, Intersect* rhs, bool all);

    QueryData* cur_ = nullptr;
    QueryData* next_ = nullptr;
    bool all_ = false;
};

struct UnionOrExcept : QueryData {
    void init(QueryType type, int parent_idx, QueryData* lhs, UnionOrExcept* rhs, bool all);

    QueryData* cur_ = nullptr;
    QueryData* next_ = nullptr; 
    bool all_ = false;
};




struct CreateTableStatementData : QueryData {
    void init(int parent_idx);

    std::vector<FieldDef> field_defs_ = {};
    std::string table_name_ = {};
};

struct CreateIndexStatementData : QueryData {
    void init(int parent_idx);

    std::vector<IndexField> fields_ = {};
    std::string index_name_ = {};
    std::string table_name_ = {};
};

struct InsertStatementData : QueryData {
    void init(int parent_idx);

    std::string table_name_ = {};
    std::vector<std::string> fields_ = {};
    // only one of these should be used per insertStatement.
    std::vector<ExpressionNode*> values_ = {};
    // this is used to support (insert into ... select .. from ..) syntax.
    int select_idx_ = -1;
};

struct DeleteStatementData : QueryData {
    void init(int parent_idx);

    // the first table is always 
    // the table to be deleted from.
};

struct UpdateStatementData : QueryData {
    void init(int parent_idx);

    // the first table is the always 
    // the table to be updated.
    std::vector<std::string> fields_ = {}; 
    std::vector<ExpressionNode*> values_ = {};
};

JoinType token_type_to_join_type (TokenType t);
bool is_corelated_subquery(QueryCTX& ctx, SelectStatementData* query, Catalog* catalog);


#endif // QUERY_DATA_H
