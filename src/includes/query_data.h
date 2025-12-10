#ifndef QUERY_DATA_H
#define QUERY_DATA_H

#include "catalog.h"
#include "ast_nodes.h"

class Catalog;


enum QueryType {
    SELECT_DATA = 0,
    CREATE_TABLE_DATA,
    CREATE_INDEX_DATA,
    DROP_TABLE_DATA,
    DROP_INDEX_DATA,
    INSERT_DATA,
    DELETE_DATA,
    UPDATE_DATA,
    // set operations.
    UNION,
    INTERSECT,
    EXCEPT
};



struct QueryData {
    QueryData(Arena* arena, QueryType type, int parent_idx);

    QueryType type_;
    int idx_ = -1;          // every query must have an id starting from 0 even the top level query.
    int parent_idx_ = -1;   // -1 means this query is the top level query.

    Vector<String> tables_      = {};
    Vector<String> table_names_ = {};
    Vector<JoinedTablesData> joined_tables_ = {};
    ExpressionNode* where_ = nullptr;

    // a mark for subqueries to indicate if they are corelated to their parent or not.
    // the top level query is always not corelated and has a parent_query_idx of -1.
    // TODO: if the top level query is corelated that is an error, indicate that.
    bool is_corelated_ = false; 
};

// SQL statement data wrappers.
struct SelectStatementData : QueryData {
    SelectStatementData(Arena* arena, int parent_idx);

    Vector<String> field_names_ = {};
    Vector<ExpressionNode*> fields_ = {};
    Vector<AggregateFuncNode*> aggregates_;  
    Vector<int> order_by_list_ = {};
    Vector<ASTNode*> group_by_ = {}; 
    bool has_star_ = false;
    ExpressionNode* having_ = nullptr;
    bool distinct_ = false;

};

struct Intersect : QueryData {
    Intersect(Arena* arena, int parent_idx, QueryData* lhs, Intersect* rhs, bool all);

    QueryData* cur_ = nullptr;
    QueryData* next_ = nullptr;
    bool all_ = false;
};

struct UnionOrExcept : QueryData {
    UnionOrExcept(Arena* arena, QueryType type, int parent_idx, QueryData* lhs, UnionOrExcept* rhs, bool all);

    QueryData* cur_ = nullptr;
    QueryData* next_ = nullptr; 
    bool all_ = false;
};

struct CreateTableStatementData : QueryData {
    CreateTableStatementData(Arena* arena, int parent_idx);

    Vector<FieldDef> field_defs_ = {};
    String table_name_ = {};
};

struct CreateIndexStatementData : QueryData {
    CreateIndexStatementData(Arena* arena, int parent_idx);

    Vector<IndexField> fields_ = {};
    String index_name_ = {};
    String table_name_ = {};
};

struct DropTableStatementData : QueryData {
    DropTableStatementData(Arena* arena, int parent_idx);

    String table_name_ = {};
};

struct DropIndexStatementData : QueryData {
    DropIndexStatementData(Arena* arena, int parent_idx);

    String index_name_ = {};
};

struct InsertStatementData : QueryData {
    InsertStatementData(Arena* arena, int parent_idx);

    String table_name_ = {};
    Vector<String> fields_ = {};
    // only one of these should be used per insertStatement.
    Vector<ExpressionNode*> values_ = {};
    // this is used to support (insert into ... select .. from ..) syntax.
    int select_idx_ = -1;
};

struct DeleteStatementData : QueryData {
    DeleteStatementData(Arena* arena, int parent_idx);

};

struct UpdateStatementData : QueryData {
    UpdateStatementData(Arena* arena, int parent_idx);

    Vector<String> fields_ = {}; 
    Vector<ExpressionNode*> values_ = {};
};

JoinType token_type_to_join_type (TokenType t);
bool is_corelated_subquery(QueryCTX& ctx, SelectStatementData* query, Catalog* catalog);


#endif // QUERY_DATA_H
