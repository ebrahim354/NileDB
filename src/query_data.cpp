#pragma once

#include "ast_nodes.cpp"


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
    /*
    QueryData(QueryType type, int parent_idx): type_(type), parent_idx_(parent_idx){}
    virtual ~QueryData(){};
    */

    void init(QueryType type, int parent_idx) {
        type_ = type;
        parent_idx_ = parent_idx;
    }

    QueryType type_;
    int idx_ = -1;          // every query must have an id starting from 0 even the top level query.
    int parent_idx_ = -1;   // -1 means this query is the top level query.

    // a mark for subqueries to indicate if they are corelated to their parent or not.
    // the top level query is always not corelated and has a parent_query_idx of -1.
    // TODO: if the top level query is corelated that is an error, indicate that.
    bool is_corelated_ = false; 
};

// SQL statement data wrappers.
struct SelectStatementData : QueryData {

    /*
    SelectStatementData(int parent_idx): QueryData(SELECT_DATA, parent_idx)
    {}
    ~SelectStatementData() {
        for(int i = 0; i < fields_.size(); ++i)
            delete fields_[i];
        for(int i = 0; i < aggregates_.size(); ++i)
            delete aggregates_[i];
        delete where_;
        delete having_;
        for(int i = 0; i < group_by_.size(); ++i)
            delete group_by_[i];
    }*/
    void init (int parent_idx) {
        type_ = SELECT_DATA;
        parent_idx_ = parent_idx;
    }

    std::vector<ExpressionNode*> fields_ = {};
    std::vector<std::string> field_names_ = {};
    std::vector<std::string> table_names_ = {};
    std::vector<std::string> tables_ = {};
    std::vector<JoinedTablesData> joined_tables_ = {};
    std::vector<AggregateFuncNode*> aggregates_;  
    std::vector<int> order_by_list_ = {};
    std::vector<ASTNode*> group_by_ = {}; 
    bool has_star_ = false;
    ExpressionNode* where_ = nullptr;
    ExpressionNode* having_ = nullptr;
    bool distinct_ = false;

};

struct Intersect : QueryData {
    /*
    Intersect(int parent_idx, QueryData* lhs, Intersect* rhs, bool all): 
        QueryData(INTERSECT, parent_idx), cur_(lhs), next_(rhs), all_(all){}
    ~Intersect() {
        // set operations don't own the queries so we can't do this:
        // the ownership of queries belongs to the query ctx.
        // delete cur_;
        // delete next_;
    }*/
    void init(int parent_idx, QueryData* lhs, Intersect* rhs, bool all) {
        type_ = INTERSECT;
        parent_idx_ = parent_idx;
        cur_ = lhs;
        next_ = rhs;
        all_ = all;
    }

    QueryData* cur_ = nullptr;
    QueryData* next_ = nullptr;
    bool all_ = false;
};

struct UnionOrExcept : QueryData {
    /*
    UnionOrExcept(QueryType type, int parent_idx, QueryData* lhs, UnionOrExcept* rhs, bool all): 
        QueryData(type, parent_idx), cur_(lhs), next_(rhs), all_(all)
    {}
    ~UnionOrExcept() {}
    */
    void init(QueryType type, int parent_idx, QueryData* lhs, UnionOrExcept* rhs, bool all){
        type_ = type;
        parent_idx_ = parent_idx;
        cur_ = lhs;
        next_ = rhs;
        all_ = all;
    }
    QueryData* cur_ = nullptr;
    QueryData* next_ = nullptr; 
    bool all_ = false;
};




struct CreateTableStatementData : QueryData {

    /*
    CreateTableStatementData(int parent_idx): QueryData(CREATE_TABLE_DATA, parent_idx){}
    ~CreateTableStatementData() {}
    */
    void init(int parent_idx) {
        type_ = CREATE_TABLE_DATA;
        parent_idx_ = parent_idx;
    }


    std::vector<FieldDef> field_defs_ = {};
    std::string table_name_ = {};
};

struct CreateIndexStatementData : QueryData {

    /*
    CreateIndexStatementData(int parent_idx): QueryData(CREATE_INDEX_DATA, parent_idx){}
    ~CreateIndexStatementData() {}
    */

    void init(int parent_idx) {
        type_ = CREATE_INDEX_DATA;
        parent_idx_ = parent_idx;
    }

    std::vector<IndexField> fields_ = {};
    std::string index_name_ = {};
    std::string table_name_ = {};
};

struct InsertStatementData : QueryData {

    /*
    InsertStatementData(int parent_idx): QueryData(INSERT_DATA, parent_idx){}
    ~InsertStatementData() {
        for(int i = 0; i < values_.size();++i)
            delete values_[i];
    }*/

    void init(int parent_idx) {
        type_ = INSERT_DATA;
        parent_idx_ = parent_idx;
    }

    std::string table_name_ = {};
    std::vector<std::string> fields_ = {};
    // only one of these should be used per insertStatement.
    std::vector<ExpressionNode*> values_ = {};
    // this is used to support (insert into ... select .. from ..) syntax.
    int select_idx_ = -1;
};

struct DeleteStatementData : QueryData {

    /*
    DeleteStatementData(int parent_idx): QueryData(DELETE_DATA, parent_idx){}
    ~DeleteStatementData() {
        delete where_;
    }*/

    void init(int parent_idx) {
        type_ = DELETE_DATA;
        parent_idx_ = parent_idx;
    }

    std::string table_name_ = {};
    ExpressionNode* where_ = nullptr;
};

struct UpdateStatementData : QueryData {

    /*
    UpdateStatementData(int parent_idx): QueryData(UPDATE_DATA, parent_idx){}
    ~UpdateStatementData() {
        delete value_;
        delete where_;
    }*/

    void init(int parent_idx) {
        type_ = UPDATE_DATA;
        parent_idx_ = parent_idx;
    }


    std::string table_name_ = {};
    std::string  field_ = {}; 
    ExpressionNode* value_ = nullptr;
    ExpressionNode* where_ = nullptr;
};


JoinType token_type_to_join_type (TokenType t) {
    switch(t){
        case TokenType::LEFT:
            return LEFT_JOIN;
        case TokenType::RIGHT:
            return RIGHT_JOIN;
        case TokenType::FULL:
            return FULL_JOIN;
        default:
            return INNER_JOIN;
    }
    return INNER_JOIN;
}


bool is_corelated_subquery(QueryCTX& ctx, SelectStatementData* query, Catalog* catalog) {
    if(!query || !catalog) assert(0 && "invalid input");
    std::vector<std::string> fields;
    for(int i = 0; i < query->fields_.size(); ++i){
        accessed_fields(query->fields_[i],fields, true);
    }
    for(int i = 0; i < query->aggregates_.size(); ++i){
        accessed_fields(query->aggregates_[i], fields, true);
    }
    accessed_fields(query->where_, fields, true);
    accessed_fields(query->having_, fields, true);
    for(int i = 0; i < query->group_by_.size(); ++i){
        accessed_fields(query->group_by_[i], fields, true);
    }
    for(int i = 0; i < fields.size(); ++i){
        auto table_field = split_scoped_field(fields[i]);
        std::string table = table_field.first;
        std::string cur_field   = table_field.second;
        if(table.size()) {  // the field is scoped.
            for(int k = 0; k < query->table_names_.size(); ++k){
                if(table != query->table_names_[k]) continue;
                table = query->tables_[k];
            }
            TableSchema* schema = catalog->getTableSchema(table);
            if(!schema) return true;
            if(!schema->isValidCol(fields[i])){
                return true; // the table does not contain this column => corelated.
            }

        } else { // the field is not scoped.
            std::vector<std::string> possible_tables = catalog->getTablesByField(cur_field);
            bool table_matched = false;
            for(int j = 0; j < possible_tables.size(); ++j){
                if(std::find(query->table_names_.begin(), query->table_names_.end(), possible_tables[j]) 
                        != query->table_names_.end()
                 ) {
                    table_matched = true;
                    break;
                }
            }
            if(!table_matched) return true; // no table matched the field in this scope => corelated.
        }
    }
    return false;
}
