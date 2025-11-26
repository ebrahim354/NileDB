#pragma once
#include "ast_nodes.cpp"

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
                if(table != query->table_names_[k] && k == query->table_names_[k].size() - 1)
                    return true;
                if(table != query->table_names_[k]) 
                    continue;
                table = query->tables_[k];
                break;
            }
            TableSchema* schema = catalog->getTableSchema(table);
            if(!schema) return true;
            if(!schema->isValidCol(cur_field)){
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


void QueryData::init(QueryType type, int parent_idx) {
    type_ = type;
    parent_idx_ = parent_idx;
}

void SelectStatementData::init (int parent_idx) {
    type_ = SELECT_DATA;
    parent_idx_ = parent_idx;
}

void Intersect::init(int parent_idx, QueryData* lhs, Intersect* rhs, bool all) {
    type_ = INTERSECT;
    parent_idx_ = parent_idx;
    cur_ = lhs;
    next_ = rhs;
    all_ = all;
}

void UnionOrExcept::init(QueryType type, int parent_idx, QueryData* lhs, UnionOrExcept* rhs, bool all){
    type_ = type;
    parent_idx_ = parent_idx;
    cur_ = lhs;
    next_ = rhs;
    all_ = all;
}

void CreateTableStatementData::init(int parent_idx) {
    type_ = CREATE_TABLE_DATA;
    parent_idx_ = parent_idx;
}

void CreateIndexStatementData::init(int parent_idx) {
    type_ = CREATE_INDEX_DATA;
    parent_idx_ = parent_idx;
}

void InsertStatementData::init(int parent_idx) {
    type_ = INSERT_DATA;
    parent_idx_ = parent_idx;
}

void DeleteStatementData::init(int parent_idx) {
    type_ = DELETE_DATA;
    parent_idx_ = parent_idx;
}

void UpdateStatementData::init(int parent_idx) {
    type_ = UPDATE_DATA;
    parent_idx_ = parent_idx;
}

