#pragma once
#include "ast_nodes.cpp"
#include "expression.h"

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
    Vector<ASTNode*> fields;
    for(int i = 0; i < query->fields_.size(); ++i){
        accessed_fields(query->fields_[i], fields, true);
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
        String table = table_field.first;
        String cur_field   = table_field.second;
        if(table.size()) {  // the field is scoped.
            for(int k = 0; k < query->table_names_.size(); ++k){
                if(table != query->table_names_[k] && k == query->table_names_.size() - 1)
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
            Vector<String> possible_tables = catalog->getTablesByField(cur_field);
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


QueryData::QueryData(Arena* arena, QueryType type, int parent_idx):
    type_(type), parent_idx_(parent_idx), 
    tables_(arena), table_names_(arena), joined_tables_(arena)
{}

SelectStatementData::SelectStatementData (Arena* arena, int parent_idx):
    QueryData(arena, SELECT_DATA, parent_idx),
    field_names_(arena), fields_(arena), aggregates_(arena),
    order_by_list_(arena), group_by_(arena)
{}

Intersect::Intersect(Arena* arena, int parent_idx, QueryData* lhs, Intersect* rhs, bool all):
    QueryData(arena, INTERSECT, parent_idx),
    cur_(lhs), next_(rhs), all_(all)
{}

UnionOrExcept::UnionOrExcept(Arena* arena, QueryType type, int parent_idx, QueryData* lhs, UnionOrExcept* rhs, bool all):
    QueryData(arena, type, parent_idx),
    cur_(lhs), next_(rhs), all_(all)
{}

CreateTableStatementData::CreateTableStatementData(Arena* arena, int parent_idx):
    QueryData(arena, CREATE_TABLE_DATA, parent_idx),
    field_defs_(arena), table_name_(arena)
{}

CreateIndexStatementData::CreateIndexStatementData(Arena* arena, int parent_idx):
    QueryData(arena, CREATE_INDEX_DATA, parent_idx),
    fields_(arena), index_name_(arena), table_name_(arena)
{}

DropTableStatementData::DropTableStatementData(Arena* arena, int parent_idx):
    QueryData(arena, DROP_TABLE_DATA, parent_idx),
    table_name_(arena)
{}

DropIndexStatementData::DropIndexStatementData(Arena* arena, int parent_idx):
    QueryData(arena, DROP_INDEX_DATA, parent_idx),
    index_name_(arena)
{}

InsertStatementData::InsertStatementData(Arena* arena, int parent_idx):
    QueryData(arena, INSERT_DATA, parent_idx),
    table_name_(arena), fields_(arena), values_(arena)
{}

DeleteStatementData::DeleteStatementData(Arena* arena, int parent_idx):
    QueryData(arena, DELETE_DATA, parent_idx)
{}

UpdateStatementData::UpdateStatementData(Arena* arena, int parent_idx):
    QueryData(arena, UPDATE_DATA, parent_idx),
    fields_(arena), values_(arena)
{}

