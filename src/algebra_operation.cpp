#pragma once
#include "ast_nodes.cpp"
#include "executor.h"

AlgebraOperation::AlgebraOperation(AlgebraOperationType type, int query_idx):
    type_(type), query_idx_(query_idx)
{}

ScanOperation::ScanOperation(Arena* arena, int query_idx, String& table_name, String& table_rename):
    AlgebraOperation(SCAN, query_idx),
    table_name_(table_name, arena), table_rename_(table_rename, arena),
    filters_(arena), index_filters_(arena)
{}
void ScanOperation::print(int prefix_space_cnt) {
    for(int i = 0; i < prefix_space_cnt; ++i)
        std::cout << " ";
    std::cout << "Scan operation, name: " << table_name_ << " rename: " << table_rename_;
    std::cout << " type: " << (scan_type_ == SEQ_SCAN ? "SEQ_SCAN \n" : "INDEX_SCAN \n");
    if(filters_.size()){
        for(int j = 0; j < filters_.size(); ++j){
            std::cout << "Scan filter number (" << (int)(j+1) << ") "; 
            std::cout << "\n";
        }
    }
    std::cout << "\n";
}

UnionOperation::UnionOperation(Arena* arena, int query_idx, AlgebraOperation* lhs, AlgebraOperation* rhs, bool all):
    AlgebraOperation(AL_UNION, query_idx),
    lhs_(lhs), rhs_(rhs), all_(all)
{}
void UnionOperation::print(int prefix_space_cnt) {
    for(int i = 0; i < prefix_space_cnt; ++i)
        std::cout << " ";
    std::cout << "union operation\n"; 
    std::cout << " lhs:\n "; 
    lhs_->print(prefix_space_cnt + 1);
    std::cout << " rhs:\n "; 
    rhs_->print(prefix_space_cnt + 1);
}

ExceptOperation::ExceptOperation(Arena* arena, int query_idx, AlgebraOperation* lhs, AlgebraOperation* rhs, bool all):
    AlgebraOperation(AL_EXCEPT, query_idx),
    lhs_(lhs), rhs_(rhs), all_(all)
{}
void ExceptOperation::print(int prefix_space_cnt) {
    for(int i = 0; i < prefix_space_cnt; ++i)
        std::cout << " ";
    std::cout << "except operation\n"; 
    lhs_->print(prefix_space_cnt + 1);
    rhs_->print(prefix_space_cnt + 1);
}

IntersectOperation::IntersectOperation(Arena* arena, int query_idx, AlgebraOperation* lhs, AlgebraOperation* rhs, bool all):
    AlgebraOperation(AL_INTERSECT, query_idx),
    lhs_(lhs), rhs_(rhs), all_(all)
{}
void IntersectOperation::print(int prefix_space_cnt) {
    for(int i = 0; i < prefix_space_cnt; ++i)
        std::cout << " ";
    std::cout << "intersect operation\n"; 
    lhs_->print(prefix_space_cnt + 1);
    rhs_->print(prefix_space_cnt + 1);
}

ProductOperation::ProductOperation(Arena* arena, int query_idx, AlgebraOperation* lhs, AlgebraOperation* rhs):
    AlgebraOperation(PRODUCT, query_idx),
    lhs_(lhs), rhs_(rhs)
{}
void ProductOperation::print(int prefix_space_cnt) {
    for(int i = 0; i < prefix_space_cnt; ++i)
        std::cout << " ";
    std::cout << "product operation\n"; 
    lhs_->print(prefix_space_cnt + 1);
    rhs_->print(prefix_space_cnt + 1);
}

JoinOperation::JoinOperation(Arena* arena, int query_idx, AlgebraOperation* lhs, AlgebraOperation* rhs,
        ExpressionNode* filter,
        JoinType type, JoinAlgorithm join_algo):
    AlgebraOperation(JOIN, query_idx),
    lhs_(lhs), rhs_(rhs), filter_(filter), join_type_(type), join_algo_(join_algo)
{}
void JoinOperation::print(int prefix_space_cnt) {
    for(int i = 0; i < prefix_space_cnt; ++i)
        std::cout << " ";
    std::cout << "join operation: "; 
    std::cout << (join_algo_ == NESTED_LOOP_JOIN ? "NESTED_LOOP_JOIN" : "HASH_JOIN") << "\n";
    lhs_->print(prefix_space_cnt + 1);
    rhs_->print(prefix_space_cnt + 1);
}

InsertionOperation::InsertionOperation(Arena* arena, int query_idx):
    AlgebraOperation(INSERTION, query_idx)
{}
void InsertionOperation::print(int prefix_space_cnt) {
    for(int i = 0; i < prefix_space_cnt; ++i)
        std::cout << " ";
    std::cout << "insertion operation\n"; 
}


DeletionOperation::DeletionOperation(Arena* arena, AlgebraOperation* child, int query_idx):
    AlgebraOperation(DELETION, query_idx),
    child_(child)
{}
void DeletionOperation::print(int prefix_space_cnt) {
    for(int i = 0; i < prefix_space_cnt; ++i)
        std::cout << " ";
    std::cout << "deletion operation\n"; 
}

UpdateOperation::UpdateOperation(Arena* arena, AlgebraOperation* child, int query_idx):
    AlgebraOperation(UPDATE, query_idx),
    child_(child)
{}
void UpdateOperation::print(int prefix_space_cnt) {
    for(int i = 0; i < prefix_space_cnt; ++i)
        std::cout << " ";
    std::cout << "update operation\n"; 
}

FilterOperation::FilterOperation(Arena* arena, int query_idx, AlgebraOperation* child, ExpressionNode* filter):
    AlgebraOperation(FILTER, query_idx),
    child_(child), filter_(filter)
{}
void FilterOperation::print(int prefix_space_cnt) {
    for(int i = 0; i < prefix_space_cnt; ++i)
        std::cout << " ";
    std::cout << "filter operation "; 
    if(child_)
        child_->print(prefix_space_cnt + 1);
}

AggregationOperation::AggregationOperation(
        Arena* arena, int query_idx, AlgebraOperation* child,
        Vector<AggregateFuncNode*>& aggregates, Vector<ASTNode*>& group_by):

    AlgebraOperation(AGGREGATION, query_idx),
    child_(child), aggregates_(aggregates, arena), group_by_(group_by, arena)
{}
void AggregationOperation::print(int prefix_space_cnt) {
    for(int i = 0; i < prefix_space_cnt; ++i)
        std::cout << " ";
    std::cout << "agg operation\n"; 
    if(child_)
        child_->print(prefix_space_cnt + 1);
}

ProjectionOperation::ProjectionOperation(Arena* arena, int query_idx,
        AlgebraOperation* child, Vector<ExpressionNode*>& fields):
    AlgebraOperation(PROJECTION, query_idx),
    child_(child), fields_(fields, arena)
{}
void ProjectionOperation::print(int prefix_space_cnt) {
    for(int i = 0; i < prefix_space_cnt; ++i)
        std::cout << " ";
    std::cout << "projection operation\n"; 
    if(child_)
        child_->print(prefix_space_cnt + 1);
}

SortOperation::SortOperation(Arena* arena, int query_idx, AlgebraOperation* child, Vector<int>& order_by_list):
    AlgebraOperation(SORT, query_idx),
    child_(child), order_by_list_(order_by_list, arena)
{}
void SortOperation::print(int prefix_space_cnt) {
    for(int i = 0; i < prefix_space_cnt; ++i)
        std::cout << " ";
    std::cout << "sort operation\n"; 
    if(child_)
        child_->print(prefix_space_cnt + 1);
}

