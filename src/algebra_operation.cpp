#pragma once
#include "ast_nodes.cpp"
#include "executor.h"

// algebra operation
void AlgebraOperation::init(int query_idx, AlgebraOperationType type) {
    query_idx_ = query_idx;
    type_ = type;
}

// scan operation
void ScanOperation::init(int query_idx, String table_name, String table_rename) {
    query_idx_ = query_idx;
    type_ = SCAN;
    table_name_ = table_name;
    table_rename_ = table_rename;
}
void ScanOperation::print(int prefix_space_cnt) {
    for(int i = 0; i < prefix_space_cnt; ++i)
        std::cout << " ";
    std::cout << "Scan operation, name: " << table_name_ << " rename: " << table_rename_;
    std::cout << " type: " << (scan_type_ == SEQ_SCAN ? "SEQ_SCAN " : "INDEX_SCAN ");
    if(filter_)
        std::cout << "filter: " << filter_->token_.val_;
    std::cout << "\n";
}

// union operation
void UnionOperation::init(int query_idx, AlgebraOperation* lhs, AlgebraOperation* rhs, bool all) {
    query_idx_ = query_idx;
    type_ = AL_UNION;
    lhs_ = lhs;
    rhs_ = rhs;
    all_ = all;
}
void UnionOperation::print(int prefix_space_cnt) {
    for(int i = 0; i < prefix_space_cnt; ++i)
        std::cout << " ";
    std::cout << "union operation\n"; 
    std::cout << " lhs:\n "; 
    lhs_->print(prefix_space_cnt + 1);
    std::cout << " rhs:\n "; 
    rhs_->print(prefix_space_cnt + 1);
}

// except operation
void ExceptOperation::init(int query_idx, AlgebraOperation* lhs, AlgebraOperation* rhs, bool all) {
    query_idx_ = query_idx;
    type_ = AL_EXCEPT;
    lhs_ = lhs;
    rhs_ = rhs;
    all_ = all;
}

void ExceptOperation::print(int prefix_space_cnt) {
    for(int i = 0; i < prefix_space_cnt; ++i)
        std::cout << " ";
    std::cout << "except operation\n"; 
    lhs_->print(prefix_space_cnt + 1);
    rhs_->print(prefix_space_cnt + 1);
}

// intersect operation
void IntersectOperation::init(int query_idx, AlgebraOperation* lhs, AlgebraOperation* rhs, bool all) {
    query_idx_ = query_idx;
    type_ = AL_INTERSECT;
    lhs_ = lhs;
    rhs_ = rhs;
    all_ = all;
}
void IntersectOperation::print(int prefix_space_cnt) {
    for(int i = 0; i < prefix_space_cnt; ++i)
        std::cout << " ";
    std::cout << "intersect operation\n"; 
    lhs_->print(prefix_space_cnt + 1);
    rhs_->print(prefix_space_cnt + 1);
}

// product operation
void ProductOperation::init(int query_idx, AlgebraOperation* lhs, AlgebraOperation* rhs) {
    query_idx_ = query_idx;
    type_ = PRODUCT;
    lhs_ = lhs;
    rhs_ = rhs;
}

void ProductOperation::print(int prefix_space_cnt) {
    for(int i = 0; i < prefix_space_cnt; ++i)
        std::cout << " ";
    std::cout << "product operation\n"; 
    lhs_->print(prefix_space_cnt + 1);
    rhs_->print(prefix_space_cnt + 1);
}

// join operation
void JoinOperation::init(int query_idx, AlgebraOperation* lhs,
        AlgebraOperation* rhs,
        ExpressionNode* filter,
        JoinType type, JoinAlgorithm join_algo) {
    query_idx_ = query_idx;
    type_ = JOIN;
    lhs_ = lhs;
    rhs_ = rhs;
    filter_ = filter;
    join_type_ = type;
    join_algo_ = join_algo;
}

void JoinOperation::print(int prefix_space_cnt) {
    for(int i = 0; i < prefix_space_cnt; ++i)
        std::cout << " ";
    std::cout << "join operation: "; 
    std::cout << (join_algo_ == NESTED_LOOP_JOIN ? "NESTED_LOOP_JOIN" : "HASH_JOIN") << "\n";
    lhs_->print(prefix_space_cnt + 1);
    rhs_->print(prefix_space_cnt + 1);
}

// insertion operation
void InsertionOperation::init(int query_idx) {
    query_idx_ = query_idx;
    type_ = INSERTION;
}

void InsertionOperation::print(int prefix_space_cnt) {
    for(int i = 0; i < prefix_space_cnt; ++i)
        std::cout << " ";
    std::cout << "insertion operation\n"; 
}
// deletion operation
void DeletionOperation::init(AlgebraOperation* child, int query_idx) {
    query_idx_ = query_idx;
    type_ = DELETION;
    child_ = child;
}

void DeletionOperation::print(int prefix_space_cnt) {
    for(int i = 0; i < prefix_space_cnt; ++i)
        std::cout << " ";
    std::cout << "deletion operation\n"; 
}
// update operation
void UpdateOperation::init(AlgebraOperation* child, int query_idx) {
    query_idx_ = query_idx;
    type_ = UPDATE;
    child_ = child;
}

void UpdateOperation::print(int prefix_space_cnt) {
    for(int i = 0; i < prefix_space_cnt; ++i)
        std::cout << " ";
    std::cout << "update operation\n"; 
}

// filter operation
void FilterOperation::init(int query_idx, AlgebraOperation* child, ExpressionNode* filter)
    /*
        ,Vector<ExpressionNode*>& fields, 
        Vector<String>& field_names) */
{
    query_idx_ = query_idx;
    type_ = FILTER;
    child_ = child;
    filter_ = filter;
    /*
    fields_ = fields;
    field_names_ = field_names;
    */
}

void FilterOperation::print(int prefix_space_cnt) {
    for(int i = 0; i < prefix_space_cnt; ++i)
        std::cout << " ";
    std::cout << "filter operation "; 
    if(child_)
        child_->print(prefix_space_cnt + 1);
}

// aggregation operation
void AggregationOperation::init(int query_idx, AlgebraOperation* child, Vector<AggregateFuncNode*> aggregates,
        Vector<ASTNode*> group_by){
    query_idx_ = query_idx;
    type_  = AGGREGATION;
    child_ = child;
    aggregates_ = aggregates;
    group_by_ = group_by;
}

void AggregationOperation::print(int prefix_space_cnt) {
    for(int i = 0; i < prefix_space_cnt; ++i)
        std::cout << " ";
    std::cout << "agg operation\n"; 
    if(child_)
        child_->print(prefix_space_cnt + 1);
}

// Projection operation
void ProjectionOperation::init(int query_idx, AlgebraOperation* child, Vector<ExpressionNode*> fields) {
    query_idx_ = query_idx;
    type_ = PROJECTION;
    child_ = child;
    fields_ = fields;
}

void ProjectionOperation::print(int prefix_space_cnt) {
    for(int i = 0; i < prefix_space_cnt; ++i)
        std::cout << " ";
    std::cout << "projection operation\n"; 
    if(child_)
        child_->print(prefix_space_cnt + 1);
}

// sort operation
void SortOperation::init(int query_idx, AlgebraOperation* child, Vector<int> order_by_list) {
    query_idx_ = query_idx;
    type_ = SORT;
    child_ = child;
    order_by_list_ = order_by_list;
}

void SortOperation::print(int prefix_space_cnt) {
    for(int i = 0; i < prefix_space_cnt; ++i)
        std::cout << " ";
    std::cout << "sort operation\n"; 
    if(child_)
        child_->print(prefix_space_cnt + 1);
}

