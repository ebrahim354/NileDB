#pragma once
#include "ast_nodes.cpp"
#include "executor.h"

// algebra operation
void AlgebraOperation::init(AlgebraOperationType type) {
    type_ = type;
}

// scan operation
void ScanOperation::init(std::string table_name, std::string table_rename) {
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
void UnionOperation::init(AlgebraOperation* lhs, AlgebraOperation* rhs, bool all) {
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
void ExceptOperation::init(AlgebraOperation* lhs, AlgebraOperation* rhs, bool all) {
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
void IntersectOperation::init(AlgebraOperation* lhs, AlgebraOperation* rhs, bool all) {
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
void ProductOperation::init(AlgebraOperation* lhs, AlgebraOperation* rhs) {
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
void JoinOperation::init(AlgebraOperation* lhs,
        AlgebraOperation* rhs,
        ExpressionNode* filter,
        JoinType type, JoinAlgorithm join_algo) {
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
void InsertionOperation::init() {
    type_ = INSERTION;
}

void InsertionOperation::print(int prefix_space_cnt) {
    for(int i = 0; i < prefix_space_cnt; ++i)
        std::cout << " ";
    std::cout << "insertion operation\n"; 
}

// filter operation
void FilterOperation::init(AlgebraOperation* child, ExpressionNode* filter, 
        std::vector<ExpressionNode*>& fields, 
        std::vector<std::string>& field_names) 
{
    type_ = FILTER;
    child_ = child;
    filter_ = filter;
    fields_ = fields;
    field_names_ = field_names;
}

void FilterOperation::print(int prefix_space_cnt) {
    for(int i = 0; i < prefix_space_cnt; ++i)
        std::cout << " ";
    std::cout << "filter operation "; 
    if(child_)
        child_->print(prefix_space_cnt + 1);
}

// aggregation operation
void AggregationOperation::init(AlgebraOperation* child, std::vector<AggregateFuncNode*> aggregates,
        std::vector<ASTNode*> group_by){
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
void ProjectionOperation::init(AlgebraOperation* child, std::vector<ExpressionNode*> fields) {
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
void SortOperation::init(AlgebraOperation* child, std::vector<int> order_by_list) {
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

