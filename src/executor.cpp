#pragma once

#include "catalog.cpp"
#include "expression.h"
#include "string"

struct QueryCTX;

std::string exec_type_to_string(ExecutorType t) {
    switch(t){
        case SEQUENTIAL_SCAN_EXECUTOR:
            return "SEQUENTIAL SCAN";
        case INDEX_SCAN_EXECUTOR:
            return "SEQUENTIAL SCAN";
        case INSERTION_EXECUTOR:
            return "INSERTION";
        case FILTER_EXECUTOR:
            return "FILTER";
        case AGGREGATION_EXECUTOR: 
            return "AGGREGATION";
        case PROJECTION_EXECUTOR: 
            return "PROJECTION";
        case SORT_EXECUTOR:
            return "SORT";
        case DISTINCT_EXECUTOR:
            return "DISTINCT";
        case PRODUCT_EXECUTOR: 
            return "PRODUCT";
        case HASH_JOIN_EXECUTOR: 
            return "HASH JOIN";
        case NESTED_LOOP_JOIN_EXECUTOR: 
            return "NESTED LOOP JOIN";
        case UNION_EXECUTOR:
            return "UNION";
        case EXCEPT_EXECUTOR:
            return "EXCEPT";
        case INTERSECT_EXECUTOR:
            return "INTERSECT";
        default:
            return "INVALID EXECUTOR";
    }
}

/*
Executor::Executor(ExecutorType type, TableSchema* output_schema, QueryCTX* ctx,int query_idx, int parent_query_idx, Executor* child): 
    type_(type), output_schema_(output_schema), ctx_(ctx), 
    query_idx_(query_idx), parent_query_idx_(parent_query_idx), child_executor_(child)
{}

Executor::~Executor(){};*/
void Executor::construct(QueryCTX* ctx, AlgebraOperation* plan_node, TableSchema* output_schema,
        Executor* child_executor,
        ExecutorType type) {
    ctx_ = ctx; 
    plan_node_ = plan_node;
    output_schema_ = output_schema;
    type_ = type;
    assert(query_idx_ < ctx->queries_call_stack_.size() && plan_node != nullptr);
    query_idx_ = plan_node->query_idx_;
    parent_query_idx_ = ctx->queries_call_stack_[query_idx_]->parent_idx_;
    child_executor_ = child_executor;
}

/*
NestedLoopJoinExecutor::NestedLoopJoinExecutor(
        TableSchema* output_schema,
        QueryCTX* ctx, int query_idx, int parent_query_idx,
        Executor* lhs, Executor* rhs,
        ExpressionNode* filter, JoinType type)
    : Executor(
            NESTED_LOOP_JOIN_EXECUTOR, output_schema, ctx,
            query_idx, parent_query_idx, lhs
            ), 
    left_child_(lhs), right_child_(rhs), 
    filter_(filter), join_type_(type)
{}
NestedLoopJoinExecutor::~NestedLoopJoinExecutor()
{
    delete left_child_;  
    delete right_child_;
    delete output_schema_;
}*/

void NestedLoopJoinExecutor::construct(QueryCTX* ctx, AlgebraOperation* plan_node, Executor* lhs, Executor* rhs) {
    assert(plan_node != nullptr && plan_node->type_ == JOIN);
    type_ = NESTED_LOOP_JOIN_EXECUTOR;
    ctx_ = ctx; 
    plan_node_ = plan_node;
    //output_schema_ = output_schema;
    query_idx_ = plan_node->query_idx_;
    assert(query_idx_ < ctx->queries_call_stack_.size());
    parent_query_idx_ = ctx->queries_call_stack_[query_idx_]->parent_idx_;

    left_child_  = lhs;
    right_child_ = rhs;

    filter_ = ((JoinOperation*)plan_node_)->filter_;
    join_type_ = ((JoinOperation*)plan_node_)->join_type_;

    std::vector<Column> lhs_columns = lhs->output_schema_->getColumns();
    std::vector<Column> rhs_columns = rhs->output_schema_->getColumns();
    for(int i = 0; i < rhs_columns.size(); i++)
        lhs_columns.push_back(rhs_columns[i]);

    output_schema_ = new TableSchema("TMP_JOIN_TABLE", nullptr, lhs_columns, true); // TODO: fix leak.
}

void NestedLoopJoinExecutor::init() {
    error_status_ = 0;
    finished_ = 0;
    if(!left_child_ || !right_child_){
        error_status_ = true;
        return;
    }
    left_child_->init();
    right_child_->init();
    error_status_ = left_child_->error_status_ || right_child_->error_status_;
    finished_ = left_child_->finished_;
    output_.resize(left_child_->output_schema_->getColumns().size() + right_child_->output_schema_->getColumns().size()); 
    if(!finished_){
        left_output_ = left_child_->next();
        if(left_output_.size() == 0 && left_child_->finished_) finished_ = true;
    }
}

std::vector<Value> NestedLoopJoinExecutor::next() {
    if(error_status_ || finished_)  return {};

    while(true){
        std::vector<Value> right_output = right_child_->next();
        if(right_child_->finished_){
            if(!left_output_visited_ && (join_type_ == LEFT_JOIN || join_type_ == FULL_JOIN)){
                left_output_visited_ = true;
                for(int i = 0; i < left_output_.size(); ++i)
                    output_[i] = left_output_[i];
                for(int i = left_output_.size(); i < output_.size(); ++i)
                    output_[i] = Value(NULL_TYPE);
                return output_;
            }
            left_output_ = left_child_->next();
            left_output_visited_ = false;
            if(left_child_->finished_){
                finished_ = true;
                return {};
            }
            right_child_->init();
            right_child_have_reset_ = true;
            right_output = right_child_->next();
        } 
        if(right_output.size() == 0) {
            finished_ = true;
            return {};
        }

        for(int i = 0; i < left_output_.size(); ++i)
            output_[i] = left_output_[i];
        for(int i = 0;i < right_output.size(); ++i)
            output_[i+(output_.size() - right_output.size())] = right_output[i];

        finished_ = left_child_->finished_;
        Value v = evaluate_expression(ctx_, filter_, this);
        if(!v.isNull() && v.getBoolVal() == true){
            left_output_visited_ = true;
            return output_;
        }
        if((join_type_ == RIGHT_JOIN || join_type_ == FULL_JOIN) && !right_child_have_reset_) {
            for(int i = 0; i < left_output_.size(); ++i)
                output_[i] = Value(NULL_TYPE);
            return output_;
        }
        //return output_;
    }
}

/*
ProductExecutor::ProductExecutor(
        TableSchema* output_schema, QueryCTX* ctx, 
        int query_idx, int parent_query_idx, 
        Executor* lhs, Executor* rhs)
    : Executor(
            PRODUCT_EXECUTOR, output_schema, ctx,
            query_idx, parent_query_idx, lhs),
    left_child_(lhs), right_child_(rhs)
{}
ProductExecutor::~ProductExecutor()
{
    delete left_child_;  
    delete right_child_;
    delete output_schema_;
}*/

void ProductExecutor::construct(QueryCTX* ctx, AlgebraOperation* plan_node, Executor* lhs, Executor* rhs) {
    assert(plan_node != nullptr && plan_node->type_ == PRODUCT);
    assert(lhs && rhs);
    type_ = PRODUCT_EXECUTOR;
    ctx_ = ctx; 
    plan_node_ = plan_node;
    //output_schema_ = output_schema;
    query_idx_ = plan_node->query_idx_;
    assert(query_idx_ < ctx->queries_call_stack_.size());
    parent_query_idx_ = ctx->queries_call_stack_[query_idx_]->parent_idx_;
    left_child_  = lhs;
    right_child_ = rhs;

    std::vector<Column> lhs_columns = lhs->output_schema_->getColumns();
    std::vector<Column> rhs_columns = rhs->output_schema_->getColumns();
    for(int i = 0; i < rhs_columns.size(); i++)
        lhs_columns.push_back(rhs_columns[i]);

    output_schema_ =  new TableSchema("TMP_PRODUCT_TABLE", nullptr, lhs_columns, true);// TODO: fix leak
}

void ProductExecutor::init() {
    error_status_ = 0;
    finished_ = 0;
    if(!left_child_ || !right_child_){
        error_status_ = true;
        return;
    }
    left_child_->init();
    right_child_->init();
    error_status_ = left_child_->error_status_ || right_child_->error_status_;
    finished_ = left_child_->finished_;
    output_.resize(left_child_->output_schema_->getColumns().size() + right_child_->output_schema_->getColumns().size()); 
    if(!finished_){
        auto left_output = left_child_->next();
        if(left_output.size() == 0 && left_child_->finished_) finished_ = true;
        for(int i = 0; i < left_output.size(); ++i)
            output_[i] = left_output[i];
    }
}

std::vector<Value> ProductExecutor::next() {
    if(error_status_ || finished_)  return {};
    std::vector<Value> left_output;
    std::vector<Value> right_output = right_child_->next();
    if(right_child_->finished_){
        left_output = left_child_->next();
        if(left_child_->finished_){
            finished_ = true;
            return {};
        }
        right_child_->init();
        right_output = right_child_->next();
    } 
    if(right_output.size() == 0) {
        finished_ = true;
        return {};
    }

    for(int i = 0; i < left_output.size(); ++i)
        output_[i] = left_output[i];
    for(int i = 0;i < right_output.size(); ++i)
        output_[i+(output_.size() - right_output.size())] = right_output[i];

    finished_ = left_child_->finished_;
    return output_;
}

/*
HashJoinExecutor::HashJoinExecutor(
        TableSchema* output_schema, QueryCTX* ctx, 
        int query_idx, int parent_query_idx, 
        Executor* lhs, Executor* rhs, 
        ExpressionNode* filter, JoinType type)
    : Executor(
            HASH_JOIN_EXECUTOR, output_schema, ctx,
            query_idx, parent_query_idx, lhs), 
    left_child_(lhs), right_child_(rhs), 
    filter_(filter), join_type_(type)
{}
HashJoinExecutor::~HashJoinExecutor() {
    delete left_child_;  
    delete right_child_;
    delete output_schema_;
}*/

void HashJoinExecutor::construct(QueryCTX* ctx, AlgebraOperation* plan_node, Executor* lhs, Executor* rhs) {
    assert(plan_node != nullptr && plan_node->type_ == JOIN);
    type_ = HASH_JOIN_EXECUTOR;
    ctx_ = ctx; 
    plan_node_ = plan_node;
    //output_schema_ = output_schema;
    query_idx_ = plan_node->query_idx_;
    assert(query_idx_ < ctx->queries_call_stack_.size());
    parent_query_idx_ = ctx->queries_call_stack_[query_idx_]->parent_idx_;
    left_child_  = lhs;
    right_child_ = rhs;

    filter_ = ((JoinOperation*)plan_node_)->filter_;
    join_type_ = ((JoinOperation*)plan_node_)->join_type_;

    std::vector<Column> lhs_columns = lhs->output_schema_->getColumns();
    std::vector<Column> rhs_columns = rhs->output_schema_->getColumns();
    for(int i = 0; i < rhs_columns.size(); i++)
        lhs_columns.push_back(rhs_columns[i]);

    output_schema_ = new TableSchema("TMP_JOIN_TABLE", nullptr, lhs_columns, true); // TODO: fix leak.
}

void HashJoinExecutor::init() {
    error_status_ = 0;
    finished_ = 0;
    if(!left_child_ || !right_child_){
        error_status_ = true;
        return;
    }
    left_child_->init();
    right_child_->init();
    error_status_ = left_child_->error_status_ || right_child_->error_status_;
    finished_ = left_child_->finished_;
    output_.resize(left_child_->output_schema_->getColumns().size() + right_child_->output_schema_->getColumns().size()); 
    prev_key_ = "";
    duplicated_idx_ = -1;
    hashed_left_child_.clear();
    non_visited_left_keys_.clear();
    right_child_fields_.clear();
    left_child_fields_.clear();
    // find out which attributes to use as keys for the hash table.
    std::vector<std::string> fields;
    accessed_fields(filter_, fields);

    for(int i = 0; i < fields.size(); ++i) {
        std::string lf = fields[i];
        int idx = left_child_->output_schema_->colExist(lf);
        // TODO: fix the case of same field names and different tables for example: t1.a = t2.a
        if(idx != -1) {
            left_child_fields_.push_back(idx);
            continue;
        }
        std::string rf = fields[i];
        idx = right_child_->output_schema_->colExist(rf);

        if(idx != -1) {
            right_child_fields_.push_back(idx);
            continue;
        }
        // invalid field name.
        error_status_ = 1;
        return;
    }
    // should at least have one field on each side or hash join does not make any sense.
    // getting to this point without a key on each side means that hash join was picked wrongly,
    // and a nested loop join was more appropriate.
    assert(left_child_fields_.size() != 0 && right_child_fields_.size() != 0);
    if(left_child_fields_.size() == 0 || right_child_fields_.size() == 0){
        error_status_ = 1;
        return;
    }
    // build the hash table.
    while(!left_child_->finished_ && !left_child_->error_status_){
        std::vector<Value> left_output = left_child_->next();
        if(left_output.size() == 0) continue;
        // build the hash key and assume non-unique keys.
        std::string key = "";
        for(int i = 0; i < left_child_fields_.size(); ++i){
            int idx = left_child_fields_[i]; 
            if(idx >= left_output.size()){
                error_status_ = 1;
                return;
            }
            key += left_output[idx].toString();
        }
        if(hashed_left_child_.count(key)) 
            hashed_left_child_[key].push_back(left_output);
        else {
            hashed_left_child_[key] = {left_output};
            non_visited_left_keys_.insert(key);
        }
    }
}

// TODO: implement merg and nested loop joines, 
// hash join is not good for cases of none equality conditions, and full outer joins.
std::vector<Value> HashJoinExecutor::next() {
    if(error_status_ || finished_)  return {};

    while(!right_child_->finished_){
        if(duplicated_idx_ != -1){
            duplicated_idx_++;
            // no need to change the right output.
            for(int i = 0; i < hashed_left_child_[prev_key_][duplicated_idx_].size(); ++i){
                output_[i] = hashed_left_child_[prev_key_][duplicated_idx_][i];
            }
            if(duplicated_idx_+1 >= hashed_left_child_[prev_key_].size())
                duplicated_idx_ = -1;
            //finished_ = right_child_->finished_ && duplicated_idx_ == -1;
            Value v = evaluate_expression(ctx_, filter_, this);
            if(!v.isNull() && v.getBoolVal() == true)
                return output_;
            continue;
        }

        std::vector<Value> right_output;
        while(true){
            right_output = right_child_->next();
            if(right_output.size() == 0) {
                //finished_ = true;
                //return {};
                break;
            }
            // build the hash key. 
            std::string key = "";
            for(int i = 0; i < right_child_fields_.size(); ++i){
                int idx = right_child_fields_[i]; 
                if(idx >= right_output.size()){
                    error_status_ = 1;
                    return {};
                }
                key += right_output[idx].toString();
            }
            if(!hashed_left_child_.count(key) && (join_type_ == RIGHT_JOIN || join_type_ == FULL_JOIN)){
                int start = output_.size()-right_output.size();
                for(int i = 0; i < output_.size(); ++i){
                    output_[i] = (i < start) ? Value(NULL_TYPE) : right_output[i-start];
                }
                return output_;
            } else if(!hashed_left_child_.count(key)){
                continue;
            }
            // this key is now visited so no need to count it for left joins.
            if(non_visited_left_keys_.count(key)) non_visited_left_keys_.erase(key);

            for(int i = 0;i < right_output.size(); ++i)
                output_[i+(output_.size() - right_output.size())] = right_output[i];

            int duplications = hashed_left_child_[key].size();
            for(int i = 0; i < hashed_left_child_[key][0].size(); ++i){
                output_[i] = hashed_left_child_[key][0][i];
            }
            if(duplications > 1){
                duplicated_idx_ = 0;
                prev_key_ = key;
            }
            break;
        }
        if(right_output.size() == 0) break;
        // if this is an outer join don't finish yet, 
        // we still need to look at the non visited left keys.
        // finished_ = right_child_->finished_ && duplicated_idx_ == -1;
        Value v = evaluate_expression(ctx_, filter_, this);
        if(!v.isNull() && v.getBoolVal() == true)
            return output_;
    }
    // we got out of the loop but this is not an outer join,
    // or it is an outer join but all keys were visited, then we are done.
    if((join_type_ != LEFT_JOIN && join_type_!= FULL_JOIN) || non_visited_left_keys_.size() == 0) {
        finished_ = true;
        return {};
    }
    std::string key = *(non_visited_left_keys_.begin());

    int duplications = hashed_left_child_[key].size();
    if(duplicated_idx_ == -1)
        duplicated_idx_ = 0;
    for(int i = 0; i < hashed_left_child_[key][duplicated_idx_].size(); ++i){
        output_[i] = hashed_left_child_[key][duplicated_idx_][i];
    }
    // TODO: redundent should be done onle once not on every iteration.
    for(int i = hashed_left_child_[key][duplicated_idx_].size(); i < output_.size(); ++i) {
        output_[i] = Value(NULL_TYPE);
    }
    duplicated_idx_++;
    if(duplicated_idx_ == duplications){
        non_visited_left_keys_.erase(key);
        duplicated_idx_ = -1;
    }
    return output_;
}

/*
UnionExecutor::UnionExecutor(
        TableSchema* output_schema, QueryCTX* ctx,
        int query_idx, int parent_query_idx,
        Executor* lhs, Executor* rhs)
    : Executor(
            UNION_EXECUTOR, output_schema, ctx,
            query_idx, parent_query_idx, lhs),
    left_child_(lhs), right_child_(rhs)
{}
// doesn't own its children, so no cleaning needed.
UnionExecutor::~UnionExecutor()
{}*/

void UnionExecutor::construct(QueryCTX* ctx, AlgebraOperation* plan_node, Executor* lhs, Executor* rhs) {
    assert(plan_node != nullptr && plan_node->type_ == AL_UNION);
    assert(lhs && rhs);
    type_ = UNION_EXECUTOR;
    ctx_ = ctx; 
    plan_node_ = plan_node;
    output_schema_ = lhs->output_schema_;
    query_idx_ = plan_node->query_idx_;
    assert(query_idx_ < ctx->set_operations_.size());
    parent_query_idx_ = ctx->set_operations_[query_idx_]->parent_idx_;
    left_child_  = lhs;
    right_child_ = rhs;
}

void UnionExecutor::init() {
    error_status_ = 0;
    finished_ = 0;
    if(!left_child_ || !right_child_){
        error_status_ = true;
        return;
    }
    left_child_->init();
    right_child_->init();
    error_status_ = left_child_->error_status_ || right_child_->error_status_;
    finished_ = left_child_->finished_ && right_child_->finished_;
}

std::vector<Value> UnionExecutor::next() {
    if(error_status_ || finished_)  return {};

    if(!left_child_->finished_){
        output_ = left_child_->next();
        error_status_ = left_child_->error_status_;
        if(left_child_->finished_ && output_.size() == 0) {
            return next();
        }
    }
    else if(!right_child_->finished_){
        output_ = right_child_->next();
        error_status_ = right_child_->error_status_;
    }

    finished_ = left_child_->finished_ && right_child_->finished_;
    return output_;
}

/*
ExceptExecutor::ExceptExecutor(
        TableSchema* output_schema, QueryCTX* ctx,
        int query_idx, int parent_query_idx,
        Executor* lhs, Executor* rhs)
    : Executor(
            EXCEPT_EXECUTOR, output_schema, ctx,
            query_idx, parent_query_idx, lhs),
    left_child_(lhs), right_child_(rhs)
{}
// doesn't own its children, so no cleaning needed.
ExceptExecutor::~ExceptExecutor()
{}*/

void ExceptExecutor::construct(QueryCTX* ctx, AlgebraOperation* plan_node, Executor* lhs, Executor* rhs) {
    assert(plan_node != nullptr && plan_node->type_ == AL_EXCEPT);
    assert(lhs && rhs);
    type_ = EXCEPT_EXECUTOR;
    ctx_ = ctx; 
    plan_node_ = plan_node;
    output_schema_ = lhs->output_schema_;
    query_idx_ = plan_node->query_idx_;
    assert(query_idx_ < ctx->set_operations_.size());
    parent_query_idx_ = ctx->set_operations_[query_idx_]->parent_idx_;
    left_child_  = lhs;
    right_child_ = rhs;
}

void ExceptExecutor::init() {
    error_status_ = 0;
    finished_ = 0;
    if(!left_child_ || !right_child_){
        error_status_ = true;
        return;
    }
    left_child_->init();
    right_child_->init();
    error_status_ = left_child_->error_status_ || right_child_->error_status_;
    finished_ = left_child_->finished_;
    while(!right_child_->finished_ && !error_status_){
        std::vector<Value> tuple = right_child_->next();
        error_status_ = error_status_ && right_child_->error_status_;
        std::string stringified_tuple = "";
        for(size_t i = 0; i < tuple.size(); i++) stringified_tuple += tuple[i].toString();
        hashed_tuples_[stringified_tuple] =  1;
    } 
}

std::vector<Value> ExceptExecutor::next() {
    if(error_status_ || finished_)  return {};
    while(true){
        std::vector<Value> tuple = left_child_->next();
        if(finished_ || error_status_ || tuple.size() == 0) return {};
        std::string stringified_tuple = "";
        for(size_t i = 0; i < tuple.size(); i++) stringified_tuple += tuple[i].toString();
        if(hashed_tuples_.count(stringified_tuple)) 
            continue; // tuple exists on both relations => skip it.
        output_ = tuple;
        return output_;
    }
}

/*
IntersectExecutor::IntersectExecutor(TableSchema* output_schema, QueryCTX* ctx, int query_idx, int parent_query_idx, Executor* lhs, Executor* rhs)
    : Executor(INTERSECT_EXECUTOR, output_schema, ctx, query_idx, parent_query_idx, lhs), left_child_(lhs), right_child_(rhs)
{}
// doesn't own its children, so no cleaning needed.
IntersectExecutor::~IntersectExecutor()
{}*/

void IntersectExecutor::construct(QueryCTX* ctx, AlgebraOperation* plan_node, Executor* lhs, Executor* rhs) {
    assert(plan_node != nullptr && plan_node->type_ == AL_INTERSECT);
    assert(lhs && rhs);
    type_ = INTERSECT_EXECUTOR;
    ctx_ = ctx; 
    plan_node_ = plan_node;
    output_schema_ = lhs->output_schema_;
    query_idx_ = plan_node->query_idx_;
    assert(query_idx_ < ctx->set_operations_.size());
    parent_query_idx_ = ctx->set_operations_[query_idx_]->parent_idx_;
    left_child_  = lhs;
    right_child_ = rhs;
}

void IntersectExecutor::init() {
    error_status_ = 0;
    finished_ = 0;
    if(!left_child_ || !right_child_){
        error_status_ = true;
        return;
    }
    left_child_->init();
    right_child_->init();
    error_status_ = left_child_->error_status_ || right_child_->error_status_;
    finished_ = left_child_->finished_;
    while(!right_child_->finished_ && !error_status_){
        std::vector<Value> tuple = right_child_->next();
        error_status_ = error_status_ && right_child_->error_status_;
        std::string stringified_tuple = "";
        for(size_t i = 0; i < tuple.size(); i++) stringified_tuple += tuple[i].toString();
        hashed_tuples_[stringified_tuple] =  1;
    } 
}

std::vector<Value> IntersectExecutor::next() {
    if(error_status_ || finished_)  return {};
    while(true){
        std::vector<Value> tuple = left_child_->next();
        if(finished_ || error_status_ || tuple.size() == 0) return {};
        std::string stringified_tuple = "";
        for(size_t i = 0; i < tuple.size(); i++) stringified_tuple += tuple[i].toString();
        if(!hashed_tuples_.count(stringified_tuple)) 
            continue; // tuple does not exists on both relations => skip it.
        output_ = tuple;
        return output_;
    }
}



/*
SeqScanExecutor::SeqScanExecutor(TableSchema* table, QueryCTX* ctx, int query_idx, int parent_query_idx)
    : Executor(SEQUENTIAL_SCAN_EXECUTOR, table, ctx, query_idx, parent_query_idx, nullptr), table_(table)
{}
SeqScanExecutor::~SeqScanExecutor()
{
    delete it_;
    delete table_;
}*/

void SeqScanExecutor::construct(QueryCTX* ctx, AlgebraOperation* plan_node, TableSchema* table) {
    assert(plan_node != nullptr && plan_node->type_ == SCAN);
    type_ = SEQUENTIAL_SCAN_EXECUTOR;
    ctx_ = ctx; 
    plan_node_ = plan_node;
    output_schema_ = table;
    query_idx_ = plan_node->query_idx_;
    assert(query_idx_ < ctx->queries_call_stack_.size());
    parent_query_idx_ = ctx->queries_call_stack_[query_idx_]->parent_idx_;
    table_ = table;
}

void SeqScanExecutor::init() {
    error_status_ = 0;
    finished_ = 0;
    output_.resize(output_schema_->numOfCols());
    //delete it_;
    it_ = table_->getTable()->begin();
    ctx_->table_handles_.push_back(it_);
}

std::vector<Value> SeqScanExecutor::next() {
    // no more records.
    if(!it_->advance()) {
        finished_ = 1;
        return {};
    };
    Record* r = it_->getCurRecordCpyPtr();
    int err = table_->translateToValuesOffset(*r, output_, 0);
    delete r;
    if(err) {
        error_status_ = 1;
        return {};
    }
    return output_;
}

// TODO: change BTreeIndex type to be a generic ( just Index ) that might be a btree or hash index.
/*
IndexScanExecutor::IndexScanExecutor(IndexHeader index, ASTNode* filter,
        TableSchema* table, QueryCTX* ctx, int query_idx, int parent_query_idx)
    : Executor(INDEX_SCAN_EXECUTOR, table, ctx, query_idx, parent_query_idx, nullptr), 
    table_(table), 
    index_header_(index),
    filter_(filter)
{}
IndexScanExecutor::~IndexScanExecutor()
{
    delete table_;
    start_it_.clear();
    end_it_.clear();
}*/

void IndexScanExecutor::construct(QueryCTX* ctx, AlgebraOperation* plan_node, TableSchema* table, IndexHeader index) {
    assert(plan_node != nullptr && plan_node->type_ == SCAN);
    type_ = INDEX_SCAN_EXECUTOR;
    ctx_ = ctx; 
    plan_node_ = plan_node;
    output_schema_ = table;
    query_idx_ = plan_node->query_idx_;
    assert(query_idx_ < ctx->queries_call_stack_.size());
    parent_query_idx_ = ctx->queries_call_stack_[query_idx_]->parent_idx_;

    table_ = table;
    index_header_ = index;
    filter_ = ((ScanOperation*)plan_node)->filter_;
}

void IndexScanExecutor::assign_iterators() {
    ASTNode* ptr = filter_;
    CategoryType cat = ptr->category_;
    switch(cat) {
        case EQUALITY:
        case COMPARISON:{
                            ASTNode* left  = nullptr; 
                            ASTNode* right = nullptr; 
                            if(cat == EQUALITY){
                                left  = ((EqualityNode*)ptr)->cur_;
                                right = ((EqualityNode*)ptr)->next_;
                            } else if(cat == COMPARISON) {
                                left  = ((ComparisonNode*)ptr)->cur_;
                                right = ((ComparisonNode*)ptr)->next_;
                            }
                            Value val;
                            std::vector<std::string> key;
                            accessed_fields(left , key);
                            int size_before = key.size();
                            bool key_on_left = (size_before != 0);
                            accessed_fields(right, key);
                            assert(key.size() == 1);

                            if(key_on_left)
                                val = evaluate_expression(ctx_, right, this);
                            else
                                val = evaluate_expression(ctx_, left, this);
                            std::vector<Value> key_vals = {val};
                            IndexKey search_key = temp_index_key_from_values(key_vals);
                            search_key.sort_order_ = create_sort_order_bitmap(index_header_.fields_numbers_);
                            if(cat == EQUALITY){
                                start_it_ = index_header_.index_->begin(search_key);
                                end_it_ = index_header_.index_->begin(search_key);
                                while(end_it_.getCurKey() == search_key){
                                    int advanced = end_it_.advance();
                                    if(!advanced) break;
                                }
                            } else if(cat == COMPARISON){
                                TokenType op = ptr->token_.type_;
                                // if both true they cancel each other to false using xor.
                                if(!key_on_left^index_header_.fields_numbers_[0].desc_){
                                    if(op == TokenType::LT) op = TokenType::GT;
                                    else if(op == TokenType::GT) op = TokenType::LT;
                                    else if(op == TokenType::LTE) op = TokenType::GTE;
                                    else if(op == TokenType::GTE) op = TokenType::LTE;
                                    else assert(0);
                                }
                                if(op == TokenType::LT || op == TokenType::LTE) {
                                    start_it_ = index_header_.index_->begin();
                                    end_it_ = index_header_.index_->begin(search_key);
                                } else if(op == TokenType::GT || op == TokenType::GTE){
                                    start_it_ = index_header_.index_->begin(search_key);
                                    end_it_.assign_to_null_page(); 
                                } else {
                                    assert(0 && "COMPARISON HAS INVALID OPERATOR");
                                }
                                if(op == TokenType::LTE && end_it_.getCurKey() == search_key  ) end_it_.advance();
                                if(op == TokenType::GT  && start_it_.getCurKey() == search_key)  start_it_.advance();
                            }
                            break;
                        }
        default:
                        assert(0 && "NOT SUPPORTED INDEX SCAN CONDITION!");
    }
    ctx_->index_handles_.push_back(&end_it_);
    ctx_->index_handles_.push_back(&start_it_);
}

void IndexScanExecutor::init() {
    error_status_ = 0;
    finished_ = 0;
    output_.resize(output_schema_->numOfCols());

    start_it_.clear();
    end_it_.clear();
    assign_iterators();
}

std::vector<Value> IndexScanExecutor::next() {
    // no more records.
    if(start_it_ == end_it_) {
        finished_ = 1;
        return {};
    };
    Record* r = start_it_.getCurRecordCpyPtr();
    if(!r){
        std::cout << "Could not translate record\n";
        error_status_ = 1;
        return {};
    }
    int err = table_->translateToValuesOffset(*r, output_, 0);
    delete r;
    if(err) {
        error_status_ = 1;
        return {};
    }
    start_it_.advance();
    return output_;
}

/*
InsertionExecutor::InsertionExecutor(TableSchema* table, std::vector<IndexHeader> indexes, QueryCTX* ctx, int query_idx, int parent_query_idx, int select_idx)
    : Executor(INSERTION_EXECUTOR, table, ctx, query_idx, parent_query_idx, nullptr),
    table_(table), indexes_(indexes), select_idx_(select_idx)
{}
InsertionExecutor::~InsertionExecutor()
{}*/

void InsertionExecutor::construct(QueryCTX* ctx, AlgebraOperation* plan_node, TableSchema* table, 
        std::vector<IndexHeader> indexes, int select_idx) {
    assert(plan_node != nullptr && plan_node->type_ == INSERTION);
    type_ = INSERTION_EXECUTOR;
    ctx_ = ctx; 
    plan_node_ = plan_node;
    output_schema_ = table;
    query_idx_ = plan_node->query_idx_;
    assert(query_idx_ < ctx->queries_call_stack_.size());
    parent_query_idx_ = ctx->queries_call_stack_[query_idx_]->parent_idx_;

    table_ = table;
    indexes_ = indexes;
    select_idx_ = select_idx;
}

void InsertionExecutor::init() {
    error_status_ = 0;
    finished_ = 0;
    if(!table_){
        error_status_ = 1;
        return;
    }
    vals_.resize(table_->numOfCols());
    statement = reinterpret_cast<InsertStatementData*>(ctx_->queries_call_stack_[query_idx_]);
    // fields
    if(!statement->fields_.size() || statement->fields_.size() < table_->getCols().size()){
        statement->fields_ = table_->getCols();
    } else {
        for(auto& field_name : statement->fields_){
            int idx = table_->colExist(field_name);
            if(!table_->isValidCol(field_name) || idx < 0) {
                error_status_ = 1;
                return;
            }
        }
    }

    // TODO: Provide strict type checking before inserting data.


    // this means we're using insert .. select syntax.
    if(select_idx_ != -1 && select_idx_ < ctx_->executors_call_stack_.size()){
        child_executor_ = ctx_->executors_call_stack_[select_idx_];
        child_executor_->init();
        if(child_executor_->error_status_) {
            error_status_ = 1;
            return;
        }
    } else {
        // insert .. values syntax.
        if(statement->values_.size() != statement->fields_.size()){
            error_status_ = 1;
            return;
        }
    }

}

std::vector<Value> InsertionExecutor::next() {
    if(error_status_ || finished_) return {};
    // TODO: replace alot of vector copying.
    if(child_executor_){
        std::vector<Value> values = child_executor_->next();
        if(child_executor_->finished_) {
            finished_ = 1;
            return {};
        }
        if(child_executor_->error_status_ || values.size() != statement->fields_.size()){
            error_status_ = 1;
            return {};
        }
        for(int i = 0; i < values.size(); ++i){
            int idx = table_->colExist(statement->fields_[i]); 
            vals_[idx] = values[i];
        }

    } else {
        for(int i = 0; i < statement->values_.size(); ++i){
            ExpressionNode* val_exp = statement->values_[i];
            int idx = table_->colExist(statement->fields_[i]); 
            vals_[idx] = evaluate_expression(ctx_, val_exp, this);
        }
    }

    RecordID* rid = new RecordID();
    Record* record = table_->translateToRecord(vals_);
    int err = table_->getTable()->insertRecord(rid, *record);
    if(err){
        error_status_ = 1;
        return {};
    }
    // loop over table indexes.
    for(int i = 0; i < indexes_.size(); ++i){
        IndexKey k = getIndexKeyFromTuple(indexes_[i].fields_numbers_, vals_);
        if(k.size_ == 0) {
            error_status_ = 1;
            break;
        }
        bool success = indexes_[i].index_->Insert(k, *rid);
        delete k.data_;
        //indexes_[i].index_->See();
        if(!success){
            std::cout << "Could Not insert into index\n";
            error_status_ = 1;
            break;
        }
    }
    delete record;
    delete rid;
    if(err || error_status_) {
        error_status_ = 1;
        return {};
    }
    if(!child_executor_ || child_executor_->finished_)
        finished_ = 1;
    return vals_;
}

/*
AggregationExecutor::AggregationExecutor(Executor* child_executor, TableSchema* output_schema, 
        std::vector<AggregateFuncNode*> aggregates, std::vector<ASTNode*> group_by, QueryCTX* ctx, int query_idx, int parent_query_idx): 
    Executor(AGGREGATION_EXECUTOR, output_schema, ctx, query_idx, parent_query_idx, child_executor), aggregates_(aggregates), group_by_(group_by)
{}
AggregationExecutor::~AggregationExecutor()
{
    delete output_schema_;
    delete child_executor_;
}*/

void AggregationExecutor::construct(QueryCTX* ctx, AlgebraOperation* plan_node, Executor* child_executor) {

    assert(plan_node != nullptr && plan_node->type_ == AGGREGATION);
    type_ = AGGREGATION_EXECUTOR;
    ctx_ = ctx; 
    plan_node_ = plan_node;
    //output_schema_ = output_schema;
    child_executor_ = child_executor;

    query_idx_ = plan_node->query_idx_;
    assert(query_idx_ < ctx->queries_call_stack_.size());
    parent_query_idx_ = ctx->queries_call_stack_[query_idx_]->parent_idx_;
    
    aggregates_ = ((AggregationOperation*)plan_node_)->aggregates_;
    group_by_ = ((AggregationOperation*)plan_node_)->group_by_;

    // build the new output schema.
    std::vector<Column> new_cols;
    int offset_ptr = 0; 
    if(child_executor_ && child_executor_->output_schema_){
        new_cols = child_executor_->output_schema_->getColumns();
        offset_ptr = Column::getSizeFromType(new_cols[new_cols.size() - 1].getType());
    } 
    for(int i = 0; i < aggregates_.size(); i++){
        std::string col_name = "agg_tmp_schema.";
        col_name += AGG_FUNC_IDENTIFIER_PREFIX;
        //col_name += intToStr(op->aggregates_[i]->parent_id_);
        col_name += intToStr(i+1);
        new_cols.push_back(Column(col_name, INT, offset_ptr));
        offset_ptr += Column::getSizeFromType(INT);
    }

    output_schema_ = new TableSchema("agg_tmp_schema", nullptr, new_cols, true); // TODO: fix leak.
}

void AggregationExecutor::init() {
    finished_ = 0;
    error_status_ = 0;
    aggregated_values_.clear();
    for(int i = 0; i < aggregates_.size(); ++i) {
        if(aggregates_[i]->distinct_) distinct_counters_["PREFIX_"][i] = std::set<std::string>();
    }


    if(child_executor_){
        child_executor_->init();
    }
    int total_size = output_schema_->getCols().size() + 1;
    aggregated_values_["PREFIX_"] = std::vector<Value> (total_size, Value(NULL_TYPE));

    int agg_base_idx = total_size - (1+aggregates_.size());
    for(int i = 0;i < aggregates_.size(); ++i) {
        if(aggregates_[i]->type_ == COUNT)
            aggregated_values_["PREFIX_"][i+agg_base_idx] = Value(0); // count can't be null.
    }

    while(true){
        // we always maintain rows count even if the user did not ask for it, that's why the size is | colmuns | + 1
        output_ = std::vector<Value> (output_schema_->getCols().size() + 1, Value(0));
        std::vector<Value> child_output; 
        if(child_executor_){
            child_output = child_executor_->next();
            if(child_executor_->error_status_)  {
                error_status_ = true;
                return;
            }
            if(child_executor_->finished_) {
                break;
            }
        } 
        for(int i = 0; i < child_output.size(); i++){
            output_[i] = child_output[i];
        }

        // build the search key for the hash table.
        std::string hash_key = "PREFIX_"; // prefix to ensure we have at least one entry in the hash table.
        for(int i = 0; i < group_by_.size(); i++){
            Value cur = evaluate_expression(ctx_, group_by_[i], this);
            hash_key += cur.toString();
        }

        // if the hash key exists we need to load it first.
        if(aggregated_values_.count(hash_key)){
            output_ = aggregated_values_[hash_key];
        } else if(hash_key != "PREFIX_"){
            for(int i = 0; i < aggregates_.size(); ++i) {
                if(aggregates_[i]->distinct_) distinct_counters_[hash_key][i] = std::set<std::string>();
            }

            int total_size = output_schema_->getCols().size() + 1;
            aggregated_values_[hash_key] = std::vector<Value> (total_size, Value(NULL_TYPE));

            int agg_base_idx = total_size - (1+aggregates_.size());
            for(int i = 0;i < aggregates_.size(); ++i) {
                if(aggregates_[i]->type_ == COUNT)
                    aggregated_values_[hash_key][i+agg_base_idx] = Value(0); // count can't be null.
            }
            output_ = aggregated_values_[hash_key];
        }

        for(int i = 0; i < child_output.size(); i++){
            output_[i] = child_output[i];
        }


        // update the extra counter.
        if(output_[output_.size() - 1].isNull()) output_[output_.size() - 1] = Value(0);
        output_[output_.size() - 1] += 1; 
        Value* counter = &output_[output_.size() - 1];

        int base_size = child_output.size();
        for(int i = 0; i < aggregates_.size(); i++){
            ExpressionNode* exp = aggregates_[i]->exp_;
            if(exp){
                Value val = evaluate_expression(ctx_, exp, this);
                if(aggregates_[i]->distinct_){
                    if(distinct_counters_[hash_key][i].count(val.toString())) continue;
                    distinct_counters_[hash_key][i].insert(val.toString());
                }
            }
            int idx = base_size+i;
            switch(aggregates_[i]->type_){
                case COUNT:
                    {
                        if(exp == nullptr){
                            ++output_[idx];
                            break;
                        }
                        Value val = evaluate_expression(ctx_, exp, this);
                        if(!val.isNull()){
                            ++output_[idx];
                        }
                    }
                    break;
                case AVG:
                case SUM:
                    {
                        Value val = evaluate_expression(ctx_, exp, this);
                        if(output_[idx].isNull() 
                                && !val.isNull()) output_[idx] = Value(0);
                        if(!val.isNull()) {
                            output_[idx] += val;
                        }
                        else if(val.isNull())
                            *counter += -1;
                    }
                    break;
                case MIN:
                    {
                        Value val = evaluate_expression(ctx_, exp, this);
                        if(!val.isNull()) {
                            if(output_[idx].isNull() || output_[idx] > val) 
                                output_[idx] = val;
                            //output_[idx] = std::min<Value>(output_[idx], val);
                        }
                    }
                    break;
                case MAX:
                    {
                        Value val = evaluate_expression(ctx_, exp, this);
                        if(!val.isNull()) {
                            if(output_[idx].isNull() || output_[idx] < val) 
                                output_[idx] = val;
                            //output_[idx] = std::max<Value>(output_[i], val);
                        }
                    }
                    break;
                default :
                    break;
            }
            if(error_status_)  return;
        }
        aggregated_values_[hash_key] = output_;
        if(!child_executor_) break;
    }
    if(aggregated_values_.size() > 1) aggregated_values_.erase("PREFIX_");
    it_ = aggregated_values_.begin();
}

std::vector<Value> AggregationExecutor::next() {
    if(error_status_ || finished_)  return {};
    if(it_== aggregated_values_.end()){
        finished_ = true;
        return {};
    }
    output_ = it_->second;
    for(int i = 0; i < aggregates_.size(); i++){
        int idx = (i + output_.size() - aggregates_.size())- 1;
        if(aggregates_[i]->type_ == AVG && output_[output_.size()-1].getIntVal() != 0) {
            if(output_[idx].isNull() || output_[output_.size() - 1].isNull()) output_[idx] = Value(NULL_TYPE);
            else {
                float denom = (float) output_[output_.size()-1].getIntVal();
                if(aggregates_[i]->distinct_) 
                    denom = distinct_counters_[it_->first][i].size();
                if(denom == 0) { // maybe too much checking?
                    output_[idx] = Value(NULL_TYPE);
                    continue;
                }
                output_[idx] /= Value(denom);
            }
        }
    }
    ++it_;
    if(it_== aggregated_values_.end())
        finished_ = true;
    output_.pop_back(); // remove the custom counter.
    return output_;
}

/*
ProjectionExecutor::ProjectionExecutor(Executor* child_executor, TableSchema* output_schema, std::vector<ExpressionNode*> fields, QueryCTX* ctx, int query_idx, int parent_query_idx): 
    Executor(PROJECTION_EXECUTOR, output_schema, ctx, query_idx, parent_query_idx, child_executor), fields_(fields)
{}
ProjectionExecutor::~ProjectionExecutor()
{
    delete child_executor_;
}*/

void ProjectionExecutor::construct(QueryCTX* ctx, AlgebraOperation* plan_node, Executor* child_executor) {

    assert(plan_node != nullptr && plan_node->type_ == PROJECTION);
    type_ = PROJECTION_EXECUTOR;
    ctx_ = ctx; 
    plan_node_ = plan_node;
    child_executor_ = child_executor;

    if(child_executor)
        output_schema_ = child_executor_->output_schema_;

    query_idx_ = plan_node->query_idx_;
    assert(query_idx_ < ctx->queries_call_stack_.size());
    parent_query_idx_ = ctx->queries_call_stack_[query_idx_]->parent_idx_;
    
    fields_ = ((ProjectionOperation*)plan_node_)->fields_;
}

void ProjectionExecutor::init() {
    finished_ = 0;
    error_status_ = 0;
    output_ = {};
    if(child_executor_) {
        child_executor_->init();
    }
}

std::vector<Value> ProjectionExecutor::next() {
    if(error_status_ || finished_)  return {};


    if(child_executor_){
        output_ = child_executor_->next();
        error_status_ = child_executor_->error_status_;
        finished_ = child_executor_->finished_;
    } else {
        finished_ = true;
    }

    std::vector<Value> tmp_output;
    if(child_executor_ && ((finished_ || error_status_) && output_.size() == 0)) return {};

    for(int i = 0; i < fields_.size(); i++){
        if(fields_[i] == nullptr){
            for(auto& val : output_){
                tmp_output.push_back(val);
            }
        } else {
            tmp_output.push_back(evaluate_expression(ctx_, fields_[i], this));
        }
    }
    return tmp_output;
}

/*
SortExecutor::SortExecutor(Executor* child_executor , TableSchema* output_schema, std::vector<int> order_by_list, QueryCTX* ctx, int query_idx, int parent_query_idx): 
    Executor(SORT_EXECUTOR, output_schema, ctx, query_idx, parent_query_idx, child_executor), order_by_list_(order_by_list)
{}
SortExecutor::~SortExecutor()
{
    delete child_executor_;
}*/

void SortExecutor::construct(QueryCTX* ctx, AlgebraOperation* plan_node, Executor* child_executor) {

    assert(plan_node != nullptr && plan_node->type_ == SORT);
    assert(child_executor != nullptr);
    type_ = SORT_EXECUTOR;
    ctx_ = ctx; 
    plan_node_ = plan_node;
    child_executor_ = child_executor;
    output_schema_ = child_executor_->output_schema_;

    query_idx_ = plan_node->query_idx_;
    assert(query_idx_ < ctx->queries_call_stack_.size());
    parent_query_idx_ = ctx->queries_call_stack_[query_idx_]->parent_idx_;
    
    order_by_list_ = ((SortOperation*)plan_node_)->order_by_list_;
}

void SortExecutor::init() {
    if(idx_ != 0) {
        idx_ = 0;
        finished_ = 0;
        error_status_ = 0;
        return;
    }
    child_executor_->init();

    while(true){
        std::vector<Value> tuple; 
        tuple = child_executor_->next();
        error_status_ = child_executor_->error_status_;
        if(error_status_)  return;
        if(tuple.size())
            tuples_.push_back(tuple);
        if(child_executor_->finished_) break;
    }

    std::vector<int> order_by = order_by_list_;

    std::sort(tuples_.begin(), tuples_.end(), 
            [&order_by](std::vector<Value>& lhs, std::vector<Value>& rhs){
            for(int i = 0; i < order_by.size(); i++){
            if(lhs[order_by[i]].isNull() && !rhs[order_by[i]].isNull()) return true;
            if(!lhs[order_by[i]].isNull()&& rhs[order_by[i]].isNull()) return false;
            if(lhs[order_by[i]].isNull() && rhs[order_by[i]].isNull()) return false;
            if(lhs[order_by[i]] != rhs[order_by[i]]) {
            return lhs[order_by[i]] < rhs[order_by[i]];
            }
            }
            return false;
            });
}

std::vector<Value> SortExecutor::next() {
    finished_ = (idx_ >= tuples_.size());
    if(error_status_ || finished_)  return {};
    output_ = tuples_[idx_];
    return tuples_[idx_++];
}

/*
DistinctExecutor::DistinctExecutor(Executor* child_executor , TableSchema* output_schema, QueryCTX* ctx, int query_idx, int parent_query_idx): 
    Executor(DISTINCT_EXECUTOR, output_schema, ctx, query_idx, parent_query_idx, child_executor)
{}
DistinctExecutor::~DistinctExecutor()
{
    delete child_executor_;
}*/

void DistinctExecutor::construct(QueryCTX* ctx, Executor* child_executor) {
    assert(child_executor != nullptr);
    type_ = DISTINCT_EXECUTOR;
    ctx_ = ctx; 
    child_executor_ = child_executor;
    plan_node_ = child_executor_->plan_node_;
    output_schema_ = child_executor_->output_schema_;

    query_idx_ = child_executor_->query_idx_;
    parent_query_idx_ = child_executor_->parent_query_idx_; 
}

void DistinctExecutor::init() {
    finished_ = 0;
    error_status_ = 0;
    child_executor_->init();
}

std::vector<Value> DistinctExecutor::next() {
    if(error_status_ || finished_)  return {};
    while(true){
        if(finished_ || error_status_) return {};
        std::vector<Value> tuple = child_executor_->next();
        if(tuple.size() == 0) {
            finished_ = true;
            return {};
        }
        error_status_ = child_executor_->error_status_;
        finished_ = child_executor_->finished_;
        std::string stringified_tuple = "";
        for(size_t i = 0; i < tuple.size(); i++) {
            stringified_tuple += ",";
            stringified_tuple += tuple[i].toString();
        }
        if(hashed_tuples_.count(stringified_tuple)) continue; // duplicated tuple => skip it.
        hashed_tuples_[stringified_tuple] =  1;
        output_ = tuple;
        return output_;
    }
}

/*

SubQueryExecutor::SubQueryExecutor(Executor* child_executor, TableSchema* output_schema, QueryCTX* ctx, int query_idx, int parent_query_idx): 
    Executor(SUB_QUERY_EXECUTOR, output_schema, ctx, query_idx, parent_query_idx, child_executor)
{}
SubQueryExecutor::~SubQueryExecutor()
{
    delete child_executor_;
}*/

void SubQueryExecutor::construct(QueryCTX* ctx, Executor* child_executor) {
    assert(child_executor != nullptr);
    type_ = SUB_QUERY_EXECUTOR;
    ctx_ = ctx; 
    child_executor_ = child_executor;
    plan_node_ = child_executor_->plan_node_;
    output_schema_ = child_executor_->output_schema_;

    query_idx_ = child_executor_->query_idx_;
    parent_query_idx_ = child_executor_->parent_query_idx_; 
}

void SubQueryExecutor::init() {
    finished_ = 0;
    error_status_ = 0;
    if(!cached_){
        child_executor_->init();
    } else {
        it_ = tuple_list_.begin();
    }
}

std::vector<Value> SubQueryExecutor::next() {
    if(finished_ || error_status_) {
        return {};
    }
    if(!cached_){
        std::vector<Value> tuple = child_executor_->next();
        if(tuple.size() == 0) {
            cached_ = true;
            finished_ = true;
            return {};
        }
        error_status_ = child_executor_->error_status_;
        finished_ = child_executor_->finished_;
        tuple_list_.push_back(tuple);
        output_ = tuple;
    } else {
        if(it_ == tuple_list_.end()) {
            error_status_ = child_executor_->error_status_;
            finished_ = child_executor_->finished_;
            return {};
        }
        output_ = *it_;
        ++it_;
    }
    if(finished_) cached_ = true;
    return output_;
}



/*
FilterExecutor::FilterExecutor(Executor* child, TableSchema* output_schema, ExpressionNode* filter, 
        std::vector<ExpressionNode*>& fields, 
        std::vector<std::string>& field_names,
        QueryCTX* ctx,
        int query_idx,
        int parent_query_idx)
    : Executor(FILTER_EXECUTOR, output_schema, ctx, query_idx, parent_query_idx, child), filter_(filter), 
    fields_(fields), field_names_(field_names)
{}
FilterExecutor::~FilterExecutor()
{
    delete child_executor_;
}*/

void FilterExecutor::construct(QueryCTX* ctx, AlgebraOperation* plan_node, Executor* child_executor) {

    assert(plan_node != nullptr && plan_node->type_ == FILTER);
    type_ = FILTER_EXECUTOR;
    ctx_ = ctx; 
    plan_node_ = plan_node;
    //output_schema_ = output_schema;
    child_executor_ = child_executor;
    if(child_executor_) output_schema_ = child_executor_->output_schema_;

    query_idx_ = plan_node->query_idx_;
    assert(query_idx_ < ctx->queries_call_stack_.size());
    parent_query_idx_ = ctx->queries_call_stack_[query_idx_]->parent_idx_;
    
    fields_      = ((FilterOperation*)plan_node_)->fields_;
    field_names_ = ((FilterOperation*)plan_node_)->field_names_;
    filter_      = ((FilterOperation*)plan_node_)->filter_;
}


void FilterExecutor::init() {
    error_status_ = 0;
    finished_ = 0;
    output_ = {};
    if(child_executor_) {
        child_executor_->init();
    }
}


std::vector<Value> FilterExecutor::next() {
    while(true){
        if(error_status_ || finished_)  return {};
        if(child_executor_) {
            output_ = child_executor_->next();
            error_status_ = child_executor_->error_status_;
            finished_ = child_executor_->finished_;
        } else {
            finished_ = true;
        }

        if(child_executor_ && ((finished_ || error_status_) && output_.size() == 0)) return {};

        Value exp = evaluate_expression(ctx_, filter_, this, true, true).getBoolVal();
        if(!exp.isNull() && exp != false){
            if(child_executor_){
                return output_;
            }
            return {exp};
        }
    }
}
