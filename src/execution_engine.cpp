#pragma once
#include "catalog.cpp"
#include "parser.cpp"
#include "expression.cpp"
#include "algebra_engine.cpp"
#include "utils.cpp"
#include <deque>


typedef std::vector<std::vector<Value>> QueryResult;


/* The execution engine that holds all execution operators that could be 
 * (sequential scan, index scan) which are access operators,
 * (filter, join, projection) that are relational algebra operators, 
 * other operator such as (sorting, aggrecations) 
 * or modification queries (delete, insert, update).
 * Each one of these operators implements a next() method that will produce the next record of the current table that
 * is being scanned filtered joined etc... This method has so many names ( volcano, pipleline, iterator ) as apposed to
 * building the entire output table and returning it all at once.
 * In order to initialize an operator we need some sorts of meta data that should be passed into it via the constructor,
 * The larger the project gets the more data we are going to need, Which might require using a wrapper class 
 * around the that data, But for now we are just going to pass everything to the constructor.
 */



enum ExecutorType {
    SEQUENTIAL_SCAN_EXECUTOR,
    INSERTION_EXECUTOR,
    FILTER_EXECUTOR,
    AGGREGATION_EXECUTOR,
    PROJECTION_EXECUTOR,
    SORT_EXECUTOR,
    DISTINCT_EXECUTOR,
};

struct Executor {
    public:
        Executor(ExecutorType type, TableSchema* output_schema, QueryCTX& ctx,int query_idx, int parent_query_idx, Executor* child): 
            type_(type), output_schema_(output_schema), ctx_(ctx), 
            query_idx_(query_idx), parent_query_idx_(parent_query_idx), child_executor_(child)
        {}
        ~Executor() {}
        virtual void init() = 0;
        virtual std::vector<Value> next() = 0;

        ExecutorType type_;
        Executor* child_executor_ = nullptr;
        TableSchema* output_schema_ = nullptr;
        std::vector<Value> output_ = {};
        bool error_status_ = 0;
        bool finished_ = 0;
        int query_idx_ = -1;
        int parent_query_idx_ = -1;
        QueryCTX& ctx_;
};

class SeqScanExecutor : public Executor {
    public:
        SeqScanExecutor(TableSchema* table, QueryCTX& ctx, int query_idx, int parent_query_idx)
            : Executor(SEQUENTIAL_SCAN_EXECUTOR, table, ctx, query_idx, parent_query_idx, nullptr), table_(table)
        {}
        ~SeqScanExecutor()
        {}

        void init() {
            error_status_ = 0;
            finished_ = 0;
            it_ = table_->getTable()->begin();
        }

        std::vector<Value> next() {
            // no more records.
            if(!it_->advance()) {
                finished_ = 1;
                return {};
            };
            output_.clear();
            Record r = it_->getCurRecordCpy();
            int err = table_->translateToValues(r, output_);
            if(err) {
                error_status_ = 1;
                return {};
            }
            return output_;
        }
    private:
        TableSchema* table_ = nullptr;
        TableIterator* it_ = nullptr;
};

class InsertionExecutor : public Executor {
    public:
        InsertionExecutor(TableSchema* table, QueryCTX& ctx, int query_idx, int parent_query_idx)
            : Executor(INSERTION_EXECUTOR, table, ctx, query_idx, parent_query_idx, nullptr), table_(table)
        {}
        ~InsertionExecutor()
        {}

        Value evaluate(ASTNode* item){
            if(item->category_ != FIELD && item->category_ != SCOPED_FIELD){
                std::cout << "[ERROR] Item type is not supported!" << std::endl;
                error_status_ = 1;
                return Value();
            }

            std::string field = item->token_.val_;
            if(!output_schema_) {
                std::cout << "[ERROR] Cant access field name without schema " << field << std::endl;
                error_status_ = 1;
                return Value();
            }
            int idx = -1;
            if(item->category_ == SCOPED_FIELD){
                std::string table = output_schema_->getTableName();
                table = reinterpret_cast<ScopedFieldNode*>(item)->table_->token_.val_;
                std::string col = table;col += "."; col+= field;
                idx = output_schema_->colExist(col);
                //    output_schema_->printTableHeader();
                if(idx < 0 || idx >= output_.size()) {
                    std::cout << "[ERROR] Invalid field name " << col << std::endl;
                    error_status_ = 1;
                    return Value();
                }
            } else {
                int num_of_matches = 0;
                auto columns = output_schema_->getColumns();
                for(size_t i = 0; i < columns.size(); ++i){
                    std::vector<std::string> splittedStr = strSplit(columns[i].getName(), '.');
                    if(splittedStr.size() != 2) {
                        //output_schema_->printTableHeader();
                        std::cout << "[ERROR] Invalid schema " << std::endl;
                        error_status_ = 1;
                        return Value();
                    }
                    if(field == splittedStr[1]){
                        num_of_matches++;
                        idx = i;
                    }
                }
                if(num_of_matches > 1){
                    std::cout << "[ERROR] Ambiguous field name: " << field << std::endl;
                    error_status_ = 1;
                    return Value();
                }
                if(idx < 0 || idx >= output_.size()) {
                    std::cout << "[ERROR] Invalid field name " << field << std::endl;
                    //output_schema_->printTableHeader();
                    error_status_ = 1;
                    return Value();
                }
            }
            return output_[idx];
        } 

        void init() {
            error_status_ = 0;
            finished_ = 0;
            if(!table_){
                error_status_ = 1;
                return;
            }
            vals_.resize(table_->numOfCols());
            statement = reinterpret_cast<InsertStatementData*>(ctx_.queries_call_stack_[query_idx_]);
            // fields
            if(!statement->fields_.size() || statement->fields_.size() < table_->getCols().size()){
                statement->fields_ = table_->getCols();
            } else {
                for(auto& field_name : statement->fields_){
                    int idx = table_->colExist(field_name);
                    if(!table_->isValidCol(field_name) || idx < 0) {
                        error_status_ = 0;
                        return;
                    }
                }
            }
            if(statement->values_.size() != statement->fields_.size()){
                error_status_ = 0;
                return;
            }
            // values
            for(int i = 0; i < statement->values_.size(); ++i){
                ExpressionNode* val_exp = statement->values_[i];
                int idx = table_->colExist(statement->fields_[i]); 
                vals_[idx] = evaluate_expression(val_exp, eval);
            }
        }

        std::vector<Value> next() {
            RecordID* rid = new RecordID();
            Record* record = table_->translateToRecord(vals_);
            int err = table_->getTable()->insertRecord(rid, *record);
            if(err) {
                error_status_ = 1;
                return {};
            }
            finished_ = 1;
            return vals_;
        }
    private:
        TableSchema* table_ = nullptr;
        InsertStatementData* statement = nullptr;
        std::vector<Value> vals_  {};
        std::function<Value(ASTNode*)>
            eval = std::bind(&InsertionExecutor::evaluate, this, std::placeholders::_1);
};


class FilterExecutor : public Executor {
    public:
        FilterExecutor(Executor* child, TableSchema* output_schema, ExpressionNode* filter, 
                std::vector<ExpressionNode*>& fields, 
                std::vector<std::string>& field_names,
                QueryCTX& ctx,
                int query_idx,
                int parent_query_idx
                )
            : Executor(FILTER_EXECUTOR, output_schema, ctx, query_idx, parent_query_idx, child), filter_(filter), 
              fields_(fields), field_names_(field_names)
        {}
        ~FilterExecutor()
        {}

        Value evaluate(ASTNode* item){
            if(item->category_ != FIELD && item->category_ != SCOPED_FIELD){
                std::cout << "[ERROR] Item type is not supported!" << std::endl;
                error_status_ = 1;
                return Value();
            }

            std::string field = item->token_.val_;
            int idx = -1;
            if(item->category_ == SCOPED_FIELD){
                TableSchema* schema_ptr = output_schema_;
                int cur_query_idx = query_idx_;
                int cur_query_parent = parent_query_idx_;
                int idx = -1;
                 while(true){
                    if(!schema_ptr && cur_query_parent != -1){
                        Executor* parent_query = ctx_.executors_call_stack_[cur_query_parent]; 
                        schema_ptr = parent_query->output_schema_;
                        cur_query_idx = parent_query->query_idx_;
                        cur_query_parent = parent_query->parent_query_idx_;
                        continue;
                    } else if(!schema_ptr){
                        std::cout << "[ERROR] Cant access field name without schema " << field << std::endl;
                        error_status_ = 1;
                        return Value();
                    }
                    std::string table = schema_ptr->getTableName();
                    table = reinterpret_cast<ScopedFieldNode*>(item)->table_->token_.val_;
                    std::string col = table;col += "."; col+= field;
                    idx = schema_ptr->colExist(col);

                    if(idx < 0 && cur_query_parent != -1){
                        Executor* parent_query = ctx_.executors_call_stack_[cur_query_parent]; 
                        schema_ptr = parent_query->output_schema_;
                        cur_query_idx = parent_query->query_idx_;
                        cur_query_parent = ctx_.executors_call_stack_[cur_query_idx]->parent_query_idx_;
                        continue;
                    }
                    Executor* cur_exec = ctx_.executors_call_stack_[cur_query_idx];

                    while(cur_exec && cur_exec->type_ != SEQUENTIAL_SCAN_EXECUTOR){
                        cur_exec = cur_exec->child_executor_;
                    }
                    if(!cur_exec){
                        std::cout << "[ERROR] Invalid scoped filter operation"<< std::endl;
                        error_status_ = 1;
                        return Value();
                    }
                    std::vector<Value> cur_output  = cur_exec->output_;

                    if(idx < 0 || idx >= cur_output.size()) {
                        std::cout << "[ERROR] Invalid scoped field name for filtering " << col << std::endl;
                        error_status_ = 1;
                        return Value();
                    }
                    return cur_output[idx];
                 }
            } else {
                TableSchema* schema_ptr = output_schema_;
                int cur_query_idx = query_idx_;
                int cur_query_parent = parent_query_idx_;
                int idx = -1;
                while(true){
                    if(!schema_ptr && cur_query_parent != -1){
                        Executor* parent_query = ctx_.executors_call_stack_[cur_query_parent]; 
                        schema_ptr = parent_query->output_schema_;
                        cur_query_idx = parent_query->query_idx_;
                        cur_query_parent = parent_query->parent_query_idx_;
                        continue;
                    } else if(!schema_ptr){
                        std::cout << "[ERROR] Cant access field name without schema " << field << std::endl;
                        error_status_ = 1;
                        return Value();
                    }
                    int num_of_matches = 0;
                    auto columns = schema_ptr->getColumns();
                    for(size_t i = 0; i < columns.size(); ++i){
                        std::vector<std::string> splittedStr = strSplit(columns[i].getName(), '.');
                        if(splittedStr.size() != 2) {
                            std::cout << "[ERROR] Invalid schema " << std::endl;
                            error_status_ = 1;
                            return Value();
                        }
                        if(field == splittedStr[1]){
                            num_of_matches++;
                            idx = i;
                        }
                    }
                    if(num_of_matches > 1){
                        std::cout << "[ERROR] Ambiguous field name: " << field << std::endl;
                        error_status_ = 1;
                        return Value();
                    }
                    // if it doesn't match any fields check for renames.
                    if(idx < 0){
                        for(int i = 0; i < field_names_.size(); i++){
                            if(field == field_names_[i]) 
                                return evaluate_expression(fields_[i], eval);
                        }
                    }
                    // can't find the field in current context,
                    // search for it in context of the parent till the top level query.
                    if(num_of_matches == 0 && cur_query_parent != -1){
                        Executor* parent_query = ctx_.executors_call_stack_[cur_query_parent]; 
                        schema_ptr = parent_query->output_schema_;
                        cur_query_idx = parent_query->query_idx_;
                        cur_query_parent = ctx_.executors_call_stack_[cur_query_idx]->parent_query_idx_;
                        continue;
                    }
                    Executor* cur_exec = ctx_.executors_call_stack_[cur_query_idx];

                    while(cur_exec && cur_exec->type_ != FILTER_EXECUTOR){
                        cur_exec = cur_exec->child_executor_;
                    }
                    if(!cur_exec){
                        std::cout << "[ERROR] Invalid filter operation"<< std::endl;
                        error_status_ = 1;
                        return Value();
                    }

                    std::vector<Value> cur_output  = cur_exec->output_;

                    if(idx < 0 || idx >= cur_output.size()) {
                        std::string prefix = AGG_FUNC_IDENTIFIER_PREFIX;
                        if(field.rfind(prefix, 0) == 0)
                            std::cout << "[ERROR] aggregate functions should not be used in here"<< std::endl;
                        else 
                            std::cout << "[ERROR] Invalid field name for filter " << field << std::endl;
                        error_status_ = 1;
                        return Value();
                    }
                    return cur_output[idx];
                }
            }
        } 

        void init() {
            error_status_ = 0;
            finished_ = 0;
            output_ = {};
            if(child_executor_) {
                child_executor_->init();
            }
        }


        std::vector<Value> next() {
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

                Value exp = evaluate_expression(filter_, eval).getBoolVal();
                if(exp != false && !exp.isNull()){
                    if(child_executor_)
                        return output_;
                    return {exp};
                }
            }
        }
    private:
        ExpressionNode* filter_ = nullptr;
        std::vector<ExpressionNode*> fields_ = {};
        std::vector<std::string> field_names_ = {};
        std::function<Value(ASTNode*)>
            eval = std::bind(&FilterExecutor::evaluate, this, std::placeholders::_1);
};

class AggregationExecutor : public Executor {
    public:
        AggregationExecutor(Executor* child_executor, TableSchema* output_schema, 
                std::vector<AggregateFuncNode*> aggregates, std::vector<ASTNode*> group_by, QueryCTX& ctx, int query_idx, int parent_query_idx): 
                
            Executor(AGGREGATION_EXECUTOR, output_schema, ctx, query_idx, parent_query_idx, child_executor), aggregates_(aggregates), group_by_(group_by)
        {}
        ~AggregationExecutor()
        {}

        Value evaluate(ASTNode* item){
            if(item->category_ != FIELD && item->category_ != SCOPED_FIELD){
                std::cout << "[ERROR] Item type is not supported!" << std::endl;
                error_status_ = 1;
                return Value();
            }

            std::string field = item->token_.val_;
            if(!output_schema_) {
                std::cout << "[ERROR] Cant access field name without schema " << field << std::endl;
                error_status_ = 1;
                return Value();
            }
            int idx = -1;
            if(item->category_ == SCOPED_FIELD){
                std::string table = output_schema_->getTableName();
                table = reinterpret_cast<ScopedFieldNode*>(item)->table_->token_.val_;
                std::string col = table;col += "."; col+= field;
                idx = output_schema_->colExist(col);
                //    output_schema_->printTableHeader();
                if(idx < 0 || idx >= output_.size()) {
                    std::cout << "[ERROR] Invalid field name " << col << std::endl;
                    error_status_ = 1;
                    return Value();
                }
            } else {
                int num_of_matches = 0;
                auto columns = output_schema_->getColumns();
                for(size_t i = 0; i < columns.size(); ++i){
                    std::vector<std::string> splittedStr = strSplit(columns[i].getName(), '.');
                    if(splittedStr.size() != 2) {
                        //output_schema_->printTableHeader();
                        std::cout << "[ERROR] Invalid schema " << std::endl;
                        error_status_ = 1;
                        return Value();
                    }
                    if(field == splittedStr[1]){
                        num_of_matches++;
                        idx = i;
                    }
                }
                if(num_of_matches > 1){
                    std::cout << "[ERROR] Ambiguous field name: " << field << std::endl;
                    error_status_ = 1;
                    return Value();
                }
                if(idx < 0 || idx >= output_.size()) {
                    std::cout << "[ERROR] Invalid field name " << field << std::endl;
                    //output_schema_->printTableHeader();
                    error_status_ = 1;
                    return Value();
                }
            }
            return output_[idx];
        } 
        void init() {
            finished_ = 0;
            error_status_ = 0;
            aggregated_values_.clear();


            if(child_executor_){
                child_executor_->init();
            }
            // build the search key for the hash table.
            std::string hash_key = "PREFIX_"; // this prefix to ensure we have at least one entry in the hash table.
            for(int i = 0; i < group_by_.size(); i++){
                Value cur = evaluate(group_by_[i]);
                hash_key += cur.toString();
            }

            // we always maintain rows count even if the user did not ask for it, that's why the size is | colmuns | + 1
            output_ = std::vector<Value> (output_schema_->getCols().size() + 1, Value(0));
            aggregated_values_[hash_key] = output_;

            while(true){
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



                // weather the key exists or not we'll just insert the new output vector or an updated one.

                if(aggregated_values_.count(hash_key)){
                    output_ = aggregated_values_[hash_key];
                }

                for(int i = 0; i < child_output.size(); i++){
                    output_[i] = child_output[i];
                }
                // update the extra counter.
                output_[output_.size() - 1] += 1; 
                Value* counter = &output_[output_.size() - 1];

                int base_size = child_output.size();
                for(int i = 0; i < aggregates_.size(); i++){
                    ExpressionNode* exp = aggregates_[i]->exp_;
                    int idx = base_size+i;
                    switch(aggregates_[i]->type_){
                        case COUNT:
                                    {
                                        if(exp == nullptr){
                                            ++output_[idx];
                                            break;
                                        }
                                        Value val = evaluate_expression(exp, eval);
                                        if(!val.isNull()) 
                                            ++output_[idx];
                                    }
                                    break;
                        case AVG:
                        case SUM:
                                   {
                                       Value val = evaluate_expression(exp, eval);
                                       if(val.type_ == INT) output_[idx] += val; 
                                   }
                                   break;
                        case MIN:
                                   {
                                       Value val = evaluate_expression(exp, eval);
                                       if(val.type_ == INT) {
                                           if(counter->getIntVal() == 1) output_[idx] = val;
                                           output_[idx] = std::min<Value>(output_[idx], val);
                                       }
                                   }
                                   break;
                        case MAX:
                                   {
                                       Value val = evaluate_expression(exp, eval);
                                       if(val.type_ == INT) {
                                           if(counter->getIntVal() == 1) output_[idx] = val;
                                           output_[idx] = std::max<Value>(output_[i], val);
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
            it_ = aggregated_values_.begin();
        }

        std::vector<Value> next() {
            if(error_status_ || finished_)  return {};
            output_ = it_->second;
            for(int i = 0; i < aggregates_.size(); i++){
                int idx = (i + output_.size() - aggregates_.size())- 1;
                if(aggregates_[i]->type_ == AVG && output_[output_.size()-1] != 0){
                    output_[idx] = Value( (float)output_[idx].getIntVal()/(float) output_[output_.size()-1].getIntVal());
                }
            }
            ++it_;
            if(it_== aggregated_values_.end())
                finished_ = true;
            return output_;
        }
    private:
        std::vector<AggregateFuncNode*> aggregates_;
        std::vector<ASTNode*> group_by_;
        std::unordered_map<std::string, std::vector<Value>> aggregated_values_;
        std::unordered_map<std::string, std::vector<Value>>::iterator it_;
        std::function<Value(ASTNode*)>
            eval = std::bind(&AggregationExecutor::evaluate, this, std::placeholders::_1);
};

class ProjectionExecutor : public Executor {
    public:
        ProjectionExecutor(Executor* child_executor, TableSchema* output_schema, std::vector<ExpressionNode*> fields, QueryCTX& ctx, int query_idx, int parent_query_idx): 
            Executor(PROJECTION_EXECUTOR, output_schema, ctx, query_idx, parent_query_idx, child_executor), fields_(fields)
        {}
        ~ProjectionExecutor()
        {}

        Value evaluate(ASTNode* item){
            if(item->category_ == SUB_QUERY){
                auto sub_query = reinterpret_cast<SubQueryNode*>(item);
                Executor* sub_query_executor = ctx_.executors_call_stack_[sub_query->idx_]; 
                sub_query_executor->init();
                if(sub_query_executor->error_status_){
                    std::cout << "[ERROR] could not initialize sub-query" << std::endl;
                    error_status_ = 1;
                    return Value();
                }
                std::vector<Value> tmp = sub_query_executor->next();
                if(tmp.size() == 0 && sub_query_executor->finished_) {
                    return Value();
                }

                if(tmp.size() == 0 || sub_query_executor->error_status_) {
                    std::cout << "[ERROR] could not execute sub-query" << std::endl;
                    error_status_ = 1;
                    return Value();
                }
                if(tmp.size() != 1) {
                    std::cout << "[ERROR] sub-query should return exactly 1 column" << std::endl;
                    error_status_ = 1;
                    return Value();
                }
                return tmp[0];
            }

            if(item->category_ != FIELD && item->category_ != SCOPED_FIELD){
                std::cout << "[ERROR] Item type is not supported!" << std::endl;
                error_status_ = 1;
                return Value();
            }

            std::string field = item->token_.val_;


            if(item->category_ == SCOPED_FIELD){
                TableSchema* schema_ptr = output_schema_;
                int cur_query_idx = query_idx_;
                int cur_query_parent = parent_query_idx_;
                int idx = -1;

                while(true){
                    if(!schema_ptr && cur_query_parent != -1){
                        Executor* parent_query = ctx_.executors_call_stack_[cur_query_parent]; 
                        schema_ptr = parent_query->output_schema_;
                        cur_query_idx = parent_query->query_idx_;
                        cur_query_parent = parent_query->parent_query_idx_;
                        continue;
                    } else if(!schema_ptr){
                        std::cout << "[ERROR] Cant access field name without schema " << field << std::endl;
                        error_status_ = 1;
                        return Value();
                    }
                    std::string table = schema_ptr->getTableName();
                    table = reinterpret_cast<ScopedFieldNode*>(item)->table_->token_.val_;
                    std::string col = table;col += "."; col+= field;
                    idx = schema_ptr->colExist(col);
                    if(idx < 0 && cur_query_parent != -1){
                        Executor* parent_query = ctx_.executors_call_stack_[cur_query_parent]; 
                        schema_ptr = parent_query->output_schema_;
                        cur_query_idx = parent_query->query_idx_;
                        cur_query_parent = ctx_.executors_call_stack_[cur_query_idx]->parent_query_idx_;
                        continue;
                    }
                    std::vector<Value> cur_output;
                    if(query_idx_ == 0)
                        cur_output = output_;
                    else
                        cur_output = ctx_.executors_call_stack_[cur_query_idx]->output_; 
                    if(idx < 0 || idx >= cur_output.size()) {
                        std::cout << "[ERROR] Invalid field name " << col << std::endl;
                        error_status_ = 1;
                        return Value();
                    }
                    return cur_output[idx];
                }

            } else {
                TableSchema* schema_ptr = output_schema_;
                int cur_query_idx = query_idx_;
                int cur_query_parent = parent_query_idx_;
                int idx = -1;
                while(true){
                    if(!schema_ptr && cur_query_parent != -1){
                        Executor* parent_query = ctx_.executors_call_stack_[cur_query_parent]; 
                        schema_ptr = parent_query->output_schema_;
                        cur_query_idx = parent_query->query_idx_;
                        cur_query_parent = parent_query->parent_query_idx_;
                        continue;
                    } else if(!schema_ptr){
                        std::cout << "[ERROR] Cant access field name without schema " << field << std::endl;
                        error_status_ = 1;
                        return Value();
                    }
                    int num_of_matches = 0;
                    auto columns = schema_ptr->getColumns();
                    for(size_t i = 0; i < columns.size(); ++i){
                        std::vector<std::string> splittedStr = strSplit(columns[i].getName(), '.');
                        if(splittedStr.size() != 2) {
                            //output_schema_->printTableHeader();
                            std::cout << "[ERROR] Invalid schema " << std::endl;
                            error_status_ = 1;
                            return Value();
                        }
                        if(field == splittedStr[1]){
                            num_of_matches++;
                            idx = i;
                        }
                    }
                    if(num_of_matches > 1){
                        std::cout << "[ERROR] Ambiguous field name: " << field << std::endl;
                        error_status_ = 1;
                        return Value();
                    }
                    // can't find the field in current context,
                    // search for it in context of the parent till the top level query.
                    if(num_of_matches == 0 && cur_query_parent != -1){
                        Executor* parent_query = ctx_.executors_call_stack_[cur_query_parent]; 
                        schema_ptr = parent_query->output_schema_;
                        cur_query_idx = parent_query->query_idx_;
                        cur_query_parent = ctx_.executors_call_stack_[cur_query_idx]->parent_query_idx_;
                        continue;
                    }
                    std::vector<Value> cur_output;
                    if(query_idx_ == 0)
                        cur_output = output_;
                    else
                        cur_output = ctx_.executors_call_stack_[cur_query_idx]->output_; 

                    if(idx < 0 || idx >= cur_output.size()) {
                        std::cout << "[ERROR] Invalid field name " << field << std::endl;
                        error_status_ = 1;
                        return Value();
                    }

                    return cur_output[idx];
                }
            }
        } 

        void init() {
            finished_ = 0;
            error_status_ = 0;
            output_ = {};
            if(child_executor_) {
                child_executor_->init();
            }
        }

        std::vector<Value> next() {
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
                    tmp_output.push_back(evaluate_expression(fields_[i], eval));
                }
            }
            return tmp_output;
        }
    private:
        // child_executor_ is optional in case of projection for example : select 1 + 1 should work without a from clause.
        std::vector<ExpressionNode*> fields_ {};
        std::function<Value(ASTNode*)>
            eval = std::bind(&ProjectionExecutor::evaluate, this, std::placeholders::_1);
};

class SortExecutor : public Executor {
    public:
        SortExecutor(Executor* child_executor , TableSchema* output_schema, std::vector<int> order_by_list, QueryCTX& ctx, int query_idx, int parent_query_idx): 
            Executor(SORT_EXECUTOR, output_schema, ctx, query_idx, parent_query_idx, child_executor), order_by_list_(order_by_list)
        {}
        ~SortExecutor()
        {}

        void init() {
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
                            if(lhs[order_by[i]] != rhs[order_by[i]]) {
                                return lhs[order_by[i]] < rhs[order_by[i]];
                            }
                        }
                        return false;
                    });
        }

        std::vector<Value> next() {
            finished_ = (idx_ >= tuples_.size());
            if(error_status_ || finished_)  return {};
            output_ = tuples_[idx_];
            return tuples_[idx_++];
        }
    private:
        std::vector<int> order_by_list_;
        std::vector<std::vector<Value>> tuples_;
        int idx_ = 0;
};

class DistinctExecutor : public Executor {
    public:
        DistinctExecutor(Executor* child_executor , TableSchema* output_schema, QueryCTX& ctx, int query_idx, int parent_query_idx): 
            Executor(DISTINCT_EXECUTOR, output_schema, ctx, query_idx, parent_query_idx, child_executor)
        {}
        ~DistinctExecutor()
        {}

        void init() {
            finished_ = 0;
            error_status_ = 0;
            child_executor_->init();
        }

        std::vector<Value> next() {
            if(error_status_ || finished_)  return {};
            while(true){
                std::vector<Value> tuple = child_executor_->next();
                finished_ = child_executor_->finished_;
                error_status_ = child_executor_->error_status_;
                if(finished_ || error_status_ || tuple.size() == 0) return {};
                std::string stringified_tuple = "";
                for(size_t i = 0; i < tuple.size(); i++) stringified_tuple += tuple[i].toString();
                if(hashed_tuples_.count(stringified_tuple)) continue; // duplicated tuple => skip it.
                hashed_tuples_[stringified_tuple] =  1;
                output_ = tuple;
                return output_;
            }
        }

    private:
        std::unordered_map<std::string, int> hashed_tuples_;
};



class ExecutionEngine {
    public:
        ExecutionEngine(Catalog* catalog): catalog_(catalog)
    {}
        ~ExecutionEngine() {}

        // DDL handlers.
        bool create_table_handler(QueryCTX& ctx) {
            CreateTableStatementData* create_table = reinterpret_cast<CreateTableStatementData*>(ctx.queries_call_stack_[0]);
            std::string table_name = create_table->table_name_;
            std::vector<FieldDef> fields = create_table->field_defs_;
            std::deque<std::string> col_names;
            std::deque<Type> col_types;
            for(int i = 0; i < fields.size(); ++i){
                // a little too much nesting (fix that later).
                std::string name = fields[i].field_name_;
                Type type = catalog_->tokenTypeToColType(fields[i].type_);
                if(type == INVALID) {
                    std::cout << "[ERROR] Invalid type\n";
                    return false;
                }
                // variable columns first;
                if(type == VARCHAR) {
                    col_names.push_front(name);
                    col_types.push_front(type);
                } else {
                    col_names.push_back(name);
                    col_types.push_back(type);
                }
            }
            std::vector<Column> columns;
            uint8_t offset_ptr = 0;
            for(size_t i = 0; i < col_names.size(); ++i){
                // assume no constraints for now.
                columns.push_back(Column(col_names[i], col_types[i], offset_ptr));
                offset_ptr += Column::getSizeFromType(col_types[i]);
            }
            TableSchema* sch = catalog_->createTable(table_name, columns);
            if(sch != nullptr) return true;
            return false;
        }

        /*
        bool insert_handler(ASTNode* statement_root){
            InsertStatementNode* insert = reinterpret_cast<InsertStatementNode*>(statement_root);
            // We are going to assume that there are no default values for now.
            // We also assume that the order of fields matches the schema.

            std::string table_name = insert->table_->token_.val_;
            TableSchema* schema = catalog_->getTableSchema(table_name);
            auto field_ptr = insert->fields_;
            auto value_ptr = insert->values_;
            std::vector<std::string> fields;
            std::vector<Value> vals(schema->numOfCols());
            // loop through fields first
            // if there are no fields use the default schema.
            if(!field_ptr){
                fields = schema->getCols();
            }
            while(field_ptr != nullptr){
                std::string field_name = field_ptr->field_->token_.val_;
                // check valid column.
                if(!schema->isValidCol(field_name)) 
                    return false;
                int idx = schema->colExist(field_name);
                if(idx  < 0) return false;
                fields.push_back(field_name);
                field_ptr = field_ptr->next_;
            }

            int fields_index = 0;
            // loop through vals.
            while(value_ptr != nullptr){
                std::string val = value_ptr->token_.val_;
                // we consider int and string types for now.
                Type val_type = INVALID;
                if(value_ptr->category_ == STRING_CONSTANT) val_type = VARCHAR;
                else if(value_ptr->category_ == INTEGER_CONSTANT) val_type = INT;
                // invalid or not supported type;
                if( val_type == INVALID ) return false;

                if(fields_index > fields.size()){
                    std::cout << "[ERROR] Size of values does not match size of fields" << std::endl;
                    return false;
                }
                int idx = schema->colExist(fields[fields_index++]);
                if(idx  < 0) {
                    std::cout << "[ERROR] field does not exist: " << fields[fields_index] << std::endl;
                    return false;
                }
                if(val_type == INT) vals[idx] = (Value(stoi(val)));
                else if(val_type == VARCHAR) vals[idx] = (Value(val));

                value_ptr = value_ptr->next_;
            }

            // invalid field list or val list.
            if(!schema->checkValidValues(fields, vals)) {
                std::cout << "[ERROR] Invalid field list or val list" << std::endl;
                return false;
            }

            Record* record = schema->translateToRecord(vals);
            // rid is not used for now.
            RecordID* rid = new RecordID();
            int err = schema->getTable()->insertRecord(rid, *record);
            if(!err) return true;
            return false;
        }*/

        /*
        bool delete_handler(ASTNode* statement_root){
            DeleteStatementNode* delete_statement = reinterpret_cast<DeleteStatementNode*>(statement_root);

            auto table_ptr = delete_statement->table_; 
            // did not find any tables.
            if(table_ptr == nullptr) return false;
            std::string table_name = table_ptr->token_.val_;
            TableSchema* schema = catalog_->getTableSchema(table_name);

            TableIterator* it = schema->getTable()->begin();
            while(it->advance()){
                RecordID rid = it->getCurRecordID();
                schema->getTable()->deleteRecord(rid);
            }
            // handle filters later.
            
            return true;
        }

        bool update_handler(ASTNode* statement_root){
            UpdateStatementNode* update_statement = reinterpret_cast<UpdateStatementNode*>(statement_root);

            std::string table_name = update_statement->table_->token_.val_;
            TableSchema* schema = catalog_->getTableSchema(table_name);
            // did not find any tables with that name.
            if(schema == nullptr) return false;
            auto field_ptr = update_statement->field_;
            std::string field_name = field_ptr->token_.val_;
            auto val_ptr = update_statement->expression_;
            std::string val_str = val_ptr->token_.val_;
            // check valid column.
            if(!schema->isValidCol(field_name)) 
                return false;
            // we consider int and string types for now.
            Type val_type = INVALID;
            if(val_ptr->category_ == STRING_CONSTANT) val_type = VARCHAR;
            else if(val_ptr->category_ == INTEGER_CONSTANT) val_type = INT;
            // invalid or not supported type;
            if( val_type == INVALID ) return false;
            Value val;
            if(val_type == INT) val = Value(stoi(val_str));
            else if(val_type == VARCHAR) val = Value(val);

            if(!schema->checkValidValue(field_name, val)) return false;


            TableIterator* it = schema->getTable()->begin();
            while(it->advance()){
                RecordID rid = it->getCurRecordID();
                // rid is not used for now.
                Record cpy = it->getCurRecordCpy();
                std::vector<Value> values;
                int err = schema->translateToValues(cpy, values);
                int idx = schema->getColIdx(field_name, val);
                if(idx < 0) return false;
                values[idx] = val;
                Record* new_rec = schema->translateToRecord(values);

                err = schema->getTable()->updateRecord(&rid, *new_rec);
                if(err) return false;
            }
            return true;
            // handle filters later.
        }
        */

        // DDL execution.
        bool directExecute(QueryCTX& ctx){
            // should always be 1.
            if(ctx.queries_call_stack_.size() != 1) return false;
            std::cout << "[INFO] executing DDL command" << std::endl;
            QueryType type = ctx.queries_call_stack_[0]->type_;
            switch (type) {
                case CREATE_TABLE_DATA:
                    return create_table_handler(ctx);
                default:
                    return false;
            }
        }

        // DML execution.
        bool executePlan(QueryCTX& ctx, QueryResult* result){
            if(ctx.queries_call_stack_.size() < 1 || !result) return false;
            std::cout << "[INFO] Creating physical plan" << std::endl;

            for(auto cur_plan : ctx.operators_call_stack_){
                Executor* created_physical_plan = buildExecutionPlan(ctx, cur_plan, cur_plan->query_idx_, cur_plan->query_parent_idx_);
                if(!created_physical_plan){
                    std::cout << "[ERROR] Could not build physical operation\n";
                    return false;
                }
                if(cur_plan->distinct_){
                    DistinctExecutor* distinct = new DistinctExecutor(created_physical_plan, created_physical_plan->output_schema_, ctx, cur_plan->query_idx_, cur_plan->query_parent_idx_);
                    ctx.executors_call_stack_.push_back(distinct);
                } else 
                    ctx.executors_call_stack_.push_back(created_physical_plan);
            }

            std::cout << "[INFO] executing physical plan" << std::endl;
            return runExecutor(ctx, result);
        }

    private:

        Executor* buildExecutionPlan(QueryCTX& ctx, AlgebraOperation* logical_plan, int query_idx, int parent_query_idx) {
            if(!logical_plan) return nullptr;
            switch(logical_plan->type_) {
                case SORT: 
                    {
                        SortOperation* op = reinterpret_cast<SortOperation*>(logical_plan);
                        Executor* child = buildExecutionPlan(ctx, op->child_, query_idx, parent_query_idx);
                        SortExecutor* sort = new SortExecutor(child, child->output_schema_, op->order_by_list_, ctx, query_idx, parent_query_idx);
                        return sort;
                    } break;
                case PROJECTION: 
                    {
                        ProjectionOperation* op = reinterpret_cast<ProjectionOperation*>(logical_plan);
                        Executor* child = buildExecutionPlan(ctx, op->child_, query_idx, parent_query_idx);

                        ProjectionExecutor* project = nullptr; 
                        if(!child)
                            project = new ProjectionExecutor(child, nullptr, op->fields_, ctx, query_idx, parent_query_idx);
                        else
                            project = new ProjectionExecutor(child, child->output_schema_, op->fields_, ctx, query_idx, parent_query_idx);
                        return project;
                    } break;
                case AGGREGATION: 
                    {
                        AggregationOperation* op = reinterpret_cast<AggregationOperation*>(logical_plan);
                        Executor* child = buildExecutionPlan(ctx, op->child_, query_idx, parent_query_idx);

                        std::vector<Column> new_cols;
                        int offset_ptr = 0; 
                        if(child && child->output_schema_){
                            new_cols = child->output_schema_->getColumns();
                            offset_ptr = Column::getSizeFromType(new_cols[new_cols.size() - 1].getType());
                        } 
                        for(int i = 0; i < op->aggregates_.size(); i++){
                            std::string col_name = "agg_tmp_schema.";
                            col_name += AGG_FUNC_IDENTIFIER_PREFIX;
                            col_name += intToStr(op->aggregates_[i]->parent_id_);
                            new_cols.push_back(Column(col_name, INT, offset_ptr));
                            offset_ptr += Column::getSizeFromType(INT);
                        }
                        TableSchema* new_output_schema = new TableSchema("agg_tmp_schema", nullptr, new_cols);

                        return new AggregationExecutor(child, new_output_schema, op->aggregates_, op->group_by_, ctx, query_idx, parent_query_idx);
                    } break;
                case FILTER: 
                    {
                        FilterOperation* op = reinterpret_cast<FilterOperation*>(logical_plan);
                        Executor* child = buildExecutionPlan(ctx, op->child_, query_idx, parent_query_idx);
                        TableSchema* schema = nullptr;
                        if(child == nullptr) schema = nullptr;
                        else                 schema = child->output_schema_;

                        FilterExecutor* filter = new FilterExecutor(child, schema, op->filter_, op->fields_, op->field_names_, ctx, query_idx, parent_query_idx);
                        return filter;
                    } break;
                case SCAN: 
                    {
                        ScanOperation* op = reinterpret_cast<ScanOperation*>(logical_plan);
                        TableSchema* schema = catalog_->getTableSchema(op->table_name_);
                        std::string tname =  op->table_name_;
                        if(op->table_rename_.size() != 0) tname = op->table_rename_;
                        std::vector<Column> columns = schema->getColumns();
                        // create a new schema and rename columns to table.col_name
                        for(int i = 0; i < columns.size(); i++){
                            std::string col_name = tname; 
                            col_name += ".";
                            col_name += columns[i].getName();
                            columns[i].setName(col_name);
                        }

                        TableSchema* new_output_schema = new TableSchema(tname, schema->getTable(), columns);
                        SeqScanExecutor* scan = new SeqScanExecutor(new_output_schema, ctx, query_idx, parent_query_idx);
                        return scan;
                    } break;
                case INSERTION: 
                    {
                        auto statement = reinterpret_cast<InsertStatementData*>(ctx.queries_call_stack_[query_idx]);
                        TableSchema* table = catalog_->getTableSchema(statement->table_name_);

                        InsertionExecutor* insert = new InsertionExecutor(table, ctx, query_idx, parent_query_idx);
                        return insert;
                    } break;
                default: 
                    std::cout << "[ERROR] unsupported Algebra Operaion\n";
                    return nullptr;
            }
        }
        bool runExecutor(QueryCTX& ctx, QueryResult* result){
            if(ctx.executors_call_stack_.size() < 1 || (bool) ctx.error_status_ ) return false;
            auto physical_plan = ctx.executors_call_stack_[0];
            physical_plan->init();
            while(!physical_plan->error_status_ && !physical_plan->finished_){
                std::vector<Value> tmp = physical_plan->next();
                if(tmp.size() == 0 || physical_plan->error_status_) break;
                    
                result->push_back(tmp);
            }
            return (physical_plan->error_status_ == 0);
        }
        Catalog* catalog_;
};
