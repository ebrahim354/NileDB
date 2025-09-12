#pragma once
#include "catalog.cpp"
#include "executor.cpp"
#include "parser.cpp"
#include "expression.cpp"
#include "algebra_engine.cpp"
#include "utils.cpp"
#include <deque>


typedef std::vector<std::vector<Value>> QueryResult;
struct IndexHeader;


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



class ProductExecutor : public Executor {
    public:
        ProductExecutor(TableSchema* output_schema, QueryCTX& ctx, int query_idx, int parent_query_idx, Executor* lhs, Executor* rhs)
            : Executor(PRODUCT_EXECUTOR, output_schema, ctx, query_idx, parent_query_idx, lhs), left_child_(lhs), right_child_(rhs)
        {}
        ~ProductExecutor()
        {
            delete left_child_;  
            delete right_child_;
            delete output_schema_;
        }

        void init() {
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
                for(int i = 0; i < left_output.size(); ++i)
                    output_[i] = left_output[i];
            }
        }

        std::vector<Value> next() {
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
            if(right_output.size() == 0) return {};

            for(int i = 0; i < left_output.size(); ++i)
                output_[i] = left_output[i];
            for(int i = 0;i < right_output.size(); ++i)
                output_[i+(output_.size() - right_output.size())] = right_output[i];

            finished_ = left_child_->finished_;
            return output_;
        }
    private:
        Executor* left_child_ = nullptr;
        Executor* right_child_ = nullptr;
};

class JoinExecutor : public Executor {
    public:
        JoinExecutor(TableSchema* output_schema, QueryCTX& ctx, int query_idx, int parent_query_idx, Executor* lhs, Executor* rhs, ExpressionNode* filter)
            : Executor(JOIN_EXECUTOR, output_schema, ctx, query_idx, parent_query_idx, lhs), left_child_(lhs), right_child_(rhs), filter_(filter)
        {}
        ~JoinExecutor()
        {
            delete left_child_;  
            delete right_child_;
            delete output_schema_;
        }

        void init() {
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
            right_child_fields_.clear();
            left_child_fields_.clear();
            // find out which attributes to use as keys for the hash table.
            std::vector<std::string> fields;
            accessed_fields(filter_, fields);

            for(int i = 0; i < fields.size(); ++i){
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
            // should at least have one field on each side or the join does not make any sense.
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
              if(hashed_left_child_.count(key)) hashed_left_child_[key].push_back(left_output);
              else hashed_left_child_[key] = {left_output};
            }
        }

        std::vector<Value> next() {
            if(error_status_ || finished_)  return {};

            if(duplicated_idx_ != -1){
              duplicated_idx_++;
              // no need to change the right output.
              for(int i = 0; i < hashed_left_child_[prev_key_][duplicated_idx_].size(); ++i){
                output_[i] = hashed_left_child_[prev_key_][duplicated_idx_][i];
              }
              if(duplicated_idx_+1 >= hashed_left_child_[prev_key_].size())
                duplicated_idx_ = -1;
              finished_ = right_child_->finished_ && duplicated_idx_ == -1;
              return output_;
            }

            while(true){
              std::vector<Value> right_output = right_child_->next();
              if(right_output.size() == 0) {
                finished_ = true;
                return {};
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
              if(!hashed_left_child_.count(key))
                continue;

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

            finished_ = right_child_->finished_ && duplicated_idx_ == -1;
            return output_;
        }
    private:
        Executor* left_child_ = nullptr;
        Executor* right_child_ = nullptr;
        std::vector<int> left_child_fields_;
        // duplicated_idx tracks last used hashed value in case of hashing on non unique keys.
        // for example: the hash key is the field 'a' and this field is not unique and may have a duplicated value of 1,
        // in that case when a join happens we store it in the hash table as 1 -> {tuple 1, tuple 2, tuple 3}.
        // when we call next() on a a key such as the previous example we track the last tuple that has been returned.
        // the value of -1 means that we will use the first tuple.
        int duplicated_idx_ = -1; 
        std::string prev_key_ = "";
        std::vector<int> right_child_fields_;
        ExpressionNode* filter_ = nullptr;
        std::unordered_map<std::string, std::vector<std::vector<Value>>> hashed_left_child_;
};

class UnionExecutor : public Executor {
    public:
        UnionExecutor(TableSchema* output_schema, QueryCTX& ctx, int query_idx, int parent_query_idx, Executor* lhs, Executor* rhs)
            : Executor(UNION_EXECUTOR, output_schema, ctx, query_idx, parent_query_idx, lhs), left_child_(lhs), right_child_(rhs)
        {}
        // doesn't own its children, so no cleaning needed.
        ~UnionExecutor()
        {}

        void init() {
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

        std::vector<Value> next() {
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
    private:
        Executor* left_child_ = nullptr;
        Executor* right_child_ = nullptr;
};

class ExceptExecutor : public Executor {
    public:
        ExceptExecutor(TableSchema* output_schema, QueryCTX& ctx, int query_idx, int parent_query_idx, Executor* lhs, Executor* rhs)
            : Executor(EXCEPT_EXECUTOR, output_schema, ctx, query_idx, parent_query_idx, lhs), left_child_(lhs), right_child_(rhs)
        {}
        // doesn't own its children, so no cleaning needed.
        ~ExceptExecutor()
        {}

        void init() {
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

        std::vector<Value> next() {
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
    private:
        Executor* left_child_ = nullptr;
        Executor* right_child_ = nullptr;
        std::unordered_map<std::string, int> hashed_tuples_;
};

class IntersectExecutor : public Executor {
    public:
        IntersectExecutor(TableSchema* output_schema, QueryCTX& ctx, int query_idx, int parent_query_idx, Executor* lhs, Executor* rhs)
            : Executor(INTERSECT_EXECUTOR, output_schema, ctx, query_idx, parent_query_idx, lhs), left_child_(lhs), right_child_(rhs)
        {}
        // doesn't own its children, so no cleaning needed.
        ~IntersectExecutor()
        {}

        void init() {
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

        std::vector<Value> next() {
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
    private:
        Executor* left_child_ = nullptr;
        Executor* right_child_ = nullptr;
        std::unordered_map<std::string, int> hashed_tuples_;
};



class SeqScanExecutor : public Executor {
    public:
        SeqScanExecutor(TableSchema* table, QueryCTX& ctx, int query_idx, int parent_query_idx)
            : Executor(SEQUENTIAL_SCAN_EXECUTOR, table, ctx, query_idx, parent_query_idx, nullptr), table_(table)
        {}
        ~SeqScanExecutor()
        {
            delete it_;
            delete table_;
        }

        void init() {
            error_status_ = 0;
            finished_ = 0;
            output_.resize(output_schema_->numOfCols());
            delete it_;
            it_ = table_->getTable()->begin();
        }

        std::vector<Value> next() {
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
    private:
        TableSchema* table_ = nullptr;
        TableIterator* it_ = nullptr;
};

class IndexScanExecutor : public Executor {
    public:
        // TODO: change BTreeIndex type to be a generic ( just Index ) that might be a btree or hash index.
        IndexScanExecutor(BTreeIndex* index, TableSchema* table, QueryCTX& ctx, int query_idx, int parent_query_idx)
            : Executor(INDEX_SCAN_EXECUTOR, table, ctx, query_idx, parent_query_idx, nullptr), 
            table_(table), 
            index_(index),
            it_ (index_->begin())
        {}
        ~IndexScanExecutor()
        {
            delete table_;
        }

        void init() {
            error_status_ = 0;
            finished_ = 0;
            output_.resize(output_schema_->numOfCols());
            //  delete it_;
            //  it_ = index_->begin();
        }

        std::vector<Value> next() {
            // no more records.
            if(!it_.advance()) {
                finished_ = 1;
                return {};
            };
            Record* r = it_.getCurRecordCpyPtr();
            if(!r){
                error_status_ = 1;
                return {};
            }
            int err = table_->translateToValuesOffset(*r, output_, 0);
            delete r;
            if(err) {
                error_status_ = 1;
                return {};
            }
            return output_;
        }
    private:
        TableSchema* table_ = nullptr;
        BTreeIndex* index_ = nullptr;
        IndexIterator it_;
};

class InsertionExecutor : public Executor {
    public:
        InsertionExecutor(TableSchema* table, std::vector<IndexHeader> indexes, QueryCTX& ctx, int query_idx, int parent_query_idx, int select_idx)
            : Executor(INSERTION_EXECUTOR, table, ctx, query_idx, parent_query_idx, nullptr),
            table_(table), indexes_(indexes), select_idx_(select_idx)
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
                        error_status_ = 1;
                        return;
                    }
                }
            }

            // TODO: Provide strict type checking before inserting data.
           
        
            // this means we're using insert .. select syntax.
            if(select_idx_ != -1 && select_idx_ < ctx_.executors_call_stack_.size()){
              child_executor_ = ctx_.executors_call_stack_[select_idx_];
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

        std::vector<Value> next() {
          if(error_status_ || finished_) return {};
          // TODO: fix alot of vector copying.
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
              vals_[idx] = evaluate_expression(ctx_, val_exp, eval);
            }
          }

          RecordID* rid = new RecordID();
          Record* record = table_->translateToRecord(vals_);
          int err = table_->getTable()->insertRecord(rid, *record);
          // loop over table indexes.
          for(int i = 0; i < indexes_.size(); ++i){
              IndexKey k = getIndexKeyFromTuple(indexes_[i].fields_numbers_, vals_);
              if(k.keys_.size() == 0) {
                  error_status_ = 1;
                  break;
              }
              bool success = indexes_[i].index_->Insert(k, *rid);
              indexes_[i].index_->See();
              if(!success){
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

        IndexKey getIndexKeyFromTuple(std::vector<int>& fields, std::vector<Value>& tuples){
            IndexKey k = {};
            for(int i = 0; i < fields.size(); ++i){
                if(fields[i] >= tuples.size()) 
                    return {};
                k.keys_.push_back(tuples[fields[i]]);
            }
            return k;
        }
    private:
        TableSchema* table_ = nullptr;
        std::vector<IndexHeader> indexes_;
        InsertStatementData* statement = nullptr;
        int select_idx_ = -1;
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
        {
            delete child_executor_;
        }

        Value evaluate(ASTNode* item){
            if(item->category_ == SUB_QUERY){
                auto sub_query = reinterpret_cast<SubQueryNode*>(item);
                bool used_with_exists = sub_query->used_with_exists_;
                Executor* sub_query_executor = ctx_.executors_call_stack_[sub_query->idx_]; 
                sub_query_executor->init();
                if(sub_query_executor->error_status_){
                    std::cout << "[ERROR] could not initialize sub-query" << std::endl;
                    error_status_ = 1;
                    return Value();
                }
                std::vector<Value> tmp = sub_query_executor->next();
                if(tmp.size() == 0 && sub_query_executor->finished_) {
                    if(used_with_exists) return Value(false);
                    return Value();
                }

                if(sub_query_executor->error_status_) {
                    std::cout << "[ERROR] could not execute sub-query" << std::endl;
                    error_status_ = 1;
                    return Value();
                }
                if(used_with_exists) return Value(tmp.size() != 0);
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
                    Executor* cur_exec = nullptr;
                    if(cur_query_idx == query_idx_){
                      cur_exec = this;
                    } else {
                      cur_exec = ctx_.executors_call_stack_[cur_query_idx];

                      while(cur_exec && cur_exec->type_ != SEQUENTIAL_SCAN_EXECUTOR && cur_exec->type_ != PRODUCT_EXECUTOR){
                        cur_exec = cur_exec->child_executor_;
                      }
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
                                return evaluate_expression(ctx_, fields_[i], eval);
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
                    Executor* cur_exec = nullptr;
                    if(cur_query_idx == query_idx_){
                      cur_exec  = this;
                    } else {
                      Executor* cur_exec = ctx_.executors_call_stack_[cur_query_idx];

                      while(cur_exec != nullptr && cur_exec->type_ != FILTER_EXECUTOR){
                          cur_exec = cur_exec->child_executor_;
                      }
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

                Value exp = evaluate_expression(ctx_, filter_, eval).getBoolVal();
                if(exp != false && !exp.isNull()){
                    if(child_executor_){
                        return output_;
                    }
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
        {
            delete output_schema_;
            delete child_executor_;
        }

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
                //output_schema_->printTableHeader();
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
            aggregated_values_["PREFIX_"] = std::vector<Value> (output_schema_->getCols().size() + 1, Value(0));

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

                // build the search key for the hash table.
                std::string hash_key = "PREFIX_"; // prefix to ensure we have at least one entry in the hash table.
                for(int i = 0; i < group_by_.size(); i++){
                    Value cur = evaluate(group_by_[i]);
                    hash_key += cur.toString();
                }


                // if the hash key exists we need to load it first.
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
                                        Value val = evaluate_expression(ctx_, exp, eval);
                                        if(!val.isNull()) 
                                            ++output_[idx];
                                    }
                                    break;
                        case AVG:
                        case SUM:
                                   {
                                       Value val = evaluate_expression(ctx_, exp, eval);
                                       if(val.type_ == INT) output_[idx] += val; 
                                       else if(val.isNull())
                                         *counter += -1;
                                   }
                                   break;
                        case MIN:
                                   {
                                       Value val = evaluate_expression(ctx_, exp, eval);
                                       if(val.type_ == INT) {
                                           if(counter->getIntVal() == 1) output_[idx] = val;
                                           output_[idx] = std::min<Value>(output_[idx], val);
                                       }
                                   }
                                   break;
                        case MAX:
                                   {
                                       Value val = evaluate_expression(ctx_, exp, eval);
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
            if(it_== aggregated_values_.end()){
              finished_ = true;
              return {};
            }
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
            output_.pop_back(); // remove the custom counter.
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
        {
            delete child_executor_;
        }

        Value evaluate(ASTNode* item){
            if(item->category_ == SUB_QUERY){
                auto sub_query = reinterpret_cast<SubQueryNode*>(item);
                bool used_with_exists = sub_query->used_with_exists_;
                Executor* sub_query_executor = ctx_.executors_call_stack_[sub_query->idx_]; 
                sub_query_executor->init();
                if(sub_query_executor->error_status_){
                    std::cout << "[ERROR] could not initialize sub-query" << std::endl;
                    error_status_ = 1;
                    return Value();
                }
                std::vector<Value> tmp = sub_query_executor->next();
                if(tmp.size() == 0 && sub_query_executor->finished_) {
                    if(used_with_exists) return Value(false);
                    return Value();
                }

                if(sub_query_executor->error_status_) {
                    std::cout << "[ERROR] could not execute sub-query" << std::endl;
                    error_status_ = 1;
                    return Value();
                }
                if(used_with_exists) return Value(tmp.size() != 0);
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
                    // if(query_idx_ == 0)
                    if(cur_query_parent == -1)
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
                    tmp_output.push_back(evaluate_expression(ctx_, fields_[i], eval));
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
        {
            delete child_executor_;
        }

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
        {
            delete child_executor_;
        }

        void init() {
            finished_ = 0;
            error_status_ = 0;
            child_executor_->init();
        }

        std::vector<Value> next() {
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
            std::deque<std::vector<Constraint>> col_constraints;
            for(int i = 0; i < fields.size(); ++i){
                std::string name = fields[i].field_name_;
                Type type = catalog_->tokenTypeToColType(fields[i].type_);
                if(type == INVALID) {
                    std::cout << "[ERROR] Invalid type\n";
                    return false;
                }
                col_names.push_back(name);
                col_types.push_back(type);
                col_constraints.push_back(fields[i].constraints_);
            }
            std::vector<Column> columns;
            std::vector<std::string> primary_key_cols;
            uint8_t offset_ptr = 0;
            for(size_t i = 0; i < col_names.size(); ++i){
                columns.push_back(Column(col_names[i], col_types[i], offset_ptr, col_constraints[i]));
                bool is_primary_key = false;
                for(int j = 0; j < col_constraints[i].size(); ++j){
                  if(col_constraints[i][j] == Constraint::PRIMARY_KEY){
                    is_primary_key = true;
                    break;
                  }
                }
                if(is_primary_key) primary_key_cols.push_back(col_names[i]);
                offset_ptr += Column::getSizeFromType(col_types[i]);
            }
            TableSchema* sch = catalog_->createTable(table_name, columns);
            if(sch == nullptr) return false;
            if(!primary_key_cols.empty()) {
                std::cout << "create idx from create pkey\n";
              int err = catalog_->createIndex(table_name, table_name+"_pkey", primary_key_cols);
              // TODO: use CTX error status instead of this.
              if(err) return false;
            }
            return true;
        }

        bool create_index_handler(QueryCTX& ctx) {
            CreateIndexStatementData* create_index = reinterpret_cast<CreateIndexStatementData*>(ctx.queries_call_stack_[0]);
            std::string index_name = create_index->index_name_;
            std::string table_name = create_index->table_name_;
            std::vector<std::string> fields = create_index->fields_;
            bool err = catalog_->createIndex(table_name, index_name, fields);
            if(err) return false;
            return true;
        }

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
                case CREATE_INDEX_DATA:
                    return create_index_handler(ctx);
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
                        TableSchema* new_output_schema = new TableSchema("agg_tmp_schema", nullptr, new_cols, true);

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

                        TableSchema* new_output_schema = new TableSchema(tname, schema->getTable(), columns, true);
                        SeqScanExecutor* scan = new SeqScanExecutor(new_output_schema, ctx, query_idx, parent_query_idx);
                        return scan;
                    } break;
                case PRODUCT: 
                    {
                        ProductOperation* op = reinterpret_cast<ProductOperation*>(logical_plan);
                        Executor* lhs = buildExecutionPlan(ctx, op->lhs_, query_idx, parent_query_idx);
                        Executor* rhs = buildExecutionPlan(ctx, op->rhs_, query_idx, parent_query_idx);
                        std::vector<Column> lhs_columns = lhs->output_schema_->getColumns();
                        std::vector<Column> rhs_columns = rhs->output_schema_->getColumns();
                        for(int i = 0; i < rhs_columns.size(); i++)
                            lhs_columns.push_back(rhs_columns[i]);

                        TableSchema* product_output_schema = new TableSchema("TMP_PRODUCT_TABLE", nullptr, lhs_columns, true);
                        ProductExecutor* product = new ProductExecutor(product_output_schema, ctx, query_idx, parent_query_idx, lhs, rhs);
                        return product;
                    } break;
                case JOIN: 
                    {
                        auto op = reinterpret_cast<JoinOperation*>(logical_plan);
                        Executor* lhs = buildExecutionPlan(ctx, op->lhs_, query_idx, parent_query_idx);
                        Executor* rhs = buildExecutionPlan(ctx, op->rhs_, query_idx, parent_query_idx);
                        std::vector<Column> lhs_columns = lhs->output_schema_->getColumns();
                        std::vector<Column> rhs_columns = rhs->output_schema_->getColumns();
                        for(int i = 0; i < rhs_columns.size(); i++)
                            lhs_columns.push_back(rhs_columns[i]);

                        TableSchema* join_output_schema = new TableSchema("TMP_JOIN_TABLE", nullptr, lhs_columns, true);
                        auto join = new JoinExecutor(join_output_schema, ctx, query_idx, parent_query_idx, lhs, rhs, op->filter_);
                        return join;
                    } break;
                case AL_UNION: 
                case AL_EXCEPT: 
                case AL_INTERSECT: 
                    {
                        // TODO: condense all set operators into one operator.
                        if(logical_plan->type_ == AL_UNION){
                            UnionOperation* op = reinterpret_cast<UnionOperation*>(logical_plan);
                            if(!op->lhs_ || !op->rhs_) return nullptr;
                            // TODO: don't rebuild queries.
                            // Executor* lhs = buildExecutionPlan(ctx, op->lhs_, op->lhs_->query_idx_, op->lhs_->query_parent_idx_);
                            // Executor* rhs = buildExecutionPlan(ctx, op->rhs_, op->rhs_->query_idx_, op->rhs_->query_parent_idx_);
                            Executor* lhs = nullptr; 
                            Executor* rhs = nullptr; 

                            if(op->lhs_->query_idx_ == -1)
                                lhs = buildExecutionPlan(ctx, op->lhs_, op->lhs_->query_idx_, op->lhs_->query_parent_idx_);
                            else
                                lhs = ctx.executors_call_stack_[op->lhs_->query_idx_];

                            if(op->rhs_->query_idx_ == -1)
                                rhs = buildExecutionPlan(ctx, op->rhs_, op->rhs_->query_idx_, op->rhs_->query_parent_idx_);
                            else
                                rhs = ctx.executors_call_stack_[op->rhs_->query_idx_];
                            // TODO: check that lhs and rhs have the same schema.
                            UnionExecutor* un = new UnionExecutor(lhs->output_schema_, ctx, query_idx, parent_query_idx, lhs, rhs);

                            // TODO: don't use an extra distinct executor for the all = false case, 
                            // Use a built-in hashtable inside of the set-operation executor instead.
                            if(op->all_ == false) 
                                return new DistinctExecutor(un , un->output_schema_, ctx, query_idx, parent_query_idx);
                            return un;
                        } else if(logical_plan->type_ == AL_EXCEPT) {
                            ExceptOperation* op = reinterpret_cast<ExceptOperation*>(logical_plan);
                            if(!op->lhs_ || !op->rhs_) return nullptr;
                            //Executor* lhs = buildExecutionPlan(ctx, op->lhs_, op->lhs_->query_idx_, op->lhs_->query_parent_idx_);
                            //Executor* rhs = buildExecutionPlan(ctx, op->rhs_, op->rhs_->query_idx_, op->rhs_->query_parent_idx_);
                            Executor* lhs = nullptr; 
                            Executor* rhs = nullptr; 

                            if(op->lhs_->query_idx_ == -1)
                                lhs = buildExecutionPlan(ctx, op->lhs_, op->lhs_->query_idx_, op->lhs_->query_parent_idx_);
                            else
                                lhs = ctx.executors_call_stack_[op->lhs_->query_idx_];

                            if(op->rhs_->query_idx_ == -1)
                                rhs = buildExecutionPlan(ctx, op->rhs_, op->rhs_->query_idx_, op->rhs_->query_parent_idx_);
                            else
                                rhs = ctx.executors_call_stack_[op->rhs_->query_idx_];
                            // TODO: check that lhs and rhs have the same schema.
                            ExceptExecutor* ex = new ExceptExecutor(lhs->output_schema_, ctx, query_idx, parent_query_idx, lhs, rhs);

                            // TODO: don't use an extra distinct executor for the all = false case, 
                            // Use a built-in hashtable inside of the set-operation executor instead.
                            if(op->all_ == false) 
                                return new DistinctExecutor(ex , ex->output_schema_, ctx, query_idx, parent_query_idx);
                            return ex;
                        } else {
                            IntersectOperation* op = reinterpret_cast<IntersectOperation*>(logical_plan);
                            if(!op->lhs_ || !op->rhs_) return nullptr;
                            //Executor* lhs = buildExecutionPlan(ctx, op->lhs_, op->lhs_->query_idx_, op->lhs_->query_parent_idx_);
                            //Executor* rhs = buildExecutionPlan(ctx, op->rhs_, op->rhs_->query_idx_, op->rhs_->query_parent_idx_);
                            Executor* lhs = nullptr; 
                            Executor* rhs = nullptr; 

                            if(op->lhs_->query_idx_ == -1)
                                lhs = buildExecutionPlan(ctx, op->lhs_, op->lhs_->query_idx_, op->lhs_->query_parent_idx_);
                            else
                                lhs = ctx.executors_call_stack_[op->lhs_->query_idx_];

                            if(op->rhs_->query_idx_ == -1)
                                rhs = buildExecutionPlan(ctx, op->rhs_, op->rhs_->query_idx_, op->rhs_->query_parent_idx_);
                            else
                                rhs = ctx.executors_call_stack_[op->rhs_->query_idx_];
                            // TODO: check that lhs and rhs have the same schema.
                            IntersectExecutor* intersect = new IntersectExecutor(lhs->output_schema_, ctx, query_idx, parent_query_idx, lhs, rhs);

                            // TODO: don't use an extra distinct executor for the all = false case, 
                            // Use a built-in hashtable inside of the set-operation executor instead.
                            if(op->all_ == false) 
                                return new DistinctExecutor(intersect , intersect->output_schema_, ctx, query_idx, parent_query_idx);
                            return intersect;
                        }
                    } break;
                case INSERTION: 
                    {
                        auto statement = reinterpret_cast<InsertStatementData*>(ctx.queries_call_stack_[query_idx]);
                        TableSchema* table = catalog_->getTableSchema(statement->table_name_);
                        int select_idx = statement->select_idx_;

                        InsertionExecutor* insert =  new InsertionExecutor(
                                                         table,
                                                         catalog_->getIndexesOfTable(statement->table_name_),            
                                                         ctx, 
                                                         query_idx, 
                                                         parent_query_idx, 
                                                         select_idx
                                                       );
                        return insert;
                    } break;
                default: 
                    std::cout << "[ERROR] unsupported Algebra Operaion\n";
                    return nullptr;
            }
        }
        bool runExecutor(QueryCTX& ctx, QueryResult* result){
            if(ctx.executors_call_stack_.size() < 1 || (bool) ctx.error_status_ ) return false;
            Executor* physical_plan = nullptr;

            // this case means we have set operations: union, except or intersect
            if(ctx.executors_call_stack_.size() > ctx.queries_call_stack_.size()) {
                // first set operation.
                physical_plan = ctx.executors_call_stack_[ctx.queries_call_stack_.size()]; 
                std::cout << "Set operation\n";
            }
            else{
                physical_plan = ctx.executors_call_stack_[0];
            }

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
