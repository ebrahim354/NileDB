#pragma once

Value evaluate_expression(
      QueryCTX& ctx, 
      ASTNode* expression, 
      Executor* this_exec,
      bool only_one,
      bool eval_sub_query
    );

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

        // TODO: Not used anymore, delete this before release.
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
                                return evaluate_expression(ctx_, fields_[i], this, true, true);
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

                Value exp = evaluate_expression(ctx_, filter_, this, true, true).getBoolVal();
                if(!exp.isNull() && exp != false){
                    if(child_executor_){
                        return output_;
                    }
                    return {exp};
                }
            }
        }
    //private:
        ExpressionNode* filter_ = nullptr;
        std::vector<ExpressionNode*> fields_ = {};
        std::vector<std::string> field_names_ = {};
        // TODO: Not used anymore, delete this before release.
        std::function<Value(ASTNode*)>
            eval = std::bind(&FilterExecutor::evaluate, this, std::placeholders::_1);
};
