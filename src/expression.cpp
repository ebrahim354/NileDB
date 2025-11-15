#pragma once
#include "parser.cpp"
#include "execution_engine.cpp"
#include "utils.cpp"
#include <string>
#include <unordered_map>

Value evaluate(QueryCTX& ctx, Executor* this_exec, ASTNode* item);
Value evaluate_subquery(QueryCTX& ctx, Executor* this_exec, ASTNode* item);


Value abs_func(std::vector<Value> vals){
  if(vals.size() != 1){
    std::cout << "[ERROR] Incorrect number of arguments\n";
    return Value();
  }
  int int_val = vals[0].getIntVal(); 
  if(int_val < 0) 
    return Value(-int_val);
  return vals[0];
}

Value nullif_func(std::vector<Value> vals){
  if(vals.size() != 2){
    std::cout << "[ERROR] Incorrect number of arguments\n";
    return Value();
  }
  if(vals[0] == vals[1]) return Value(NULL_TYPE);
  return vals[0];
}

Value coalesce_func(std::vector<Value> vals){
  if(!vals.size()){
    std::cout << "[ERROR] Incorrect number of arguments\n";
    return Value();
  }
  for(Value& val : vals){
    if(!val.isNull()) 
      return val;
  }
  return Value(NULL_TYPE);
}

//TODO: should be moved the the catalog class.
std::unordered_map<std::string, std::function<Value(std::vector<Value>)>> reserved_functions = 
//{{"ABS", abs_func}, {"COALESCE", coalesce_func}, {"NULLIF", nullif_func}};
{{"ABS", abs_func}, {"COALESCE", coalesce_func}};


Value evaluate_expression(
      QueryCTX& ctx, 
      ASTNode* expression, 
      Executor* this_exec,
      bool only_one = true,
      bool eval_sub_query = true
    ) {
    switch(expression->category_) {
        case EXPRESSION  : 
            {
                ExpressionNode* ex = reinterpret_cast<ExpressionNode*>(expression);
                return evaluate_expression(ctx, ex->cur_, this_exec, false, eval_sub_query);
            }
        case CASE_EXPRESSION  : 
            {
                CaseExpressionNode* case_ex = reinterpret_cast<CaseExpressionNode*>(expression);
                Value initial_value;
                if(case_ex->initial_value_){
                    initial_value = evaluate_expression(ctx, case_ex->initial_value_, this_exec, true, eval_sub_query);
                }
                for(auto& [when, then] : case_ex->when_then_pairs_){
                    Value evaluated_when = evaluate_expression(ctx, when, this_exec, true, eval_sub_query);
                    if(evaluated_when.isNull()) continue;
                    if(case_ex->initial_value_ && !initial_value.isNull() && evaluated_when == initial_value){
                        return evaluate_expression(ctx, then, this_exec, true, eval_sub_query);
                    }
                    else if(!case_ex->initial_value_ && evaluated_when.getBoolVal()) 
                        return evaluate_expression(ctx, then, this_exec, true, eval_sub_query);
                }
                if(case_ex->else_) return evaluate_expression(ctx, case_ex->else_, this_exec, true, eval_sub_query);
                return Value(NULL_TYPE);
            }
        case NULLIF_EXPRESSION  : 
            {
                NullifExpressionNode* nullif_ex = reinterpret_cast<NullifExpressionNode*>(expression);
                assert(nullif_ex->lhs_ && nullif_ex->rhs_); // if you are here they must not be null pointers.

                Value lhs_val = evaluate_expression(ctx, nullif_ex->lhs_, this_exec, true, eval_sub_query);
                // lhs is already null no need to check the rhs.
                if(lhs_val.isNull()) return Value(NULL_TYPE); 
                Value rhs_val = evaluate_expression(ctx, nullif_ex->rhs_, this_exec, true, eval_sub_query);

                if(rhs_val.isNull() || lhs_val != rhs_val)
                    return lhs_val;
                return Value(NULL_TYPE);
            }
        case IN  : 
            {
                InNode* in = reinterpret_cast<InNode*>(expression);
                Value val = evaluate_expression(ctx, in->val_, this_exec, false, eval_sub_query);
                //if(val.isNull()) return val;
                bool answer = false;
                bool null_ret = false;
                for(int i = 0; i < in->list_.size(); ++i){
                    Value tmp = evaluate_expression(ctx, in->list_[i], this_exec, false, false);
                    if(tmp.type_ == Type::EXECUTOR_ID){
                        int idx = tmp.getIntVal();
                        Executor* sub_query_executor = ctx.executors_call_stack_[idx]; 
                        sub_query_executor->init();
                        // TODO: register errors to ctx object and stop execution.
                        if(sub_query_executor->error_status_){
                            std::cout << "[ERROR] could not initialize sub-query" << std::endl;
                            return Value();
                        }

                        while(!answer && !sub_query_executor->finished_ && !sub_query_executor->error_status_){
                            std::vector<Value> sub_query_output = sub_query_executor->next();
                            if(sub_query_output.size() == 0 && sub_query_executor->finished_) {
                                break;
                            }

                            if(sub_query_executor->error_status_) {
                                std::cout << "[ERROR] could not execute sub-query" << std::endl;
                                this_exec->error_status_ = 1;
                                //ctx.error_status_ = 1; // TODO: better error handling.
                                return Value();
                            }

                            if(sub_query_output.size() != 1) {
                                std::cout << "[ERROR] sub-query should return exactly 1 column" << std::endl;
                                this_exec->error_status_ = 1;
                                //ctx.error_status_ = 1; // TODO: better error handling.
                                return Value();
                            }
                            if(sub_query_output[0].isNull()){
                                null_ret = true;
                            }
                            else if(val == sub_query_output[0]){
                                answer = true;
                                null_ret = false;
                                break;
                            }
                        }
                    } else if(tmp.isNull() || val.isNull()) {
                        null_ret = true;
                    }
                    else if(val == tmp) {
                        answer = true;
                        null_ret = false;
                        break;
                    }
                }
                if(null_ret) return Value(NULL_TYPE);
                if(in->negated_) answer = !answer;
                return Value(answer);
            }
        case BETWEEN  : 
            {
                bool answer = false;
                bool null_return = false;
                BetweenNode* between = reinterpret_cast<BetweenNode*>(expression);
                Value val = evaluate_expression(ctx, between->val_, this_exec, false, eval_sub_query);
                if(val.isNull()) return val; 

                Value lhs = evaluate_expression(ctx, between->lhs_, this_exec, false, eval_sub_query);
                Value rhs = evaluate_expression(ctx, between->rhs_, this_exec, false, eval_sub_query);

                if(lhs.isNull() && rhs.isNull()) return lhs;  // both null => null

                if(!lhs.isNull() && !rhs.isNull()) { // both valid => true/false
                    if(val >= lhs && val <= rhs)
                        answer = true;
                }
                // this logic is used to handle null lhs and rhs.
                // if val is in the range of ]null, rhs] or [lhs, null[
                // the result is always null and is always false in case of not being in that range.
                if(lhs.isNull() && !rhs.isNull()) {
                    if(val <= rhs) null_return = true;
                }
                if(!lhs.isNull() && rhs.isNull()) {
                    if(val >= lhs) null_return = true;
                }
                if(null_return) 
                    return Value(NULL_TYPE);

                if(between->negated_) answer = !answer;
                return Value(answer);
            }
        case NOT  : 
            {
                NotNode* lnot = reinterpret_cast<NotNode*>(expression);
                Value val = evaluate_expression(ctx, lnot->cur_, this_exec, true, eval_sub_query);
                if(val.isNull()) return val; 
                return Value((bool)(lnot->effective_^val.getBoolVal())); // 1 1 = 0, 1 0 = 1, 0 1 = 1, 0 0 = 0
            }
        case OR  : 
            {
                OrNode* lor = reinterpret_cast<OrNode*>(expression);
                ASTNode* ptr = lor;
                Value lhs; 
                while(ptr){
                    lhs = evaluate_expression(ctx, lor->cur_, this_exec, true, eval_sub_query);
                    if(lhs.getBoolVal() != 0) break;
                    ptr = lor->next_;
                    if(!ptr) break;
                    if(ptr->category_ == OR){
                        lor = reinterpret_cast<OrNode*>(ptr);
                    } else {
                        lhs = evaluate_expression(ctx, ptr, this_exec, true, eval_sub_query);
                        break;
                    };
                }
                if(lhs.isNull()) return Value(NULL_TYPE);
                return lhs.getBoolVal() != 0 ? Value(true) : Value(false);
            }
        case AND : 
            {
                AndNode* land = reinterpret_cast<AndNode*>(expression);
                ASTNode* ptr = land;
                Value lhs;
                while(ptr){
                    lhs = evaluate_expression(ctx, land->cur_, this_exec, true, eval_sub_query);
                    if(lhs.isNull()) break; 
                    if(lhs.getBoolVal() == 0) break;
                    ptr = land->next_;
                    if(!ptr) break;
                    if(ptr->category_ == AND){
                        land = reinterpret_cast<AndNode*>(land->next_);
                    } else {
                        lhs = evaluate_expression(ctx, ptr, this_exec, true, eval_sub_query);
                        break;
                    }
                }
                if(lhs.isNull()) return Value(NULL_TYPE);
                return lhs.getBoolVal() == 0 ? Value(false) : Value(true);
            } 
        case EQUALITY : 
            {
                EqualityNode* eq = reinterpret_cast<EqualityNode*>(expression);
                TokenType op = eq->token_.type_;
                Value lhs = evaluate_expression(ctx, eq->cur_, this_exec, 
                        eq->cur_->category_ == EQUALITY, 
                        eval_sub_query);
                ASTNode* ptr = eq->next_;
                while(ptr){
                    Value rhs = evaluate_expression(ctx, ptr, this_exec, 
                            eq->cur_->category_ == EQUALITY, 
                            eval_sub_query);
                    bool is_or_isnot = (op == TokenType::IS || op == TokenType::ISNOT);
                    if((lhs.isNull() || rhs.isNull()) && !is_or_isnot) return Value(NULL_TYPE); 
                    //if(op == TokenType::IS)    op = TokenType::EQ;
                    //if(op == TokenType::ISNOT) op = TokenType::NEQ;
                    if(is_or_isnot) {
                        assert(rhs.isNull() && "using 'is' operator with a non null rhs!");

                        if(op == TokenType::IS)
                            lhs = Value(lhs.isNull());
                        else 
                            lhs = Value(!lhs.isNull());
                    }

                    if(op == TokenType::EQ && lhs == rhs) lhs = Value(true);
                    else if(op == TokenType::EQ && lhs != rhs) lhs = Value(false);

                    if(op == TokenType::NEQ && lhs == rhs) lhs = Value(false);
                    else if(op == TokenType::NEQ && lhs != rhs) lhs = Value(true);

                    if(ptr->category_ == EQUALITY) {
                        EqualityNode* tmp = reinterpret_cast<EqualityNode*>(ptr);
                        op = ptr->token_.type_;
                        ptr = tmp->next_;
                    } else break;
                }
                return lhs;
            } 
        case COMPARISON : 
            {
                ComparisonNode* comp = reinterpret_cast<ComparisonNode*>(expression);
                TokenType op = comp->token_.type_;
                Value lhs = evaluate_expression(ctx, comp->cur_, this_exec, 
                    comp->cur_->category_ == COMPARISON, eval_sub_query);
                ASTNode* ptr = comp->next_;
                while(ptr){
                    Value rhs = evaluate_expression(ctx, ptr, this_exec, 
                        comp->cur_->category_ == COMPARISON, 
                        eval_sub_query);
                    if(lhs.isNull() || rhs.isNull()) return Value(NULL_TYPE);

                    if(op == TokenType::GT && lhs > rhs ) lhs = Value(true);
                    else if(op == TokenType::GT) lhs = Value(false);

                    if(op == TokenType::LT && lhs < rhs) return Value(true);
                    else if(op == TokenType::LT) lhs = Value(false);

                    if(op == TokenType::GTE && lhs >= rhs) return Value(true);
                    else if(op == TokenType::GTE) lhs = Value(false);

                    if(op == TokenType::LTE && lhs <= rhs) lhs = Value(true);
                    else if(op == TokenType::LTE)  lhs = Value(false);

                    //lhs = compareStr(lhs, rhs, op);
                    if(ptr->category_ == COMPARISON){
                        ComparisonNode* tmp = reinterpret_cast<ComparisonNode*>(ptr);
                        op = ptr->token_.type_;
                        ptr = tmp->next_;
                    } else break;
                }
                return lhs;
            } 
        case SUB_QUERY:
            {
              if(eval_sub_query) return evaluate_subquery(ctx, this_exec, expression);
              auto sub_query = reinterpret_cast<SubQueryNode*>(expression);
              auto v = Value(sub_query->idx_);
              v.type_ = Type::EXECUTOR_ID;
              return v;
            }
        case TERM : 
            {
                TermNode* t = reinterpret_cast<TermNode*>(expression);
                Value lhs = evaluate_expression(ctx, t->cur_, this_exec, 
                    t->cur_->category_ == TERM, eval_sub_query);
                if(only_one) return lhs;
                TokenType op = t->token_.type_;
                ASTNode* ptr = t->next_;
                while(ptr){
                    Value rhs = evaluate_expression(ctx, ptr, this_exec, ptr->category_ == TERM, eval_sub_query);
                    if(lhs.isNull()) return lhs;
                    if(rhs.isNull()) return rhs;
                    //int lhs_num = lhs.getIntVal();
                    //int rhs_num = rhs.getIntVal();
                    //if(op == TokenType::PLUS) lhs_num += rhs_num; 
                    //if(op == TokenType::MINUS) lhs_num -= rhs_num;
                    //lhs = Value(lhs_num);
                    if(op == TokenType::PLUS) lhs += rhs; 
                    if(op == TokenType::MINUS) lhs -= rhs;
                    if(ptr->category_ == TERM){
                        TermNode* tmp = reinterpret_cast<TermNode*>(ptr);
                        op = ptr->token_.type_;
                        ptr = tmp->next_;
                    } else break;
                }
                return lhs;
            } 
        case FACTOR : {
                          FactorNode* f = reinterpret_cast<FactorNode*>(expression);
                          Value lhs = evaluate_expression(ctx, f->cur_, this_exec, 
                              f->cur_->category_ == FACTOR, eval_sub_query);
                          if(only_one) return lhs;
                          TokenType op = f->token_.type_;
                          ASTNode* ptr = f->next_;
                          while(ptr){
                              Value rhs = evaluate_expression(ctx, ptr, this_exec, 
                                  ptr->category_ == FACTOR, eval_sub_query);
                              if(lhs.isNull()) return lhs;
                              if(rhs.isNull()) return rhs;
                              //int lhs_num = lhs.getIntVal();
                              //int rhs_num = rhs.getIntVal();
                              if(op == TokenType::STAR) lhs *= rhs; //lhs_num *= rhs_num; 
                              //if(op == TokenType::SLASH && rhs_num != 0)  lhs_num /= rhs_num;
                              if(op == TokenType::SLASH && rhs.getIntVal() != 0) lhs /= rhs; 
                              //lhs = Value(lhs_num);
                              if(ptr->category_ == FACTOR){
                                  FactorNode* tmp = reinterpret_cast<FactorNode*>(ptr);
                                  op = ptr->token_.type_;
                                  ptr = tmp->next_;
                              } else break;
                          }
                          return lhs;
                      } 
        case UNARY : 
                      {
                          UnaryNode* u = reinterpret_cast<UnaryNode*>(expression);
                          Value cur = evaluate_expression(ctx, u->cur_, this_exec, true, eval_sub_query);
                          if(cur.isNull()) return cur;
                          if(u->token_.type_ == TokenType::MINUS){
                              cur *= Value(-1);
                              return Value(cur);
                          }
                          return cur;
                      } 
        case SCALAR_FUNC: 
                      {
                          ScalarFuncNode* sfn = reinterpret_cast<ScalarFuncNode*>(expression);
                          std::string name = str_toupper(sfn->name_);
                          if(!reserved_functions.count(name)){
                              // TODO: this check should be in the query validation phase.
                              std::cout << "[ERROR] undefined function call " << sfn->name_ << "\n";
                              return Value();
                          }
                          std::vector<Value> vals;
                          for(int i = 0; i < sfn->args_.size(); ++i){
                            vals.emplace_back(evaluate_expression(ctx, sfn->args_[i], this_exec, true, eval_sub_query));
                          }
                          return reserved_functions[name](vals);
                      } 
        case TYPE_CAST: 
                      {
                          TypeCastNode* cast = reinterpret_cast<TypeCastNode*>(expression);
                          Value val = evaluate_expression(ctx, cast->exp_, this_exec, true, eval_sub_query);
                          if(val.type_ == NULL_TYPE) return val; // NULL values can't be casted? 
                          if(val.type_ == cast->type_)
                              return val;
                          if(val.type_ == INT && cast->type_ == FLOAT) {
                              float res = (float) val.getIntVal(); 
                              return Value(res);
                          } 
                          if(val.type_ == FLOAT && cast->type_ == INT) {
                              int res = (int)  round(val.getFloatVal());
                              return Value(res);
                          } 
                          assert(0  && "TYPE CASTING NOT SUPPORTED YET!");
                          // TODO: do more usefull type casting.
                          return val;
                      } 
        case STRING_CONSTANT: 
                      {
                          std::string val = "";
                          for(int i = 1; i < expression->token_.val_.size() - 1; i++){
                              val += expression->token_.val_[i];
                          }
                          return Value(val);
                      }
        case FLOAT_CONSTANT:
                      {
                          errno = 0;
                          float val = str_to_float(expression->token_.val_);
                          if(!errno) return Value(val);

                          errno = 0;
                          double dval = str_to_double(expression->token_.val_);
                          assert(errno == 0);
                          return Value(dval);
                      }
        case INTEGER_CONSTANT: 
                      {
                          errno = 0;
                          long long val = str_to_ll(expression->token_.val_);
                          assert(errno == 0);
                          if(val < INT_MAX && val > INT_MIN)
                              return Value((int) val);
                          return Value(val);
                      }
        case NULL_CONSTANT: 
                      {
                          return Value(NULL_TYPE);
                      }
        default:
                      {
                          return evaluate(ctx, this_exec, expression);
                      }
    }
}



// assumes top level ands only.
std::vector<ExpressionNode*> split_by_and(ExpressionNode* expression) {
  ExpressionNode* ex = reinterpret_cast<ExpressionNode*>(expression);
  if(!ex) return {};

  ASTNode* ptr = ex->cur_;
  std::vector<ExpressionNode*> ret;
  while(ptr){
    // TODO: should be changed.
    ExpressionNode* ex_copy = new ExpressionNode(ex->top_level_statement_, ex->query_idx_);
    ex_copy->cur_ = ptr;
    ret.push_back(ex_copy);
    if(ptr->category_ != AND){
      break;
    } 
    auto cur = reinterpret_cast<AndNode*>(ptr);
    ptr = cur->next_;
    cur->next_ = nullptr;
  }
  return ret;
}


void accessed_tables(ASTNode* expression ,std::vector<std::string>& tables, Catalog* catalog, bool only_one = true) {
    if(!expression) return;
    switch(expression->category_){
        case EXPRESSION  : 
            {
                ExpressionNode* ex = reinterpret_cast<ExpressionNode*>(expression);
                return accessed_tables(ex->cur_, tables, catalog, false);
            }
        case CASE_EXPRESSION  : 
            {
                CaseExpressionNode* case_ex = reinterpret_cast<CaseExpressionNode*>(expression);
                if(case_ex->initial_value_){
                    accessed_tables(case_ex->initial_value_, tables, catalog);
                }
                for(auto& [when, then] : case_ex->when_then_pairs_){
                    accessed_tables(when, tables, catalog);
                    accessed_tables(then, tables, catalog);
                }
                if(case_ex->else_)  
                    accessed_tables(case_ex->else_, tables, catalog);
                return;
            }
        case NULLIF_EXPRESSION  : 
            {
                NullifExpressionNode* nullif_ex = reinterpret_cast<NullifExpressionNode*>(expression);
                assert(nullif_ex->lhs_ && nullif_ex->rhs_); // if you are here they must not be null pointers.

                accessed_tables(nullif_ex->lhs_, tables, catalog);
                accessed_tables(nullif_ex->rhs_, tables, catalog);
                return;
            }
        case IN  : 
            {
                InNode* in = reinterpret_cast<InNode*>(expression);
                accessed_tables(in->val_, tables, catalog, false);
                for(int i = 0; i < in->list_.size(); ++i){
                    accessed_tables(in->list_[i], tables, catalog, false);
                }
                return;
            }
        case BETWEEN  : 
            {
                BetweenNode* between = reinterpret_cast<BetweenNode*>(expression);
                accessed_tables(between->val_, tables, catalog, false);
                accessed_tables(between->lhs_, tables, catalog, false);
                accessed_tables(between->rhs_, tables, catalog, false);
                return;
            }
        case NOT  : 
            {
                NotNode* lnot = reinterpret_cast<NotNode*>(expression);
                accessed_tables(lnot->cur_, tables, catalog);
                return;
            }
        case OR  : 
            {
                OrNode* lor = reinterpret_cast<OrNode*>(expression);
                ASTNode* ptr = lor;
                while(ptr){
                    accessed_tables(lor->cur_, tables, catalog);
                    ptr = lor->next_;
                    if(!ptr) break;
                    if(ptr->category_ == OR){
                        lor = reinterpret_cast<OrNode*>(ptr);
                    } else {
                        accessed_tables(ptr, tables, catalog);
                        break;
                    };
                }
                return;
            }
        case AND : 
            {
                AndNode* land = reinterpret_cast<AndNode*>(expression);
                ASTNode* ptr = land;
                while(ptr){
                    accessed_tables(land->cur_, tables, catalog);
                    ptr = land->next_;
                    if(!ptr) break;
                    if(ptr->category_ == AND){
                        land = reinterpret_cast<AndNode*>(land->next_);
                    } else {
                        accessed_tables(ptr, tables, catalog);
                        break;
                    }
                }
                return;
            } 
        case EQUALITY : 
            {
                EqualityNode* eq = reinterpret_cast<EqualityNode*>(expression);
                TokenType op = eq->token_.type_;
                accessed_tables(eq->cur_, tables, catalog);
                ASTNode* ptr = eq->next_;
                while(ptr){
                    accessed_tables(ptr, tables, catalog);
                    if(ptr->category_ == EQUALITY) {
                        EqualityNode* tmp = reinterpret_cast<EqualityNode*>(ptr);
                        op = ptr->token_.type_;
                        ptr = tmp->next_;
                    } else break;
                }
                return;
            } 
        case COMPARISON : 
            {
                ComparisonNode* comp = reinterpret_cast<ComparisonNode*>(expression);
                TokenType op = comp->token_.type_;
                accessed_tables(comp->cur_, tables, catalog, comp->cur_->category_ == COMPARISON);
                ASTNode* ptr = comp->next_;
                while(ptr){
                    accessed_tables(ptr, tables, catalog, comp->cur_->category_ == COMPARISON);

                    if(ptr->category_ == COMPARISON){
                        ComparisonNode* tmp = reinterpret_cast<ComparisonNode*>(ptr);
                        op = ptr->token_.type_;
                        ptr = tmp->next_;
                    } else break;
                }
                return;
            } 
        case TERM : 
            {
                TermNode* t = reinterpret_cast<TermNode*>(expression);
                accessed_tables(t->cur_, tables, catalog, t->cur_->category_ == TERM);
                if(only_one) return;
                TokenType op = t->token_.type_;
                ASTNode* ptr = t->next_;
                while(ptr){
                    accessed_tables(ptr, tables, catalog, ptr->category_ == TERM);
                    if(ptr->category_ == TERM){
                        TermNode* tmp = reinterpret_cast<TermNode*>(ptr);
                        op = ptr->token_.type_;
                        ptr = tmp->next_;
                    } else break;
                }
                return;
            } 
        case FACTOR : {
                          FactorNode* f = reinterpret_cast<FactorNode*>(expression);
                          accessed_tables(f->cur_, tables, catalog, f->cur_->category_ == FACTOR);
                          if(only_one) return;
                          TokenType op = f->token_.type_;
                          ASTNode* ptr = f->next_;
                          while(ptr){
                              accessed_tables(ptr, tables, catalog, ptr->category_ == FACTOR);
                              if(ptr->category_ == FACTOR){
                                  FactorNode* tmp = reinterpret_cast<FactorNode*>(ptr);
                                  op = ptr->token_.type_;
                                  ptr = tmp->next_;
                              } else break;
                          }
                          return;
                      } 
        case UNARY : 
                      {
                          UnaryNode* u = reinterpret_cast<UnaryNode*>(expression);
                          accessed_tables(u->cur_, tables, catalog);
                          return;
                      } 
        case SCALAR_FUNC: 
                      {
                          ScalarFuncNode* sfn = reinterpret_cast<ScalarFuncNode*>(expression);
                          for(int i = 0; i < sfn->args_.size(); ++i){
                              accessed_tables(sfn->args_[i], tables, catalog);
                          }
                          return;
                      } 
        case TYPE_CAST: 
                      {
                          TypeCastNode* cast = reinterpret_cast<TypeCastNode*>(expression);
                          accessed_tables(cast->exp_, tables, catalog);
                          return;
                      } 
        case SCOPED_FIELD:{
                              std::string table = reinterpret_cast<ScopedFieldNode*>(expression)->table_->token_.val_;
                              tables.push_back(table);
                              return;
                          }
        case FIELD:{
                       std::string field = reinterpret_cast<ASTNode*>(expression)->token_.val_;
                       std::vector<std::string> valid_tables = catalog->getTablesByField(field);
                       for(int i = 0; i < valid_tables.size(); ++i){
                           int n = tables.size();
                           bool exists = false;
                           for(int j = 0; j < n; ++j){
                               if(tables[j] == valid_tables[i]) {
                                   exists = true; 
                                   break;
                               }
                           }
                           if(!exists)
                               tables.push_back(valid_tables[i]);
                       }
                       return;
                   }

        case STRING_CONSTANT: 
        case FLOAT_CONSTANT: 
        case INTEGER_CONSTANT: 
        case NULL_CONSTANT: 
        default:
                   return;
    }
}

void accessed_fields(ASTNode* expression ,std::vector<std::string>& fields, bool only_one = true) {
    if(!expression) return;
    switch(expression->category_){
        case EXPRESSION  : 
            {
                ExpressionNode* ex = reinterpret_cast<ExpressionNode*>(expression);
                return accessed_fields(ex->cur_, fields,  false);
            }
        case CASE_EXPRESSION  : 
            {
                CaseExpressionNode* case_ex = reinterpret_cast<CaseExpressionNode*>(expression);
                if(case_ex->initial_value_){
                    accessed_fields(case_ex->initial_value_, fields);
                }
                for(auto& [when, then] : case_ex->when_then_pairs_){
                    accessed_fields(when, fields);
                    accessed_fields(then, fields);
                }
                if(case_ex->else_)  
                    accessed_fields(case_ex->else_, fields);
                return;
            }
        case NULLIF_EXPRESSION  : 
            {
                NullifExpressionNode* nullif_ex = reinterpret_cast<NullifExpressionNode*>(expression);
                assert(nullif_ex->lhs_ && nullif_ex->rhs_); // if you are here they must not be null pointers.

                accessed_fields(nullif_ex->lhs_, fields);
                accessed_fields(nullif_ex->rhs_, fields);
                return;
            }
        case IN  : 
            {
                InNode* in = reinterpret_cast<InNode*>(expression);
                accessed_fields(in->val_, fields, false);
                for(int i = 0; i < in->list_.size(); ++i){
                    accessed_fields(in->list_[i], fields, false);
                }
                return;
            }
        case BETWEEN  : 
            {
                BetweenNode* between = reinterpret_cast<BetweenNode*>(expression);
                accessed_fields(between->val_, fields,  false);
                accessed_fields(between->lhs_, fields,  false);
                accessed_fields(between->rhs_, fields,  false);
                return;
            }
        case NOT  : 
            {
                NotNode* lnot = reinterpret_cast<NotNode*>(expression);
                accessed_fields(lnot->cur_, fields);
                return;
            }
        case OR  : 
            {
                OrNode* lor = reinterpret_cast<OrNode*>(expression);
                ASTNode* ptr = lor;
                while(ptr){
                    accessed_fields(lor->cur_, fields);
                    ptr = lor->next_;
                    if(!ptr) break;
                    if(ptr->category_ == OR){
                        lor = reinterpret_cast<OrNode*>(ptr);
                    } else {
                        accessed_fields(ptr, fields);
                        break;
                    };
                }
                return;
            }
        case AND : 
            {
                AndNode* land = reinterpret_cast<AndNode*>(expression);
                ASTNode* ptr = land;
                while(ptr){
                    accessed_fields(land->cur_, fields);
                    ptr = land->next_;
                    if(!ptr) break;
                    if(ptr->category_ == AND){
                        land = reinterpret_cast<AndNode*>(land->next_);
                    } else {
                        accessed_fields(ptr, fields);
                        break;
                    }
                }
                return;
            } 
        case EQUALITY : 
            {
                EqualityNode* eq = reinterpret_cast<EqualityNode*>(expression);
                TokenType op = eq->token_.type_;
                accessed_fields(eq->cur_, fields);
                ASTNode* ptr = eq->next_;
                while(ptr){
                    accessed_fields(ptr, fields);
                    if(ptr->category_ == EQUALITY) {
                        EqualityNode* tmp = reinterpret_cast<EqualityNode*>(ptr);
                        op = ptr->token_.type_;
                        ptr = tmp->next_;
                    } else break;
                }
                return;
            } 
        case COMPARISON : 
            {
                ComparisonNode* comp = reinterpret_cast<ComparisonNode*>(expression);
                TokenType op = comp->token_.type_;
                accessed_fields(comp->cur_, fields, comp->cur_->category_ == COMPARISON);
                ASTNode* ptr = comp->next_;
                while(ptr){
                    accessed_fields(ptr, fields, comp->cur_->category_ == COMPARISON);

                    if(ptr->category_ == COMPARISON){
                        ComparisonNode* tmp = reinterpret_cast<ComparisonNode*>(ptr);
                        op = ptr->token_.type_;
                        ptr = tmp->next_;
                    } else break;
                }
                return;
            } 
        case TERM : 
            {
                TermNode* t = reinterpret_cast<TermNode*>(expression);
                accessed_fields(t->cur_, fields, t->cur_->category_ == TERM);
                if(only_one) return;
                TokenType op = t->token_.type_;
                ASTNode* ptr = t->next_;
                while(ptr){
                    accessed_fields(ptr, fields, ptr->category_ == TERM);
                    if(ptr->category_ == TERM){
                        TermNode* tmp = reinterpret_cast<TermNode*>(ptr);
                        op = ptr->token_.type_;
                        ptr = tmp->next_;
                    } else break;
                }
                return;
            } 
        case FACTOR : {
                          FactorNode* f = reinterpret_cast<FactorNode*>(expression);
                          accessed_fields(f->cur_, fields, f->cur_->category_ == FACTOR);
                          if(only_one) return;
                          TokenType op = f->token_.type_;
                          ASTNode* ptr = f->next_;
                          while(ptr){
                              accessed_fields(ptr, fields, ptr->category_ == FACTOR);
                              if(ptr->category_ == FACTOR){
                                  FactorNode* tmp = reinterpret_cast<FactorNode*>(ptr);
                                  op = ptr->token_.type_;
                                  ptr = tmp->next_;
                              } else break;
                          }
                          return;
                      } 
        case UNARY : 
                      {
                          UnaryNode* u = reinterpret_cast<UnaryNode*>(expression);
                          accessed_fields(u->cur_, fields);
                          return;
                      } 
        case SCALAR_FUNC: 
                      {
                          ScalarFuncNode* sfn = reinterpret_cast<ScalarFuncNode*>(expression);
                          for(int i = 0; i < sfn->args_.size(); ++i){
                              accessed_fields(sfn->args_[i], fields);
                          }
                          return;
                      } 
        case TYPE_CAST: 
                      {
                          TypeCastNode* cast = reinterpret_cast<TypeCastNode*>(expression);
                          accessed_fields(cast->exp_, fields);
                          return;
                      } 
        case SCOPED_FIELD:{
                              std::string table = reinterpret_cast<ScopedFieldNode*>(expression)->table_->token_.val_;
                              std::string field = reinterpret_cast<ScopedFieldNode*>(expression)->token_.val_;
                              fields.push_back(table+"."+field);
                              return;
                          }
        case FIELD:{
                       std::string field = reinterpret_cast<ASTNode*>(expression)->token_.val_;
                       std::string prefix = AGG_FUNC_IDENTIFIER_PREFIX; // skip aggregate functions.
                       if(field.rfind(prefix, 0) == 0) return;
                       fields.push_back(field);
                       return;
                   }

        case STRING_CONSTANT: 
        case FLOAT_CONSTANT: 
        case INTEGER_CONSTANT: 
        case NULL_CONSTANT: 
        default:
                   return;
    }
}

Value evaluate_subquery(QueryCTX& ctx, Executor* this_exec, ASTNode* item) {
    auto sub_query = reinterpret_cast<SubQueryNode*>(item);
    bool used_with_exists = sub_query->used_with_exists_;
    Executor* sub_query_executor = ctx.executors_call_stack_[sub_query->idx_]; 
    sub_query_executor->init();
    if(sub_query_executor->error_status_){
        std::cout << "[ERROR] could not initialize sub-query" << std::endl;
        this_exec->error_status_ = 1;
        ctx.error_status_ = Error::QUERY_NOT_SUPPORTED; // TODO: put a better error_status_.
        return Value();
    }

    std::vector<Value> tmp = sub_query_executor->next();
    if(tmp.size() == 0 && sub_query_executor->finished_) {
        if(used_with_exists) 
            return Value(false);
        return Value();
    }

    if(sub_query_executor->error_status_) {
        std::cout << "[ERROR] could not execute sub-query" << std::endl;
        this_exec->error_status_ = 1;
        ctx.error_status_ = Error::QUERY_NOT_SUPPORTED; // TODO: put a better error_status_.
        return Value();
    }
    if(used_with_exists) return Value(tmp.size() != 0);
    if(tmp.size() != 1) {
        std::cout << "[ERROR] sub-query should return exactly 1 column" << std::endl;
        this_exec->error_status_ = 1;
        ctx.error_status_ = Error::QUERY_NOT_SUPPORTED; // TODO: put a better error_status_.
        return Value();
    }
    return tmp[0];
}


Value evaluate_field(QueryCTX& ctx, Executor* this_exec, ASTNode* item) {

    std::string field = item->token_.val_;
    //TableSchema* schema_ptr = output_schema_;
    TableSchema* schema_ptr = this_exec->output_schema_;
    int cur_query_idx = this_exec->query_idx_;
    //int cur_query_parent = parent_query_idx_;
    int cur_query_parent = this_exec->parent_query_idx_;
    int idx = -1;
    while(true){
        if(!schema_ptr && cur_query_parent != -1){
            Executor* parent_query = ctx.executors_call_stack_[cur_query_parent]; 
            schema_ptr = parent_query->output_schema_;
            cur_query_idx = parent_query->query_idx_;
            cur_query_parent = parent_query->parent_query_idx_;
            continue;
        } else if(!schema_ptr){
            std::cout << "[ERROR] Cant access field name without schema " << field << std::endl;
            this_exec->error_status_ = 1;
            ctx.error_status_ = Error::QUERY_NOT_SUPPORTED; // TODO: make a better error_status_.
            return Value();
        }
        int num_of_matches = 0;
        auto columns = schema_ptr->getColumns();
        for(size_t i = 0; i < columns.size(); ++i){
            std::vector<std::string> splittedStr = strSplit(columns[i].getName(), '.');
            if(splittedStr.size() != 2) {
                std::cout << "[ERROR] Invalid schema " << std::endl;
                this_exec->error_status_ = 1;
                ctx.error_status_ = Error::QUERY_NOT_SUPPORTED; // TODO: make a better error_status_.
                return Value();
            }
            if(field == splittedStr[1]){
                num_of_matches++;
                idx = i;
            }
        }
        if(num_of_matches > 1){
            std::cout << "[ERROR] Ambiguous field name: " << field << std::endl;
            this_exec->error_status_ = 1;
            ctx.error_status_ = Error::QUERY_NOT_SUPPORTED; // TODO: make a better error_status_.
            return Value();
        }
        if(this_exec->type_ == FILTER_EXECUTOR){ // TODO: check why only filter executor type has field renames?
            // if it doesn't match any fields check for renames.
            if(idx < 0){
                for(int i = 0; i < ((FilterExecutor*)this_exec)->field_names_.size(); ++i){
                    if(field == ((FilterExecutor*)this_exec)->field_names_[i]) 
                        return evaluate_expression(ctx, ((FilterExecutor*)this_exec)->fields_[i], this_exec);
                }
            }
        }
        // can't find the field in current context,
        // search for it in context of the parent till the top level query.
        if(num_of_matches == 0 && cur_query_parent != -1){
            Executor* parent_query = ctx.executors_call_stack_[cur_query_parent]; 
            schema_ptr = parent_query->output_schema_;
            cur_query_idx = parent_query->query_idx_;
            cur_query_parent = ctx.executors_call_stack_[cur_query_idx]->parent_query_idx_;
            continue;
        }
        std::vector<Value> cur_output;
        if(this_exec->type_ == FILTER_EXECUTOR){ // TODO: test why do we need to do this?.
            Executor* cur_exec = nullptr;
            if(cur_query_idx == this_exec->query_idx_){
                cur_exec  = this_exec;
            } else {
                Executor* cur_exec = ctx.executors_call_stack_[cur_query_idx];

                while(cur_exec != nullptr && cur_exec->type_ != FILTER_EXECUTOR){
                    cur_exec = cur_exec->child_executor_;
                }
            }
            if(!cur_exec){
                std::cout << "[ERROR] Invalid filter operation"<< std::endl;
                this_exec->error_status_ = 1;
                ctx.error_status_ = Error::QUERY_NOT_SUPPORTED; // TODO: better error handling.
                return Value();
            }

           cur_output  = cur_exec->output_;
        } else {
            //if(cur_query_parent == -1)
                cur_output = this_exec->output_;
            //else
             //   cur_output = ctx.executors_call_stack_[cur_query_idx]->output_; 
        }

        if(idx < 0 || idx >= cur_output.size()) {
            std::string prefix = AGG_FUNC_IDENTIFIER_PREFIX;
            if(field.rfind(prefix, 0) == 0)
                std::cout << "[ERROR] aggregate functions should not be used in here"<< std::endl;
            else 
                std::cout << "[ERROR] Invalid field name " << field << std::endl;
            this_exec->error_status_ = 1;
            ctx.error_status_ = Error::QUERY_NOT_SUPPORTED; // TODO: better error handling.
            return Value();
        }
        return cur_output[idx];
    }
}

Value evaluate_scoped_field(QueryCTX& ctx, Executor* this_exec, ASTNode* item) {

    std::string field = item->token_.val_;
    //TableSchema* schema_ptr = output_schema_;
    TableSchema* schema_ptr = this_exec->output_schema_;
    int cur_query_idx = this_exec->query_idx_;
    //int cur_query_parent = parent_query_idx_;
    
    int cur_query_parent = this_exec->parent_query_idx_;
    int idx = -1;
    while(true){
        if(!schema_ptr && cur_query_parent != -1){
            Executor* parent_query = ctx.executors_call_stack_[cur_query_parent]; 
            schema_ptr = parent_query->output_schema_;
            cur_query_idx = parent_query->query_idx_;
            cur_query_parent = parent_query->parent_query_idx_;
            continue;
        } else if(!schema_ptr){
            std::cout << "[ERROR] Cant access field name without schema " << field << std::endl;
            this_exec->error_status_ = 1;
            ctx.error_status_ = Error::QUERY_NOT_SUPPORTED; // TODO: better error handling.
            return Value();
        }
        std::string table = schema_ptr->getTableName();
        table = reinterpret_cast<ScopedFieldNode*>(item)->table_->token_.val_;
        std::string col = table;col += "."; col+= field;
        idx = schema_ptr->colExist(col);

        if(idx < 0 && cur_query_parent != -1){
            Executor* parent_query = ctx.executors_call_stack_[cur_query_parent]; 
            schema_ptr = parent_query->output_schema_;
            cur_query_idx = parent_query->query_idx_;
            cur_query_parent = ctx.executors_call_stack_[cur_query_idx]->parent_query_idx_;
            continue;
        }

        std::vector<Value> cur_output;
        if(this_exec->type_ == FILTER_EXECUTOR){
            Executor* cur_exec = nullptr;
            if(cur_query_idx == this_exec->query_idx_){
                cur_exec = this_exec;
            } else {
                cur_exec = ctx.executors_call_stack_[cur_query_idx];

                while(cur_exec && cur_exec->type_ != SEQUENTIAL_SCAN_EXECUTOR && cur_exec->type_ != PRODUCT_EXECUTOR){
                    cur_exec = cur_exec->child_executor_;
                }
            }
            if(!cur_exec){
                std::cout << "[ERROR] Invalid scoped operation"<< std::endl;
                this_exec->error_status_ = 1;
                ctx.error_status_ = Error::QUERY_NOT_SUPPORTED; // TODO: better error handling.
                return Value();
            }
                cur_output  = cur_exec->output_;
        } else if(this_exec->type_ == PROJECTION_EXECUTOR) {
            if(this_exec->query_idx_ == 0)
                cur_output = this_exec->output_;
            else
                cur_output = ctx.executors_call_stack_[cur_query_idx]->output_; 
        } else {
                cur_output = this_exec->output_;
        }


        if(idx < 0 || idx >= cur_output.size()) {
            std::cout << "[ERROR] Invalid scoped field name " << col << std::endl;
            this_exec->error_status_ = 1;
            ctx.error_status_ = Error::QUERY_NOT_SUPPORTED; // TODO: better error handling.
            return Value();
        }
        return cur_output[idx];
    }
}

Value evaluate(QueryCTX& ctx, Executor* this_exec, ASTNode* item){
    if(item->category_ == FIELD) return evaluate_field(ctx, this_exec, item);
    if(item->category_ == SCOPED_FIELD) return evaluate_scoped_field(ctx, this_exec, item);
    if(item->category_ == SUB_QUERY) return evaluate_scoped_field(ctx, this_exec, item);
    std::cout << "?????[ERROR] Item type is not supported!" << std::endl;
    // error_status_ = 1;
    ctx.error_status_ = Error::QUERY_NOT_SUPPORTED; // TODO: put a better error_status_.
    return Value();
}
