#ifndef EXPRESSION_H
#define EXPRESSION_H

#include "../parser.cpp"
#include "executor.h"
#include "utils.h"
#include <string>
#include <unordered_map>

Value evaluate(QueryCTX* ctx, const Tuple& cur_tuple, ASTNode* item);
Value evaluate_subquery(QueryCTX* ctx, const Tuple& cur_tuple, ASTNode* item);


Value abs_func(Vector<Value> vals){
    if(vals.size() != 1){
        std::cout << "[ERROR] Incorrect number of arguments\n";
        return Value();
    }
    int int_val = vals[0].getIntVal(); 
    if(int_val < 0) 
        return Value(-int_val);
    return vals[0];
}

Value nullif_func(Vector<Value> vals){
    if(vals.size() != 2){
        std::cout << "[ERROR] Incorrect number of arguments\n";
        return Value();
    }
    if(vals[0] == vals[1]) return Value(NULL_TYPE);
    return vals[0];
}

Value coalesce_func(Vector<Value> vals){
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
std::unordered_map<String8, std::function<Value(Vector<Value>)>, String_ihash, String_ieq> reserved_functions = 
{{str_lit("ABS"), abs_func}, {str_lit("COALESCE"), coalesce_func}};


Value evaluate_expression(
        QueryCTX* ctx, 
        ASTNode* expression, 
        const Tuple& cur_tuple,
        bool only_one = true,
        bool eval_sub_query = true
        ) {
    switch(expression->category_) {
        case EXPRESSION  : 
            {
                ExpressionNode* ex = reinterpret_cast<ExpressionNode*>(expression);
                return evaluate_expression(ctx, ex->cur_, cur_tuple, false, eval_sub_query);
            }
        case CASE_EXPRESSION  : 
            {
                CaseExpressionNode* case_ex = reinterpret_cast<CaseExpressionNode*>(expression);
                Value initial_value;
                if(case_ex->initial_value_){
                    initial_value = evaluate_expression(ctx, case_ex->initial_value_, cur_tuple, true, eval_sub_query);
                }
                for(auto& [when, then] : case_ex->when_then_pairs_){
                    Value evaluated_when = evaluate_expression(ctx, when, cur_tuple, true, eval_sub_query);
                    if(evaluated_when.isNull()) continue;
                    if(case_ex->initial_value_ && !initial_value.isNull() && evaluated_when == initial_value){
                        return evaluate_expression(ctx, then, cur_tuple, true, eval_sub_query);
                    }
                    else if(!case_ex->initial_value_ && evaluated_when.getBoolVal()) 
                        return evaluate_expression(ctx, then, cur_tuple, true, eval_sub_query);
                }
                if(case_ex->else_) return evaluate_expression(ctx, case_ex->else_, cur_tuple, true, eval_sub_query);
                return Value(NULL_TYPE);
            }
        case NULLIF_EXPRESSION  : 
            {
                NullifExpressionNode* nullif_ex = reinterpret_cast<NullifExpressionNode*>(expression);
                assert(nullif_ex->lhs_ && nullif_ex->rhs_); // if you are here they must not be null pointers.

                Value lhs_val = evaluate_expression(ctx, nullif_ex->lhs_, cur_tuple, true, eval_sub_query);
                // lhs is already null no need to check the rhs.
                if(lhs_val.isNull()) return Value(NULL_TYPE); 
                Value rhs_val = evaluate_expression(ctx, nullif_ex->rhs_, cur_tuple, true, eval_sub_query);

                if(rhs_val.isNull() || lhs_val != rhs_val)
                    return lhs_val;
                return Value(NULL_TYPE);
            }
        case IN  : 
            {
                InNode* in = reinterpret_cast<InNode*>(expression);
                Value val = evaluate_expression(ctx, in->val_, cur_tuple, false, eval_sub_query);
                //if(val.isNull()) return val;
                bool answer = false;
                bool null_ret = false;
                for(int i = 0; i < in->list_.size(); ++i){
                    Value tmp = evaluate_expression(ctx, in->list_[i], cur_tuple, false, false);
                    if(tmp.type_ == Type::EXECUTOR_ID){
                        int idx = tmp.getIntVal();
                        Executor* sub_query_executor = ctx->executors_call_stack_[idx]; 
                        sub_query_executor->init();
                        // TODO: register errors to ctx object and stop execution.
                        if(sub_query_executor->error_status_){
                            std::cout << "[ERROR] could not initialize sub-query" << std::endl;
                            return Value();
                        }

                        while(!answer && !sub_query_executor->finished_ && !sub_query_executor->error_status_){
                            Tuple sub_query_output = sub_query_executor->next();
                            if(sub_query_output.size() == 0 && sub_query_executor->finished_) {
                                break;
                            }

                            if(sub_query_executor->error_status_) {
                                std::cout << "[ERROR] could not execute sub-query" << std::endl;
                                ctx->error_status_ = (Error)1; // TODO: better error handling.
                                return Value();
                            }

                            if(sub_query_output.size() != 1) {
                                std::cout << "[ERROR] sub-query should return exactly 1 column" << std::endl;
                                ctx->error_status_ = (Error)1; // TODO: better error handling.
                                return Value();
                            }
                            if(sub_query_output.get_val_at(0).isNull()){
                                null_ret = true;
                            }
                            else if(val == sub_query_output.get_val_at(0)){
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
                Value val = evaluate_expression(ctx, between->val_, cur_tuple, false, eval_sub_query);
                if(val.isNull()) return val; 

                Value lhs = evaluate_expression(ctx, between->lhs_, cur_tuple, false, eval_sub_query);
                Value rhs = evaluate_expression(ctx, between->rhs_, cur_tuple, false, eval_sub_query);

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
                Value val = evaluate_expression(ctx, lnot->cur_, cur_tuple, true, eval_sub_query);
                if(val.isNull()) return val; 
                return Value((bool)(lnot->effective_^val.getBoolVal())); // 1 1 = 0, 1 0 = 1, 0 1 = 1, 0 0 = 0
            }
        case OR  : 
            {
                OrNode* lor = reinterpret_cast<OrNode*>(expression);
                ASTNode* ptr = lor;
                Value lhs; 
                while(ptr){
                    lhs = evaluate_expression(ctx, lor->cur_, cur_tuple, true, eval_sub_query);
                    if(lhs.getBoolVal() != 0) break;
                    ptr = lor->next_;
                    if(!ptr) break;
                    if(ptr->category_ == OR){
                        lor = reinterpret_cast<OrNode*>(ptr);
                    } else {
                        lhs = evaluate_expression(ctx, ptr, cur_tuple, true, eval_sub_query);
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
                    lhs = evaluate_expression(ctx, land->cur_, cur_tuple, true, eval_sub_query);
                    if(lhs.isNull()) break; 
                    if(lhs.getBoolVal() == 0) break;
                    ptr = land->next_;
                    if(!ptr) break;
                    if(ptr->category_ == AND){
                        land = reinterpret_cast<AndNode*>(land->next_);
                    } else {
                        lhs = evaluate_expression(ctx, ptr, cur_tuple, true, eval_sub_query);
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
                Value lhs = evaluate_expression(ctx, eq->cur_, cur_tuple, 
                        eq->cur_->category_ == EQUALITY, 
                        eval_sub_query);
                ASTNode* ptr = eq->next_;
                while(ptr){
                    Value rhs = evaluate_expression(ctx, ptr, cur_tuple, 
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
                Value lhs = evaluate_expression(ctx, comp->cur_, cur_tuple, 
                        comp->cur_->category_ == COMPARISON, eval_sub_query);
                ASTNode* ptr = comp->next_;
                while(ptr){
                    Value rhs = evaluate_expression(ctx, ptr, cur_tuple, 
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
                if(eval_sub_query) return evaluate_subquery(ctx, cur_tuple, expression);
                auto sub_query = reinterpret_cast<SubQueryNode*>(expression);
                auto v = Value(sub_query->idx_);
                v.type_ = Type::EXECUTOR_ID;
                return v;
            }
        case TERM : 
            {
                TermNode* t = reinterpret_cast<TermNode*>(expression);
                Value lhs = evaluate_expression(ctx, t->cur_, cur_tuple, 
                        t->cur_->category_ == TERM, eval_sub_query);
                if(only_one) return lhs;
                TokenType op = t->token_.type_;
                ASTNode* ptr = t->next_;
                while(ptr){
                    Value rhs = evaluate_expression(ctx, ptr, cur_tuple, ptr->category_ == TERM, eval_sub_query);
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
                          Value lhs = evaluate_expression(ctx, f->cur_, cur_tuple, 
                                  f->cur_->category_ == FACTOR, eval_sub_query);
                          if(only_one) return lhs;
                          TokenType op = f->token_.type_;
                          ASTNode* ptr = f->next_;
                          while(ptr){
                              Value rhs = evaluate_expression(ctx, ptr, cur_tuple, 
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
                          Value cur = evaluate_expression(ctx, u->cur_, cur_tuple, true, eval_sub_query);
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
                          if(!reserved_functions.count(sfn->name_)){
                              // TODO: this check should be in the query validation phase.
                              std::cout << "[ERROR] undefined function call" << "\n";
                              return Value();
                          }
                          Vector<Value> vals;
                          for(int i = 0; i < sfn->args_.size(); ++i){
                              vals.emplace_back(evaluate_expression(ctx, sfn->args_[i], cur_tuple, true, eval_sub_query));
                          }
                          return reserved_functions[sfn->name_](vals);
                      } 
        case TYPE_CAST: 
                      {
                          TypeCastNode* cast = reinterpret_cast<TypeCastNode*>(expression);
                          Value val = evaluate_expression(ctx, cast->exp_, cur_tuple, true, eval_sub_query);
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
                          return Value(expression->token_.val_);
                      }
        case FLOAT_CONSTANT:
                      {
                          double val  = str_to_f64(expression->token_.val_);
                          if(val <= MAX_F32) return Value((float) val);
                          return Value(val);
                      }
        case INTEGER_CONSTANT: 
                      {
                          i64 val = str_to_i64(expression->token_.val_);
                          if(val <= MAX_I32) return Value((i32) val);
                          return Value((i64)val);
                      }
        case NULL_CONSTANT: 
                      {
                          return Value(NULL_TYPE);
                      }
        case FIELD_EXPR: 
                      {
                          FieldNode* field = ((FieldNode*)expression);
                          assert(field->offset_ < cur_tuple.size());
                          assert(field->offset_ >= 0);
                          return cur_tuple.get_val_at(field->offset_);
                      }
        default:
                      {
                          assert(0);
                      }
    }
}



// assumes top level ands only.
Vector<ExpressionNode*> split_by_and(QueryCTX* ctx, ExpressionNode* expression) {
    ExpressionNode* ex = reinterpret_cast<ExpressionNode*>(expression);
    if(!ex) return {};

    ASTNode* ptr = ex->cur_;
    Vector<ExpressionNode*> ret;
    while(ptr){
        // TODO: should be changed.
        //ExpressionNode* ex_copy = new ExpressionNode(ex->top_level_statement_, ex->query_idx_);
        ExpressionNode* ex_copy = nullptr; 
        ALLOCATE_INIT(ctx->arena_, ex_copy, ExpressionNode, ex->top_level_statement_, ex->query_idx_);
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


void accessed_tables(ASTNode* expression ,Vector<String8>& tables, Catalog* catalog, bool only_one = true) {
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
        case FIELD_EXPR:{
                       FieldNode* field = (FieldNode*)expression;
                       // if the field explicitly has the table e.g: foo.bar
                       if(field->table_ != nullptr){
                           tables.push_back(field->table_->token_.val_);
                           return;
                       }
                       // if the field does not explicitly have the table e.g: bar
                       String8 field_name = field->token_.val_;
                       Vector<String8> valid_tables = catalog->get_tables_by_field((field_name));
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

void accessed_fields(ASTNode* expression ,Vector<FieldNode*>& fields, bool only_one = true) {
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
        case FIELD_EXPR:
                    {
                       fields.push_back((FieldNode*)expression);
                       return;
                   }
        case AGG_FUNC:{
                          AggregateFuncNode* func = (AggregateFuncNode*)expression;
                          accessed_fields(func->exp_, fields);
                          return;
                      }
        case STRING_CONSTANT: 
        case FLOAT_CONSTANT: 
        case INTEGER_CONSTANT: 
        case NULL_CONSTANT: 
        case SUB_QUERY: 
                      return;
        default:
                      assert(0);
                      return;
    }
}

Value evaluate_subquery(QueryCTX* ctx, const Tuple& cur_tuple, ASTNode* item) {
    auto sub_query = reinterpret_cast<SubQueryNode*>(item);
    bool used_with_exists = sub_query->used_with_exists_;
    Executor* sub_query_executor = ctx->executors_call_stack_[sub_query->idx_]; 
    ctx->query_inputs.push_back(cur_tuple);
    sub_query_executor->init();
    if(sub_query_executor->error_status_){
        std::cout << "[ERROR] could not initialize sub-query" << std::endl;
        ctx->error_status_ = Error::QUERY_NOT_SUPPORTED; // TODO: put a better error_status_.
        return Value();
    }

    Tuple tmp = sub_query_executor->next();
    if(tmp.size() == 0 && sub_query_executor->finished_) {
        ctx->query_inputs.pop_back();
        if(used_with_exists) 
            return Value(false);
        return Value();
    }

    if(sub_query_executor->error_status_) {
        std::cout << "[ERROR] could not execute sub-query" << std::endl;
        ctx->error_status_ = Error::QUERY_NOT_SUPPORTED; // TODO: put a better error_status_.
        return Value();
    }
    if(used_with_exists) return Value(tmp.size() != 0);
    if(tmp.size() != 1) {
        std::cout << "[ERROR] sub-query should return exactly 1 column" << std::endl;
        ctx->error_status_ = Error::QUERY_NOT_SUPPORTED; // TODO: put a better error_status_.
        return Value();
    }
    ctx->query_inputs.pop_back();
    return tmp.get_val_at(0);
}

#endif // EXPRESSION_H
