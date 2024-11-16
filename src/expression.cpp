#pragma once

#include "parser.cpp"
#include <string>
#include <unordered_map>

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
{{"ABS", abs_func}, {"COALESCE", coalesce_func}};

Value evaluate_expression(ASTNode* expression, std::function<Value(ASTNode*)>evaluator, bool only_one = true) {
    switch(expression->category_){
        case EXPRESSION  : 
            {
                ExpressionNode* ex = reinterpret_cast<ExpressionNode*>(expression);
                return evaluate_expression(ex->cur_, evaluator, false);
            }
        case CASE_EXPRESSION  : 
            {
                CaseExpressionNode* case_ex = reinterpret_cast<CaseExpressionNode*>(expression);
                Value initial_value;
                if(case_ex->initial_value_){
                    initial_value = evaluate_expression(case_ex->initial_value_, evaluator);
                }
                for(auto& [when, then] : case_ex->when_then_pairs_){
                    Value evaluated_when = evaluate_expression(when, evaluator);
                    if(case_ex->initial_value_ && evaluated_when == initial_value){
                        return evaluate_expression(then, evaluator);
                    }
                    else if(!case_ex->initial_value_ && evaluated_when.getBoolVal()) 
                        return evaluate_expression(then, evaluator);
                }
                if(case_ex->else_) return evaluate_expression(case_ex->else_, evaluator);
                return Value(NULL_TYPE);
            }
        case BETWEEN  : 
            {
                BetweenNode* between = reinterpret_cast<BetweenNode*>(expression);
                Value val = evaluate_expression(between->val_, evaluator, false);
                Value lhs = evaluate_expression(between->lhs_, evaluator, false);
                Value rhs = evaluate_expression(between->rhs_, evaluator, false);
                bool answer = false;
                if(val >= lhs && val <= rhs) answer = true;
                if(between->negated_) answer = !answer;
                return Value(answer);
            }
        case NOT  : 
            {
                NotNode* lnot = reinterpret_cast<NotNode*>(expression);
                Value val = evaluate_expression(lnot->cur_, evaluator);
                if(val.isNull()) return val; 
                return Value((bool)(lnot->effective_^val.getBoolVal())); // 1 1 = 0, 1 0 = 1, 0 1 = 1, 0 0 = 0
            }
        case OR  : 
            {
                OrNode* lor = reinterpret_cast<OrNode*>(expression);
                ASTNode* ptr = lor;
                Value lhs; 
                while(ptr){
                    lhs = evaluate_expression(lor->cur_, evaluator);
                    if(lhs.getBoolVal() != 0) break;
                    ptr = lor->next_;
                    if(!ptr) break;
                    if(ptr->category_ == OR){
                        lor = reinterpret_cast<OrNode*>(ptr);
                    } else {
                        lhs = evaluate_expression(ptr, evaluator);
                        break;
                    };
                }
                return lhs.getBoolVal() != 0 ? Value(true) : Value(false);
            }
        case AND : 
            {
                AndNode* land = reinterpret_cast<AndNode*>(expression);
                ASTNode* ptr = land;
                Value lhs;
                while(ptr){
                    lhs = evaluate_expression(land->cur_, evaluator);
                    if(lhs.getBoolVal() == 0) break;
                    ptr = land->next_;
                    if(!ptr) break;
                    if(ptr->category_ == AND){
                        land = reinterpret_cast<AndNode*>(land->next_);
                    } else {
                        lhs = evaluate_expression(ptr, evaluator);
                        break;
                    }
                }
                return lhs.getBoolVal() == 0 ? Value(false) : Value(true);
            } 
        case EQUALITY : 
            {
                EqualityNode* eq = reinterpret_cast<EqualityNode*>(expression);
                TokenType op = eq->token_.type_;
                Value lhs = evaluate_expression(eq->cur_, evaluator);
                ASTNode* ptr = eq->next_;
                while(ptr){
                    Value rhs = evaluate_expression(ptr, evaluator);
                    bool is_or_isnot = (op == TokenType::IS || op == TokenType::ISNOT);
                    if((lhs.isNull() || rhs.isNull()) && !is_or_isnot) return Value(NULL_TYPE); 
                    if(op == TokenType::IS)    op = TokenType::EQ;
                    if(op == TokenType::ISNOT) op = TokenType::NEQ;

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
                Value lhs = evaluate_expression(comp->cur_, evaluator, comp->cur_->category_ == COMPARISON);
                ASTNode* ptr = comp->next_;
                while(ptr){
                    Value rhs = evaluate_expression(ptr, evaluator, comp->cur_->category_ == COMPARISON);
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
        case TERM : 
            {
                TermNode* t = reinterpret_cast<TermNode*>(expression);
                Value lhs = evaluate_expression(t->cur_, evaluator, t->cur_->category_ == TERM);
                if(only_one) return lhs;
                TokenType op = t->token_.type_;
                ASTNode* ptr = t->next_;
                while(ptr){
                    Value rhs = evaluate_expression(ptr, evaluator, ptr->category_ == TERM);
                    if(lhs.isNull() || rhs.isNull()) return Value(NULL_TYPE);
                    int lhs_num = lhs.getIntVal();
                    int rhs_num = rhs.getIntVal();
                    if(op == TokenType::PLUS) lhs_num += rhs_num; 
                    if(op == TokenType::MINUS) lhs_num -= rhs_num;
                    lhs = Value(lhs_num);
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
                          Value lhs = evaluate_expression(f->cur_, evaluator, f->cur_->category_ == FACTOR);
                          if(only_one) return lhs;
                          TokenType op = f->token_.type_;
                          ASTNode* ptr = f->next_;
                          while(ptr){
                              Value rhs = evaluate_expression(ptr, evaluator, ptr->category_ == FACTOR);
                              if(lhs.isNull() || rhs.isNull()) return Value(NULL_TYPE);
                              int lhs_num = lhs.getIntVal();
                              int rhs_num = rhs.getIntVal();
                              if(op == TokenType::STAR) lhs_num *= rhs_num; 
                              if(op == TokenType::SLASH && rhs_num != 0) lhs_num /= rhs_num;
                              lhs = Value(lhs_num);
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
                          Value cur = evaluate_expression(u->cur_, evaluator);
                          if(cur.isNull()) return cur;
                          if(u->token_.type_ == TokenType::MINUS){
                              return Value(cur.getIntVal()*-1);
                          }
                          return cur;
                      } 
        case SCALAR_FUNC: 
                      {
                          ScalarFuncNode* sfn = reinterpret_cast<ScalarFuncNode*>(expression);
                          if(!reserved_functions.count(sfn->name_)){
                              // TODO: this check should be in the query validation phase.
                              std::cout << "[ERROR] undefined function call " << sfn->name_ << "\n";
                              return Value();
                          }
                          std::vector<Value> vals;
                          for(int i = 0; i < sfn->args_.size(); ++i){
                            vals.emplace_back(evaluate_expression(sfn->args_[i], evaluator));
                          }
                          return reserved_functions[sfn->name_](vals);
                      } 
        case TYPE_CAST: 
                      {
                          TypeCastNode* cast = reinterpret_cast<TypeCastNode*>(expression);
                          Value val = evaluate_expression(cast->exp_, evaluator);
                          if(val.type_ == NULL_TYPE) return val; // NULL values can't be casted? 
                          val.type_ = cast->type_; // TODO: do more usefull type casting.
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
        case INTEGER_CONSTANT: 
                      {
                          return Value(str_to_int(expression->token_.val_));
                      }
        case NULL_CONSTANT: 
                      {
                          return Value(NULL_TYPE);
                      }
        default:
                      {
                          return evaluator(expression);
                      }
    }
}


