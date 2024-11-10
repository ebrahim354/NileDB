#pragma once

#include "parser.cpp"



// abstract class
class FieldEvaluator {
    public:
        virtual Value evaluate(ASTNode* field) = 0;
};

Value(*fn)(ASTNode*)  = nullptr;

/*
class ClassicEvaluator: public FieldEvaluator {
    public:
    TableSchema* schema = nullptr;
    std::vector<Value> values = {};

    Value evaluate(ASTNode* item){
        if(item->category_ == FIELD) {
            std::string field = item->token_.val_;
            if(!schema) {
                std::cout << "[ERROR] Invalid field name " << field << std::endl;
                return Value();
            }
            int idx = schema->colExist(field);
            if(idx < 0 || idx >= values.size()) {
                // TODO: check that in the Algebra Engine not here.
                std::cout << "[ERROR] Invalid field name " << field << std::endl;
                return Value();
            }
            return values[idx];
        }
        std::cout << "[ERROR] Item type is not supported!" << std::endl;
        return Value();
    } 
};*/


Value evaluate_expression(ASTNode* expression, std::function<Value(ASTNode*)>evaluator, bool only_one = true) {
    switch(expression->category_){
        case EXPRESSION  : {
                               ExpressionNode* ex = reinterpret_cast<ExpressionNode*>(expression);
                               return evaluate_expression(ex->cur_, evaluator, false);
                           }
        case CASE_EXPRESSION  : {
                               CaseExpressionNode* case_ex = reinterpret_cast<CaseExpressionNode*>(expression);
                               for(auto& [when, then] : case_ex->when_then_pairs_){
                                   if(evaluate_expression(when, evaluator).getBoolVal()) 
                                       return evaluate_expression(then, evaluator);
                               }
                               if(case_ex->else_) return evaluate_expression(case_ex->else_, evaluator);
                               return Value(NULL_TYPE);
                           }
        case OR  : {
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
        case AND : {
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
        case EQUALITY : {
                            EqualityNode* eq = reinterpret_cast<EqualityNode*>(expression);
                            TokenType op = eq->token_.type_;
                            Value lhs = evaluate_expression(eq->cur_, evaluator);
                            ASTNode* ptr = eq->next_;
                            while(ptr){
                                Value rhs = evaluate_expression(ptr, evaluator);
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
        case COMPARISON : {
                              ComparisonNode* comp = reinterpret_cast<ComparisonNode*>(expression);
                              TokenType op = comp->token_.type_;
                              Value lhs = evaluate_expression(comp->cur_, evaluator, comp->cur_->category_ == COMPARISON);
                              ASTNode* ptr = comp->next_;
                              while(ptr){
                                  Value rhs = evaluate_expression(ptr, evaluator, comp->cur_->category_ == COMPARISON);

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
        case TERM : {
                        TermNode* t = reinterpret_cast<TermNode*>(expression);
                        Value lhs = evaluate_expression(t->cur_, evaluator, t->cur_->category_ == TERM);
                        if(only_one) return lhs;
                        TokenType op = t->token_.type_;
                        ASTNode* ptr = t->next_;
                        while(ptr){
                            Value rhs = evaluate_expression(ptr, evaluator, ptr->category_ == TERM);
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
        case UNARY : {
                         UnaryNode* u = reinterpret_cast<UnaryNode*>(expression);
                         Value cur = evaluate_expression(u->cur_, evaluator);
                         if(u->token_.type_ == TokenType::MINUS){
                            return Value(cur.getIntVal()*-1);
                         }
                         return cur;
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
        default:{
                    return evaluator(expression);
                }
    }
}


