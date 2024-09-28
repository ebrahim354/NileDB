#include "parser.cpp"
#include "catalog.cpp"


Value evaluate_item(ASTNode* item, TableSchema* schema, std::vector<Value>& values){
    if(item->category_ == FIELD) {
        std::string field = item->token_.val_;
        int idx = schema->colExist(field);
        if(idx < 0 || idx >= values.size()) {
            // TODO: check that in the Algebra Engine not here.
            std::cout << "[ERROR] Invalid field name " << field << std::endl;
            return Value("");
        }
       return values[idx];
    }
    if(item->category_ == STRING_CONSTANT){
        // trim the double quotes. Should've been done by the tokenizer but whatever.
        std::string val = "";
        for(int i = 1; i < val.size() - 1; i++){
            val += item->token_.val_[i];
        }
        return Value(val);
    }
    if(item->category_ == INTEGER_CONSTANT)
        return Value(str_to_int(item->token_.val_));
    std::cout << "[ERROR] Item type is not supported!" << std::endl;
    return Value("");
} 

Value evaluate_expression(ASTNode* expression, TableSchema* schema, std::vector<Value>& values, bool only_one = true) {
    switch(expression->category_){
        case EXPRESSION  : {
                               ExpressionNode* ex = reinterpret_cast<ExpressionNode*>(expression);
                               return evaluate_expression(ex->cur_, schema, values, false);
                           }
        case OR  : {
                       OrNode* lor = reinterpret_cast<OrNode*>(expression);
                       ASTNode* ptr = lor;
                       Value lhs; 
                       while(ptr){
                           lhs = evaluate_expression(lor->cur_, schema, values);
                           if(lhs.getBoolVal() != 0) break;
                           ptr = lor->next_;
                           if(!ptr) break;
                           if(ptr->category_ == OR){
                               lor = reinterpret_cast<OrNode*>(ptr);
                           } else {
                               lhs = evaluate_expression(ptr, schema, values);
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
                           lhs = evaluate_expression(land->cur_, schema, values);
                           if(lhs.getBoolVal() == 0) break;
                           ptr = land->next_;
                           if(!ptr) break;
                           if(ptr->category_ == AND){
                               land = reinterpret_cast<AndNode*>(land->next_);
                           } else {
                               lhs = evaluate_expression(ptr, schema, values);
                               break;
                           }
                       }
                       return lhs.getBoolVal() == 0 ? Value(false) : Value(true);
                   } 
        case EQUALITY : {
                            EqualityNode* eq = reinterpret_cast<EqualityNode*>(expression);
                            std::string op = eq->token_.val_;
                            Value lhs = evaluate_expression(eq->cur_, schema, values);
                            ASTNode* ptr = eq->next_;
                            while(ptr){
                                Value rhs = evaluate_expression(ptr, schema, values);
                                if(op == "=" && lhs == rhs) lhs = Value(true);
                                else if(op == "=" && lhs != rhs) lhs = Value(false);

                                if(op == "!=" && lhs == rhs) lhs = Value(false);
                                else if(op == "!=" && lhs != rhs) lhs = Value(true);

                                if(ptr->category_ == EQUALITY) {
                                    EqualityNode* tmp = reinterpret_cast<EqualityNode*>(ptr);
                                    op = ptr->token_.val_;
                                    ptr = tmp->next_;
                                } else break;
                            }
                            return lhs;
                        } 
        case COMPARISON : {
                              ComparisonNode* comp = reinterpret_cast<ComparisonNode*>(expression);
                              std::string op = comp->token_.val_;
                              Value lhs = evaluate_expression(comp->cur_, schema, values);
                              ASTNode* ptr = comp->next_;
                              while(ptr){
                                  Value rhs = evaluate_expression(ptr, schema, values);

                                  if(op == ">" && lhs > rhs ) lhs = Value(true);
                                  else if(op == ">") lhs = Value(false);

                                  if(op == "<" && lhs < rhs) return Value(true);
                                  else if(op == "<") lhs = Value(false);

                                  if(op == ">=" && lhs >= rhs) return Value(true);
                                  else if(op == ">=") lhs = Value(false);

                                  if(op == "<=" && lhs <= rhs) lhs = Value(true);
                                  else if(op == "<=")  lhs = Value(false);

                                  //lhs = compareStr(lhs, rhs, op);
                                  if(ptr->category_ == COMPARISON){
                                      ComparisonNode* tmp = reinterpret_cast<ComparisonNode*>(ptr);
                                      op = ptr->token_.val_;
                                      ptr = tmp->next_;
                                  } else break;
                              }
                              return lhs;
                          } 
        case TERM : {
                        TermNode* t = reinterpret_cast<TermNode*>(expression);
                        Value lhs = evaluate_expression(t->cur_, schema, values, t->cur_->category_ == TERM);
                        if(only_one) return lhs;
                        std::string op = t->token_.val_;
                        ASTNode* ptr = t->next_;
                        while(ptr){
                            Value rhs = evaluate_expression(ptr, schema, values, ptr->category_ == TERM);
                            int lhs_num = lhs.getIntVal();
                            int rhs_num = rhs.getIntVal();
                            if(op == "+") lhs_num += rhs_num; 
                            if(op == "-") lhs_num -= rhs_num;
                            lhs = Value(lhs_num);
                            if(ptr->category_ == TERM){
                                TermNode* tmp = reinterpret_cast<TermNode*>(ptr);
                                op = ptr->token_.val_;
                                ptr = tmp->next_;
                            } else break;
                        }
                        return lhs;
                    } 
        case FACTOR : {
                          FactorNode* f = reinterpret_cast<FactorNode*>(expression);
                          Value lhs = evaluate_expression(f->cur_, schema, values, f->cur_->category_ == FACTOR);
                          if(only_one) return lhs;
                          std::string op = f->token_.val_;
                          ASTNode* ptr = f->next_;
                          while(ptr){
                              Value rhs = evaluate_expression(ptr, schema, values, ptr->category_ == FACTOR);
                              int lhs_num = lhs.getIntVal();
                              int rhs_num = rhs.getIntVal();
                              if(op == "*") lhs_num *= rhs_num; 
                              if(op == "/" && rhs_num != 0) lhs_num /= rhs_num;
                              lhs = Value(lhs_num);
                              if(ptr->category_ == FACTOR){
                                  FactorNode* tmp = reinterpret_cast<FactorNode*>(ptr);
                                  op = ptr->token_.val_;
                                  ptr = tmp->next_;
                              } else break;
                          }
                          return lhs;
                      } 
        case UNARY : {
                         UnaryNode* u = reinterpret_cast<UnaryNode*>(expression);
                         Value cur = evaluate_expression(u->cur_, schema, values);
                         return Value(cur.getIntVal()*-1);
                     } 
        default:{
                    return evaluate_item(expression, schema, values);
                }
    }
}
