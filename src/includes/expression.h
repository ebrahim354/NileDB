#ifndef EXPRESSION_H
#define EXPRESSION_H

#include "../parser.cpp"
//#include "query_ctx.h"
#include "executor.h"
#include "utils.h"
#include <string>
#include <unordered_map>
#include <stack>

Value evaluate_subquery(QueryCTX* ctx, const Tuple& cur_tuple, ASTNode* item);
void get_fields_of_query_deep(QueryCTX& ctx, QueryData* data, Vector<FieldNode*>& fields);
Value evaluate_subquery_flat(QueryCTX* ctx, i32 query_idx);
Value match_with_subquery(QueryCTX* ctx, i32 query_idx, Value val);


Value invalid_func(std::vector<Value>& vals){
    return Value();
}

Value abs_func(std::vector<Value>& vals){
    if(vals.size() != 1){
        std::cout << "[ERROR] Incorrect number of arguments\n";
        return Value();
    }
    int int_val = vals[0].getIntVal(); 
    if(int_val < 0) 
        return Value(-int_val);
    return vals[0];
}

Value coalesce_func(std::vector<Value>& vals){
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

Value nullif_func(std::vector<Value>& vals){
    if(vals.size() != 2){
        std::cout << "[ERROR] Incorrect number of arguments\n";
        return Value();
    }
    Value* lhs_val = &vals[0];
    // lhs is already null no need to check the rhs.
    if(lhs_val->isNull()) return Value(NULL_TYPE); 
    Value* rhs_val = &vals[1]; 

    if(rhs_val->isNull() || *lhs_val != *rhs_val)
        return *lhs_val;
    return Value(NULL_TYPE);
}

//TODO: should be moved the the catalog class, and stored with the meta data to support custom functions.
std::unordered_map<String8, size_t, String_ihash, String_ieq> reserved_functions_indexes = 
{{str_lit("ABS"), 1}, {str_lit("COALESCE"), 2}, {str_lit("NULLIF"), 3}};

std::function<Value(std::vector<Value>&)> reserved_functions [] = {
    invalid_func,
    abs_func,
    coalesce_func,
    nullif_func,
};

enum class ExprOpCode: u8 {
    NOT,
    IS,
    IS_NOT,
    EQ,
    NEQ,
    LT,
    LTE,
    GT,
    GTE,
    SUB_QUERY,
    SUB_QUERY_MATCH,
    ADD,
    SUBTRACT,
    MULTIPLY,
    DEVIDE, 
    NEGATE,  
    TYPE_CAST,
    STRING_CONST,
    FLOAT_CONST,
    INT_CONST,
    NULL_CONST,
    FIELD,

    JUMP,
    JUMP_IF_FALSY,
    JUMP_IF_NULL,

    FUNC_CALL,
    AND,
    OR,
    BETWEEN,
    IN, 
};

struct ExprStep {
    ExprOpCode op_;
    i32 r1, r2, r3; // r1 holds the final result of a step while r2, r3 hold operands.
};

struct FlatExpr {
    // fields used during runtime.
    Vector<ExprStep> steps_;
    Vector<Value> constants_pool_;
    Vector<Value> registers_;
    i32 query_idx_;
    
    // fields used during compilation.
    u32 sp_ = 1;
    u32 max_sp_ = 1;
    std::stack<i32> in_expr_vals_;

    u32 allocate_register() {
        sp_++;
        if(max_sp_ < sp_) max_sp_ = sp_;
        return sp_ - 1;
    }

    void reset_sp(i32 top_reg) {
        assert(top_reg >= 0);
        sp_ = top_reg + 1;
    }

    void push_step(ExprOpCode op, i32 r1 = 0, i32 r2 = 0, i32 r3 = 0) {
        steps_.push_back({op, r1, r2, r3});
    }

    void print(){
        const char* pattern = "--------------------------";
        printf("%-4s   %-11s   %-2s %-2s %-2s\n", "addr", "opcode", "r1", "r2", "r3");
        printf("%.*s   %.*s   %.*s %.*s %.*s\n", 
                4, pattern,
                11, pattern,
                2, pattern,
                2, pattern,
                2, pattern);
        for(int i = 0; i < steps_.size(); ++i) {
            i32 r1 = steps_[i].r1;
            i32 r2 = steps_[i].r2;
            i32 r3 = steps_[i].r3;
            const char* name = nullptr;
            switch(steps_[i].op_){
                case ExprOpCode::NOT: 
                    {
                        name = "Not";
                        break;
                    }
                case ExprOpCode::IS:
                    {
                        name = "Is";
                        break;
                    }
                case ExprOpCode::IS_NOT:
                    {
                        name = "IsNot";
                        break;
                    }
                case ExprOpCode::EQ: 
                    {
                        name = "Eq";
                        break;
                    }
                case ExprOpCode::NEQ: 
                    {
                        name = "NEq";
                        break;
                    }
                case ExprOpCode::LT:
                    {
                        name = "LT";
                        break;
                    }
                case ExprOpCode::LTE:
                    {
                        name = "LTE";
                        break;
                    }
                case ExprOpCode::GT:
                    {
                        name = "GT";
                        break;
                    }
                case ExprOpCode::GTE:
                    {
                        name = "GTE";
                        break;
                    }
                case ExprOpCode::SUB_QUERY:
                    {
                        name = "SubQuery";
                        break;
                    }
                case ExprOpCode::SUB_QUERY_MATCH:
                    {
                        name = "SubQueryMatch";
                        break;
                    }
                case ExprOpCode::ADD:
                    {
                        name = "Add";
                        break;
                    }
                case ExprOpCode::SUBTRACT:
                    {
                        name = "Subtract";
                        break;
                    }
                case ExprOpCode::MULTIPLY:
                    {
                        name = "Multiply";
                        break;
                    }
                case ExprOpCode::DEVIDE:
                    {
                        name = "Devide";
                        break;
                    }
                case ExprOpCode::NEGATE:
                    {
                        name = "Negate";
                        break;
                    }
                case ExprOpCode::TYPE_CAST:
                    {
                        name = "TypeCast";
                        break;
                    }

                case ExprOpCode::STRING_CONST:
                case ExprOpCode::FLOAT_CONST:
                case ExprOpCode::INT_CONST:
                    {
                        name = "Const";
                        break;
                    }
                case ExprOpCode::NULL_CONST:
                    {
                        name = "NULL";
                        break;
                    }
                case ExprOpCode::FIELD:
                    {
                        name = "Field";
                        break;
                    }
                case ExprOpCode::JUMP_IF_FALSY:
                    {
                        name = "JumpIfFalsy";
                        break;
                    }
                case ExprOpCode::JUMP:
                    {
                        name = "Jump";
                        break;
                    }
                case ExprOpCode::FUNC_CALL:
                    {
                        name = "FuncCall";
                        break;
                    }
                case ExprOpCode::AND:
                    {
                        name = "And";
                        break;
                    }
                case ExprOpCode::OR:
                    {
                        name = "Or";
                        break;
                    }
                case ExprOpCode::IN:
                    {
                        name = "In";
                        break;
                    }
                default:;
                    assert(0 && "TODO!");
            }
            printf("%-4d   %-11s   %-2d %-2d %-2d\n", i, name, r1, r2, r3);
        }
    }
};

// turn the AST representation of an expression into a sequence of virtual machine instructions.
// this virtual machine is sort of a mix between register based and stack based VMs.
// returns the the index of the resulting value within the registers_ array,
// either in the out_expr->constants_pool_ or out_expr->registers_,
// the stack machine will know weather the value is in the constants_pool_ or the registers_ based on the op code.
// the stack pointer (sp_) always starts counting from 1.
// any register that holds the value 0 will refer to a constant null value.
// a null value will be put by this function as the first element of the constants_pool_ if it doesn't already exist.
// the schema_ field inside out_expr must be set by the caller of this function before calling it,
// the schema_ field is used to get offsets of variables inside the tuple that will be used during runtime.
// TODO: this VM implementation is very naive and slow and could be vectorized for more performance.
i32 flatten_expression (
        QueryCTX* ctx, 
        ASTNode* in_expr, 
        FlatExpr* out_expr, 
        bool only_one = true) {
    if(out_expr->constants_pool_.size() == 0) 
        out_expr->constants_pool_.emplace_back(NULL_TYPE);

    switch(in_expr->category_) {
        case EXPRESSION  : 
            {
                ExpressionNode* ex = reinterpret_cast<ExpressionNode*>(in_expr);
                return flatten_expression(ctx, ex->cur_, out_expr, false);
            }
        case NOT  : 
            {
                NotNode* lnot = reinterpret_cast<NotNode*>(in_expr);
                if(!lnot->effective_) // only compile it if it's effective
                    return flatten_expression(ctx, lnot->cur_, out_expr, true);

                // pre-allocate the register that's used to store the result.
                i32 r1 = out_expr->allocate_register();
                i32 r2 = flatten_expression(ctx, lnot->cur_, out_expr, true);
                // clear the state that is used by the child expression.
                out_expr->reset_sp(r1);

                out_expr->push_step(ExprOpCode::NOT, r1, r2);
                return r1;
            }
        case EQUALITY : 
            {
                EqualityNode* eq = reinterpret_cast<EqualityNode*>(in_expr);
                TokenType op = eq->token_.type_;

                i32 r1 = out_expr->allocate_register();

                i32 r2 = flatten_expression(ctx, eq->cur_, out_expr, 
                        eq->cur_->category_ == EQUALITY);
                if(only_one) return r2;

                ASTNode* ptr = eq->next_;
                while(ptr){
                    i32 r3 = flatten_expression(ctx, ptr, out_expr, 
                            ptr->category_ == EQUALITY);
                    switch(op){
                        case TokenType::IS :
                            out_expr->push_step(ExprOpCode::IS        , r1, r2, r3);
                        break;
                        case TokenType::ISNOT :
                            out_expr->push_step(ExprOpCode::IS_NOT    , r1, r2, r3);
                        break;
                        case TokenType::EQ :
                            out_expr->push_step(ExprOpCode::EQ        , r1, r2, r3);
                        break;
                        case TokenType::NEQ :
                            out_expr->push_step(ExprOpCode::NEQ       , r1, r2, r3);
                        break;
                        default:
                            assert(0 && "UNREACHABLE!");
                    }
                    out_expr->reset_sp(r1);

                    if(ptr->category_ != EQUALITY) break;
                    r2 = r1;
                    r1 = out_expr->allocate_register();

                    EqualityNode* tmp = reinterpret_cast<EqualityNode*>(ptr);
                    op = ptr->token_.type_;
                    ptr = tmp->next_;
                }
                return r1;
            } 
        case COMPARISON : 
            {
                ComparisonNode* comp = reinterpret_cast<ComparisonNode*>(in_expr);
                TokenType op = comp->token_.type_;

                i32 r1 = out_expr->allocate_register();

                i32 r2 = flatten_expression(ctx, comp->cur_, out_expr, 
                        comp->cur_->category_ == COMPARISON);
                if(only_one) return r2;

                ASTNode* ptr = comp->next_;
                while(ptr){
                    i32 r3 = flatten_expression(ctx, ptr, out_expr, 
                            ptr->category_ == COMPARISON);

                    switch(op){
                        case TokenType::GT :
                            out_expr->push_step(ExprOpCode::GT , r1, r2, r3);
                        break;
                        case TokenType::GTE :
                            out_expr->push_step(ExprOpCode::GTE, r1, r2, r3);
                        break;
                        case TokenType::LT :
                            out_expr->push_step(ExprOpCode::LT , r1, r2, r3);
                        break;
                        case TokenType::LTE :
                            out_expr->push_step(ExprOpCode::LTE, r1, r2, r3);
                        break;
                        default:
                            assert(0 && "UNREACHABLE!");
                    }
                    out_expr->reset_sp(r1);

                    if(ptr->category_ != COMPARISON) break;
                    r2 = r1;
                    r1 = out_expr->allocate_register();

                    ComparisonNode* tmp = reinterpret_cast<ComparisonNode*>(ptr);
                    op = ptr->token_.type_;
                    ptr = tmp->next_;
                }
                return r1;
            } 
        case SUB_QUERY:
            {
                auto sub_query = reinterpret_cast<SubQueryNode*>(in_expr);
                i32 r1 = out_expr->allocate_register();
                i32 r2 = sub_query->idx_; 
                if(out_expr->in_expr_vals_.size())
                    out_expr->push_step(ExprOpCode::SUB_QUERY_MATCH, r1, r2, out_expr->in_expr_vals_.top());
                else
                    out_expr->push_step(ExprOpCode::SUB_QUERY, r1, r2); 
                assert(r2 > 0);
                return r1;
            }
        case TERM : 
            {
                TermNode* t = reinterpret_cast<TermNode*>(in_expr);
                i32 r1 = out_expr->allocate_register();

                i32 r2 = flatten_expression(ctx, t->cur_, out_expr, 
                        t->cur_->category_ == TERM);
                if(only_one) {
                    return r2;
                }


                TokenType op = t->token_.type_;
                ASTNode* ptr = t->next_;
                while(ptr){
                    i32 r3 = flatten_expression(ctx, ptr, out_expr, ptr->category_ == TERM);
                    if(op == TokenType::PLUS) out_expr->push_step(ExprOpCode::ADD     , r1, r2, r3);
                    else if (op == TokenType::MINUS) out_expr->push_step(ExprOpCode::SUBTRACT, r1, r2, r3);
                    else assert(0 && "Unexpected token type!");

                    out_expr->reset_sp(r1);

                    if(ptr->category_ != TERM) break;

                    r2 = r1;
                    r1 = out_expr->allocate_register();

                    TermNode* tmp = reinterpret_cast<TermNode*>(ptr);
                    op = ptr->token_.type_;
                    ptr = tmp->next_;
                }
                return r1;
            } 
        case FACTOR : 
            {
                FactorNode* f = reinterpret_cast<FactorNode*>(in_expr);

                i32 r1 = out_expr->allocate_register();
                i32 r2 = flatten_expression(ctx, f->cur_, out_expr, 
                        f->cur_->category_ == FACTOR);
                if(only_one) {
                    return r2;
                } 
                TokenType op = f->token_.type_;
                ASTNode* ptr = f->next_;
                while(ptr){
                    i32 r3 = flatten_expression(ctx, ptr, out_expr, 
                            ptr->category_ == FACTOR);

                    if(op == TokenType::STAR) 
                        out_expr->push_step(ExprOpCode::MULTIPLY , r1, r2, r3);
                    else if (op == TokenType::SLASH)
                        out_expr->push_step(ExprOpCode::DEVIDE , r1, r2, r3);
                    else assert(0 && "Unexpected TokenType");

                    out_expr->reset_sp(r1);

                    if(ptr->category_ != FACTOR) break;

                    r2 = r1;
                    r1 = out_expr->allocate_register();

                    FactorNode* tmp = reinterpret_cast<FactorNode*>(ptr);
                    op = ptr->token_.type_;
                    ptr = tmp->next_;
                }
                return r1;
            } 
        case UNARY : 
            {
                UnaryNode* u = reinterpret_cast<UnaryNode*>(in_expr);
                i32 r1 = out_expr->allocate_register();
                i32 r2 = flatten_expression(ctx, u->cur_, out_expr, true);
                if(u->token_.type_ == TokenType::MINUS){
                    out_expr->push_step(ExprOpCode::NEGATE , r1, r2);
                }
                out_expr->reset_sp(r1);
                return r1;
            } 
        case TYPE_CAST: 
            {
                TypeCastNode* cast = reinterpret_cast<TypeCastNode*>(in_expr);
                i32 r1 = out_expr->allocate_register(); // to store the result 
                i32 r2 = flatten_expression(ctx, cast->exp_, out_expr, true); // to store the child expr result.
                assert(cast->type_ >= 0);
                i32 r3 = cast->type_;
                out_expr->push_step(ExprOpCode::TYPE_CAST, r1, r2, r3);
                out_expr->reset_sp(r1);
                return r1;
            } 
        case STRING_CONSTANT: 
            {
                i32 r1 = out_expr->allocate_register();
                i32 r2 = out_expr->constants_pool_.size(); 

                out_expr->constants_pool_.emplace_back(in_expr->token_.val_);

                out_expr->push_step(ExprOpCode::STRING_CONST, r1, r2);
                return r1;
            }
        case FLOAT_CONSTANT:
            {
                i32 r1 = out_expr->allocate_register();
                i32 r2 = out_expr->constants_pool_.size(); 

                double val  = str_to_f64(in_expr->token_.val_);
                if(val <= MAX_F32) out_expr->constants_pool_.emplace_back((float) val);
                else out_expr->constants_pool_.emplace_back(val);

                out_expr->push_step(ExprOpCode::FLOAT_CONST, r1, r2);
                return r1;
            }
        case INTEGER_CONSTANT: 
            {
                i32 r1 = out_expr->allocate_register();

                i32 r2 = out_expr->constants_pool_.size(); 

                i64 val = str_to_i64(in_expr->token_.val_);
                if(val <= MAX_I32) out_expr->constants_pool_.emplace_back((i32) val);
                else  out_expr->constants_pool_.emplace_back((i64) val);

                out_expr->push_step(ExprOpCode::INT_CONST, r1, r2);
                return r1;
            }
        case NULL_CONSTANT: 
            {
                i32 r1 = out_expr->allocate_register();
                out_expr->push_step(ExprOpCode::NULL_CONST, r1);
                return r1;
            }
        case FIELD_EXPR: 
            {
                auto field = ((FieldNode*)in_expr);
                assert(field->schema_ != nullptr);
                i32 r1 = out_expr->allocate_register();
                assert(field->schema_ && "field doesn't have any schema provided");
                i32 r2 = field->schema_->col_exist(field->token_.val_, field->table_name_->token_.val_);
                i32 r3 = field->query_idx_;

                assert(r1 >= 0 && "Field does not exist in the provided schema!");
                out_expr->push_step(ExprOpCode::FIELD, r1, r2, r3);
                return r1;
            }
        case CASE_EXPRESSION  : 
            {
                CaseExpressionNode* case_ex = (CaseExpressionNode*)(in_expr);


                std::vector<u32> jump_instructions_idxs;
                // the extra 2 are the optional initial_value and the optional else.
                jump_instructions_idxs.reserve(case_ex->when_then_pairs_.size() + 2); 

                i32 save_point = -1;

                // CASE exp WHEN exp THEN exp ... ELSE exp END.
                if(case_ex->initial_value_){
                    i32 r2 = flatten_expression(ctx, case_ex->initial_value_, out_expr);
                    // the result of every WHEN exp THEN pair must be stored right after this register.
                    save_point = r2;

                    for(auto& [when, then] : case_ex->when_then_pairs_) {
                        out_expr->reset_sp(save_point);
                        
                        i32 r1 = out_expr->allocate_register();
                        i32 r3 = flatten_expression(ctx, when, out_expr);

                        out_expr->push_step(ExprOpCode::EQ, r1, r2, r3);

                        u32 jump_if_falsy_idx = out_expr->steps_.size();

                        out_expr->push_step(ExprOpCode::JUMP_IF_FALSY, r1, 0);

                        out_expr->reset_sp(save_point);
                        // all then results must be be equal to save_point + 1.
                        i32 then_result = flatten_expression(ctx, then, out_expr);
                        assert(then_result == save_point + 1);

                        jump_instructions_idxs.push_back(out_expr->steps_.size());
                        out_expr->push_step(ExprOpCode::JUMP, then_result, 0);  
                        
                        // back patch jump_if_falsy.
                        out_expr->steps_[jump_if_falsy_idx].r2 = out_expr->steps_.size();
                    }

                } else {
                    // CASE WHEN exp THEN exp ... ELSE exp END.
                    // the result of every WHEN exp THEN pair must be stored right after this register.
                    save_point = out_expr->sp_;

                    for(auto& [when, then] : case_ex->when_then_pairs_) {
                        out_expr->reset_sp(save_point);
                        
                        i32 r1 = flatten_expression(ctx, when, out_expr);

                        u32 jump_if_falsy_idx = out_expr->steps_.size();

                        out_expr->push_step(ExprOpCode::JUMP_IF_FALSY, r1, 0); 

                        out_expr->reset_sp(save_point);
                        // all then results must be be equal to save_point + 1.
                        i32 then_result = flatten_expression(ctx, then, out_expr);
                        assert(then_result == save_point + 1);

                        jump_instructions_idxs.push_back(out_expr->steps_.size());
                        out_expr->push_step(ExprOpCode::JUMP, then_result, 0);  
                        
                        // back patch jump_if_falsy.
                        out_expr->steps_[jump_if_falsy_idx].r2 = out_expr->steps_.size();
                    }
                }
                out_expr->reset_sp(save_point);

                // optional else
                if(case_ex->else_) {
                    i32 res = flatten_expression(ctx, case_ex->else_, out_expr);
                    out_expr->reset_sp(save_point + 1);
                    assert(res == save_point + 1);
                } else {
                    // if no else provided return null.
                    i32 r1 = out_expr->allocate_register();
                    assert(r1 == save_point + 1);
                    out_expr->push_step(ExprOpCode::NULL_CONST, r1);
                }

                // back patch all jump instructions.
                for(int i = 0; i < jump_instructions_idxs.size(); ++i) {
                    out_expr->steps_[jump_instructions_idxs[i]].r2 = out_expr->steps_.size();
                }

                return save_point + 1;
            }
        case NULLIF_EXPRESSION  : 
            {
                NullifExpressionNode* nullif_ex = (NullifExpressionNode*)(in_expr);
                assert(nullif_ex->lhs_ && nullif_ex->rhs_); // if you are here they must not be null pointers.

                i32 r2 = reserved_functions_indexes[str_lit("NULLIF")];  // store the function index.
                assert(r2 != 0);
                i32 r3 = 2; // store the number of arguments to the function .

                i32 lhs_res = flatten_expression(ctx, nullif_ex->lhs_, out_expr); 
                out_expr->reset_sp(lhs_res);

                i32 rhs_res = flatten_expression(ctx, nullif_ex->rhs_, out_expr);
                out_expr->reset_sp(rhs_res);

                i32 r1 = out_expr->allocate_register(); // one register for the result.
                // arguments of the function are stored in r1 - 1  and r1 - 2.
                assert(r1-2 == lhs_res && r1-1 == rhs_res);
                out_expr->push_step(ExprOpCode::FUNC_CALL, r1, r2, r3);
                return r1;
            }
        case SCALAR_FUNC: 
            {
                ScalarFuncNode* sfn = (ScalarFuncNode*)(in_expr);
                if(!reserved_functions_indexes.count(sfn->name_)){
                    // TODO: this check should be in the query validation phase.
                    std::cout << "[ERROR] undefined function call" << "\n";
                    return 0;
                }
                i32 r2 = reserved_functions_indexes[sfn->name_];  // store the function index.
                i32 r3 = sfn->args_.size(); // store the number of arguments to the function.

                for(int i = 0; i < sfn->args_.size(); ++i){
                    i32 res = flatten_expression(ctx, sfn->args_[i], out_expr);
                    out_expr->reset_sp(res);
                }
                i32 r1 = out_expr->allocate_register(); // one register for the result.
                out_expr->push_step(ExprOpCode::FUNC_CALL, r1, r2, r3);
                return r1;
            }
        case AND : 
            {
                AndNode* land = (AndNode*)(in_expr);
                i32 r1 = out_expr->allocate_register();
                i32 r2 = flatten_expression(ctx, land->cur_, out_expr, land->cur_->category_ == AND);
                ASTNode* ptr = land->next_;
                if(only_one || land->mark_split_) return r2;

                while(ptr){
                    if(ptr->category_ == AND && ((AndNode*)ptr)->mark_split_) break;
                    i32 r3 = flatten_expression(ctx, ptr, out_expr, ptr->category_ == AND);
                    out_expr->push_step(ExprOpCode::AND, r1, r2, r3);

                    out_expr->reset_sp(r1);
                    if(ptr->category_ != AND) break;
                    r2 = r1;
                    r1 = out_expr->allocate_register();

                    ptr = ((AndNode*)(ptr))->next_;
                }
                return r1;
            }
        case OR  : 
            {
                OrNode* lor = (OrNode*)(in_expr);
                i32 r1 = out_expr->allocate_register();
                i32 r2 = flatten_expression(ctx, lor->cur_, out_expr, lor->cur_->category_ == OR);
                if(only_one) return r2;

                ASTNode* ptr = lor->next_;
                while(ptr){
                    i32 r3 = flatten_expression(ctx, ptr, out_expr, ptr->category_ == OR);
                    out_expr->push_step(ExprOpCode::OR, r1, r2, r3);

                    out_expr->reset_sp(r1);
                    if(ptr->category_ != OR) break;
                    r2 = r1;
                    r1 = out_expr->allocate_register();

                    ptr = ((OrNode*)(ptr))->next_;
                }
                return r1;
            }
        case BETWEEN  : 
            {
                bool answer = false;
                bool null_return = false;
                BetweenNode* between = (BetweenNode*)(in_expr);
                i32 r1 = out_expr->allocate_register();
                i32 r2 = out_expr->allocate_register();
                i32 r3 = out_expr->allocate_register();

                i32 val_res = flatten_expression(ctx, between->val_, out_expr, false);
                i32 lhs_res = flatten_expression(ctx, between->lhs_, out_expr, false);
                i32 rhs_res = flatten_expression(ctx, between->rhs_, out_expr, false);

                out_expr->push_step(ExprOpCode::GTE, r2, val_res, lhs_res);
                out_expr->push_step(ExprOpCode::LTE, r3, val_res, rhs_res);
                //out_expr->push_step(ExprOpCode::AND, r1, r2, r3);
                if(between->negated_){
                    i32 negated_reg = out_expr->allocate_register();
                    out_expr->push_step(ExprOpCode::AND, negated_reg, r2, r3);
                    out_expr->push_step(ExprOpCode::NOT, r1, negated_reg);
                } else {
                    out_expr->push_step(ExprOpCode::AND, r1, r2, r3);
                }

                out_expr->reset_sp(r1);
                return r1;
            }
        case IN  : 
            {
                InNode* in = reinterpret_cast<InNode*>(in_expr);
                // the value to be compared.
                i32 r2 = flatten_expression(ctx, in->val_, out_expr, false); 
                out_expr->in_expr_vals_.push(r2);
                i32 r3 = in->list_.size(); // the number of items to compare to.
                for(int i = 0; i < in->list_.size(); ++i){
                    i32 res = flatten_expression(ctx, in->list_[i], out_expr, false);
                    out_expr->reset_sp(res);
                }
                out_expr->in_expr_vals_.pop();

                i32 r1 = out_expr->allocate_register();
                out_expr->push_step(ExprOpCode::IN, r1, r2, r3);


                if(in->negated_) {
                    i32 not_res = out_expr->allocate_register();
                    out_expr->push_step(ExprOpCode::NOT, not_res, r1);
                    return not_res;
                }
                return r1;
            }
            break;
        default: assert(0 && "UNREACHABLE!");
    }
    return 0;
}

Value evaluate_flat_expression(QueryCTX* ctx, FlatExpr& expr, const Tuple& cur_tuple) {
    u32 pc = 0;
    u32 program_size = expr.steps_.size();
    if(!expr.registers_.size()) expr.registers_.resize(expr.max_sp_, Value(NULL_TYPE)); 
    assert(expr.constants_pool_.size() >= 1);


    u32 r1, r2, r3;


    while(pc < program_size){
        r1 = expr.steps_[pc].r1;
        r2 = expr.steps_[pc].r2;
        r3 = expr.steps_[pc].r3;

        switch(expr.steps_[pc].op_){
            case ExprOpCode::NOT: 
                {
                    Value* v1 = &expr.registers_[r1];
                    Value* v2 = &expr.registers_[r2];

                    if(v2->isNull()) *v1 = *v2;
                    else *v1 = Value(!((bool)v2->getBoolVal()));
                    break;
                }
            case ExprOpCode::IS:
                {
                    Value* v1 = &expr.registers_[r1];
                    Value* v2 = &expr.registers_[r2];
                    Value* v3 = &expr.registers_[r3];
                    
                    if(v3->isNull() || v2->isNull()) *v1 = Value((bool)(v2->isNull() && v3->isNull()));
                    else *v1 = Value((bool)(*v2 == *v3));
                    break;
                }
            case ExprOpCode::IS_NOT:
                {
                    Value* v1 = &expr.registers_[r1];
                    Value* v2 = &expr.registers_[r2];
                    Value* v3 = &expr.registers_[r3];

                    if(v2->isNull()) *v1 = Value((bool)(!v3->isNull()));
                    else if(v3->isNull()) *v1 = Value((bool)(!v2->isNull()));
                    else *v1 = Value((bool)(*v2 != *v3));
                    break;
                }
            case ExprOpCode::EQ: 
                {
                    Value* v1 = &expr.registers_[r1];
                    Value* v2 = &expr.registers_[r2];
                    Value* v3 = &expr.registers_[r3];

                    if(v2->isNull() || v3->isNull()) *v1 = Value(NULL_TYPE);
                    else *v1 = Value((bool)(*v2 == *v3));
                    break;
                }
            case ExprOpCode::NEQ: 
                {
                    Value* v1 = &expr.registers_[r1];
                    Value* v2 = &expr.registers_[r2];
                    Value* v3 = &expr.registers_[r3];

                    if(v2->isNull() || v3->isNull()) *v1 = Value(NULL_TYPE);
                    else *v1 = Value((bool)(*v2 != *v3));
                    break;
                }
            case ExprOpCode::LT:
                {
                    Value* v1 = &expr.registers_[r1];
                    Value* v2 = &expr.registers_[r2];
                    Value* v3 = &expr.registers_[r3];

                    if(v2->isNull() || v3->isNull()) *v1 = Value(NULL_TYPE);
                    else *v1 = Value((bool)(*v2 < *v3));
                    break;
                }
            case ExprOpCode::LTE:
                {
                    Value* v1 = &expr.registers_[r1];
                    Value* v2 = &expr.registers_[r2];
                    Value* v3 = &expr.registers_[r3];

                    if(v2->isNull() || v3->isNull()) *v1 = Value(NULL_TYPE);
                    else *v1 = Value((bool)(*v2 <= *v3));
                    break;
                }
            case ExprOpCode::GT:
                {
                    Value* v1 = &expr.registers_[r1];
                    Value* v2 = &expr.registers_[r2];
                    Value* v3 = &expr.registers_[r3];

                    if(v2->isNull() || v3->isNull()) *v1 = Value(NULL_TYPE);
                    else *v1 = Value((bool)(*v2 > *v3));
                    break;
                }
            case ExprOpCode::GTE:
                {
                    Value* v1 = &expr.registers_[r1];
                    Value* v2 = &expr.registers_[r2];
                    Value* v3 = &expr.registers_[r3];

                    if(v2->isNull() || v3->isNull()) *v1 = Value(NULL_TYPE);
                    else *v1 = Value((bool)(*v2 >= *v3));
                    break;
                }
            case ExprOpCode::SUB_QUERY:
                {
                    Value* v1 = &expr.registers_[r1];
                    ctx->query_inputs[expr.query_idx_] = cur_tuple; 
                    *v1 = evaluate_subquery_flat(ctx, r2);
                    break;
                }
            case ExprOpCode::SUB_QUERY_MATCH:
                {
                    Value* v1 = &expr.registers_[r1];
                    Value* v3 = &expr.registers_[r3];
                    ctx->query_inputs[expr.query_idx_] = cur_tuple; 
                    *v1 = match_with_subquery(ctx, r2, *v3);
                    break;
                }
            case ExprOpCode::ADD:
                {
                    Value* v1 = &expr.registers_[r1];
                    Value* v2 = &expr.registers_[r2];
                    Value* v3 = &expr.registers_[r3];

                    if(v2->isNull() || v3->isNull()) *v1 = Value(NULL_TYPE);
                    else {
                        *v2 += *v3;
                        *v1 = *v2;
                    }
                    break;
                }
            case ExprOpCode::SUBTRACT:
                {
                    Value* v1 = &expr.registers_[r1];
                    Value* v2 = &expr.registers_[r2];
                    Value* v3 = &expr.registers_[r3];

                    if(v2->isNull() || v3->isNull()) *v1 = Value(NULL_TYPE);
                    else *v1 = (*v2 - *v3);
                    break;
                }
            case ExprOpCode::MULTIPLY:
                {
                    Value* v1 = &expr.registers_[r1];
                    Value* v2 = &expr.registers_[r2];
                    Value* v3 = &expr.registers_[r3];

                    if(v2->isNull() || v3->isNull()) *v1 = Value(NULL_TYPE);
                    else {
                        *v2 *= *v3;
                        *v1 = *v2;
                    }
                    break;
                }
            case ExprOpCode::DEVIDE:
                {
                    Value* v1 = &expr.registers_[r1];
                    Value* v2 = &expr.registers_[r2];
                    Value* v3 = &expr.registers_[r3];

                    if(v2->isNull() || v3->isNull()) *v1 = Value(NULL_TYPE);
                    else {
                        *v2 /= *v3;
                        *v1 = *v2;
                    }
                    break;
                }
            case ExprOpCode::NEGATE:
                {
                    Value* v1 = &expr.registers_[r1];
                    Value* v2 = &expr.registers_[r2];

                    if(v2->isNull()) *v1 = Value(NULL_TYPE);
                    else {
                        *v1 = Value((int)0);
                        *v1 -= *v2;
                    }
                    break;
                }
            case ExprOpCode::TYPE_CAST:
                {
                    Type t = (Type)r3; 
                    Value* v1 = &expr.registers_[r1];
                    Value* v2 = &expr.registers_[r2];

                    if(v2->isNull()) {
                        *v1 = Value(NULL_TYPE);
                        break;
                    }
                    else if(v2->type_ == t) {
                        *v1 = *v2;
                        break;
                    }
                    if(v2->type_ == INT && t == FLOAT) {
                        float res = (float) v2->getIntVal(); 
                        *v1 = Value(res);
                        break;
                    } 
                    if(v2->type_ == FLOAT && t == INT) {
                        int res = (int)  round(v2->getFloatVal());
                        *v1 = Value(res);
                        break;
                    } 
                    // TODO: do more usefull type casting.
                    assert(0  && "TYPE CASTING NOT SUPPORTED YET!");
                    break;
                }

            case ExprOpCode::STRING_CONST:
            case ExprOpCode::FLOAT_CONST:
            case ExprOpCode::INT_CONST:
                {
                    Value* v1 = &expr.registers_[r1];
                    *v1 = expr.constants_pool_[r2];
                    break;
                }
            case ExprOpCode::NULL_CONST:
                {
                    Value* v1 = &expr.registers_[r1];
                    *v1 = expr.constants_pool_[0];
                    break;
                }
            case ExprOpCode::FIELD:
                {
                    Value* v1 = &expr.registers_[r1];
                    if(expr.query_idx_ == r3)
                        *v1 = cur_tuple.get_val_at(r2);
                    else
                        *v1 = ctx->query_inputs[r3].get_val_at(r2);
                    break;
                }
            case ExprOpCode::JUMP_IF_FALSY:
                {
                    Value* v1 = &expr.registers_[r1];
                    if(v1->getBoolVal() == false) {
                        pc = r2;
                        continue;
                    }
                    break;
                }
            case ExprOpCode::JUMP:
                {
                    pc = r2;
                    continue;
                }
            case ExprOpCode::FUNC_CALL:
                {
                    std::vector<Value> args;
                    args.reserve(r3);
                    for(i32 i = r1-r3; i < r1; ++i)
                        args.push_back(expr.registers_[i]);
                    expr.registers_[r1] = reserved_functions[r2](args);
                    break;
                }
            case ExprOpCode::AND:
                {
                    Value* v1 = &expr.registers_[r1];
                    Value* v2 = &expr.registers_[r2];
                    Value* v3 = &expr.registers_[r3];

                    if(v2->isNull() || v3->isNull()) *v1 = Value(NULL_TYPE);
                    else {
                        *v1 = Value((bool)(v2->getBoolVal() && v3->getBoolVal()));
                    }
                    break;
                }
            case ExprOpCode::OR:
                {
                    Value* v1 = &expr.registers_[r1];
                    Value* v2 = &expr.registers_[r2];
                    Value* v3 = &expr.registers_[r3];

                    if(v2->isNull() && v3->isNull()) *v1 = Value(NULL_TYPE);
                    else if(v2->isNull()) *v1 = v3->getBoolVal();
                    else if(v3->isNull()) *v1 = v2->getBoolVal();
                    else {
                        *v1 = Value((bool)((v2->getBoolVal() || v3->getBoolVal())));
                    }
                    break;
                }
            case ExprOpCode::IN:
                {
                    Value* v1 = &expr.registers_[r1];
                    Value* test_val = &expr.registers_[r2];
                    if(test_val->isNull()) {
                        *v1 = *test_val;
                        break;
                    }
                    *v1 = Value(false);

                    for(i32 i = r1-r3; i < r1; ++i){
                        if(expr.registers_[i].isNull()) {
                            *v1 = Value(NULL_TYPE);
                            continue;
                        }
                        if(expr.registers_[i] == *test_val) {
                            *v1 = Value(true);
                            break;
                        }
                    }
                    break;
                }
            default:
                assert(0 && "TODO!");
        }
        pc++;
    }
    assert(r1 < expr.registers_.size());
    return expr.registers_[r1];
}


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
                        if(land->mark_split_) break;
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
                          if(!reserved_functions_indexes.count(sfn->name_)){
                              // TODO: this check should be in the query validation phase.
                              std::cout << "[ERROR] undefined function call" << "\n";
                              return Value();
                          }
                          std::vector<Value> vals;
                          for(int i = 0; i < sfn->args_.size(); ++i){
                              vals.emplace_back(evaluate_expression(ctx, sfn->args_[i], cur_tuple, true, eval_sub_query));
                          }
                          return reserved_functions[reserved_functions_indexes[sfn->name_]](vals);
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
                          assert(field->schema_);
                          i32 idx = field->schema_->col_exist(field->token_.val_, field->table_name_->token_.val_);
                          assert(idx < cur_tuple.size());
                          assert(idx >= 0);
                          return cur_tuple.get_val_at(idx);
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
        cur->mark_split_ = true;
        //cur->next_ = nullptr;
    }
    return ret;
}


void accessed_tables(ASTNode* expression ,Vector<String8>& tables, Catalog* catalog, bool only_one) {
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
                        if(land->mark_split_) break;
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
                       if(field->table_name_ != nullptr){
                           tables.push_back(field->table_name_->token_.val_);
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

void accessed_fields(ASTNode* expression, Vector<FieldNode*>& fields, bool only_one) {
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
                        if(land->mark_split_) break;
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
                          FieldNode* f = (FieldNode*)(expression);
                          if(f->table_name_ && f->table_name_->token_.val_ == str_lit(AGG_FUNC_IDENTIFIER_PREFIX)) return;
                          fields.push_back(f);
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

void accessed_fields_deep(QueryCTX& ctx, ASTNode* expression, Vector<FieldNode*>& fields, bool only_one) {
    if(!expression) return;
    switch(expression->category_){
        case EXPRESSION  : 
            {
                ExpressionNode* ex = reinterpret_cast<ExpressionNode*>(expression);
                return accessed_fields_deep(ctx, ex->cur_, fields,  false);
            }
        case CASE_EXPRESSION  : 
            {
                CaseExpressionNode* case_ex = reinterpret_cast<CaseExpressionNode*>(expression);
                if(case_ex->initial_value_){
                    accessed_fields_deep(ctx, case_ex->initial_value_, fields);
                }
                for(auto& [when, then] : case_ex->when_then_pairs_){
                    accessed_fields_deep(ctx, when, fields);
                    accessed_fields_deep(ctx, then, fields);
                }
                if(case_ex->else_)  
                    accessed_fields_deep(ctx, case_ex->else_, fields);
                return;
            }
        case NULLIF_EXPRESSION  : 
            {
                NullifExpressionNode* nullif_ex = reinterpret_cast<NullifExpressionNode*>(expression);
                assert(nullif_ex->lhs_ && nullif_ex->rhs_); // if you are here they must not be null pointers.

                accessed_fields_deep(ctx, nullif_ex->lhs_, fields);
                accessed_fields_deep(ctx, nullif_ex->rhs_, fields);
                return;
            }
        case IN  : 
            {
                InNode* in = reinterpret_cast<InNode*>(expression);
                accessed_fields_deep(ctx, in->val_, fields, false);
                for(int i = 0; i < in->list_.size(); ++i){
                    accessed_fields_deep(ctx, in->list_[i], fields, false);
                }
                return;
            }
        case BETWEEN  : 
            {
                BetweenNode* between = reinterpret_cast<BetweenNode*>(expression);
                accessed_fields_deep(ctx, between->val_, fields,  false);
                accessed_fields_deep(ctx, between->lhs_, fields,  false);
                accessed_fields_deep(ctx, between->rhs_, fields,  false);
                return;
            }
        case NOT  : 
            {
                NotNode* lnot = reinterpret_cast<NotNode*>(expression);
                accessed_fields_deep(ctx, lnot->cur_, fields);
                return;
            }
        case OR  : 
            {
                OrNode* lor = reinterpret_cast<OrNode*>(expression);
                ASTNode* ptr = lor;
                while(ptr){
                    accessed_fields_deep(ctx, lor->cur_, fields);
                    ptr = lor->next_;
                    if(!ptr) break;
                    if(ptr->category_ == OR){
                        lor = reinterpret_cast<OrNode*>(ptr);
                    } else {
                        accessed_fields_deep(ctx, ptr, fields);
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
                    accessed_fields_deep(ctx, land->cur_, fields);
                    ptr = land->next_;
                    if(!ptr) break;
                    if(ptr->category_ == AND){
                        land = reinterpret_cast<AndNode*>(land->next_);
                    } else {
                        accessed_fields_deep(ctx, ptr, fields);
                        break;
                    }
                }
                return;
            } 
        case EQUALITY : 
            {
                EqualityNode* eq = reinterpret_cast<EqualityNode*>(expression);
                TokenType op = eq->token_.type_;
                accessed_fields_deep(ctx, eq->cur_, fields);
                ASTNode* ptr = eq->next_;
                while(ptr){
                    accessed_fields_deep(ctx, ptr, fields);
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
                accessed_fields_deep(ctx, comp->cur_, fields, comp->cur_->category_ == COMPARISON);
                ASTNode* ptr = comp->next_;
                while(ptr){
                    accessed_fields_deep(ctx, ptr, fields, comp->cur_->category_ == COMPARISON);

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
                accessed_fields_deep(ctx, t->cur_, fields, t->cur_->category_ == TERM);
                if(only_one) return;
                TokenType op = t->token_.type_;
                ASTNode* ptr = t->next_;
                while(ptr){
                    accessed_fields_deep(ctx, ptr, fields, ptr->category_ == TERM);
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
                          accessed_fields_deep(ctx, f->cur_, fields, f->cur_->category_ == FACTOR);
                          if(only_one) return;
                          TokenType op = f->token_.type_;
                          ASTNode* ptr = f->next_;
                          while(ptr){
                              accessed_fields_deep(ctx, ptr, fields, ptr->category_ == FACTOR);
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
                          accessed_fields_deep(ctx, u->cur_, fields);
                          return;
                      } 
        case SCALAR_FUNC: 
                      {
                          ScalarFuncNode* sfn = reinterpret_cast<ScalarFuncNode*>(expression);
                          for(int i = 0; i < sfn->args_.size(); ++i){
                              accessed_fields_deep(ctx, sfn->args_[i], fields);
                          }
                          return;
                      } 
        case TYPE_CAST: 
                      {
                          TypeCastNode* cast = reinterpret_cast<TypeCastNode*>(expression);
                          accessed_fields_deep(ctx, cast->exp_, fields);
                          return;
                      } 
        case FIELD_EXPR:
                    {
                       fields.push_back((FieldNode*)expression);
                       return;
                   }
        case AGG_FUNC:{
                          AggregateFuncNode* func = (AggregateFuncNode*)expression;
                          accessed_fields_deep(ctx, func->exp_, fields);
                          return;
                      }
        case SUB_QUERY: 
                      {
                          auto sub_query = (SubQueryNode*)(expression);
                          QueryData* data = ctx.queries_call_stack_[sub_query->idx_];
                          get_fields_of_query_deep(ctx, data, fields);
                      }
        case STRING_CONSTANT: 
        case FLOAT_CONSTANT: 
        case INTEGER_CONSTANT: 
        case NULL_CONSTANT: 
                      return;
        default:
                      assert(0);
                      return;
    }
}

void get_fields_of_query_deep(QueryCTX& ctx, QueryData* data, Vector<FieldNode*>& fields){
    if(!data) return;
    accessed_fields_deep(ctx, data->where_, fields);
    for(u32 i = 0 ; i < data->joined_tables_.size(); ++i)
        accessed_fields_deep(ctx, data->joined_tables_[i].condition_, fields);
    switch(data->type_){
        case SELECT_DATA:
            {
                auto select_data = reinterpret_cast<SelectStatementData*>(data);
                accessed_fields_deep(ctx, select_data->having_, fields);
                for(u32 i = 0; i < select_data->fields_.size(); ++i)
                    accessed_fields_deep(ctx, select_data->fields_[i], fields);
                for(u32 i = 0; i < select_data->aggregates_.size(); ++i)
                    accessed_fields_deep(ctx, select_data->aggregates_[i], fields);
                for(u32 i = 0; i < select_data->group_by_.size(); ++i)
                    accessed_fields_deep(ctx, select_data->group_by_[i], fields);
            } break;
        case INSERT_DATA:
            {
                auto insert_data = reinterpret_cast<InsertStatementData*>(data);
                for(u32 i = 0; i < insert_data->values_.size(); ++i)
                    accessed_fields_deep(ctx, insert_data->values_[i], fields);
            } break;
        case DELETE_DATA:
            {
                auto delete_data = reinterpret_cast<DeleteStatementData*>(data);
            } break;
        case UPDATE_DATA:
            {
                auto update_data = reinterpret_cast<UpdateStatementData*>(data);
                for(u32 i = 0; i < update_data->values_.size(); ++i)
                    accessed_fields_deep(ctx, update_data->values_[i], fields);
            } break;
        default:
            assert(0);
    }
}

void get_fields_of_query(QueryData* data, Vector<FieldNode*>& fields){
    if(!data) return;
    accessed_fields(data->where_, fields);
    for(u32 i = 0 ; i < data->joined_tables_.size(); ++i)
        accessed_fields(data->joined_tables_[i].condition_, fields);
    switch(data->type_){
        case SELECT_DATA:
            {
                auto select_data = reinterpret_cast<SelectStatementData*>(data);
                accessed_fields(select_data->having_, fields);
                for(u32 i = 0; i < select_data->fields_.size(); ++i)
                    accessed_fields(select_data->fields_[i], fields);
                for(u32 i = 0; i < select_data->aggregates_.size(); ++i)
                    accessed_fields(select_data->aggregates_[i], fields);
                for(u32 i = 0; i < select_data->group_by_.size(); ++i)
                    accessed_fields(select_data->group_by_[i], fields);
            } break;
        case INSERT_DATA:
            {
                auto insert_data = reinterpret_cast<InsertStatementData*>(data);
                for(u32 i = 0; i < insert_data->values_.size(); ++i)
                    accessed_fields(insert_data->values_[i], fields);
            } break;
        case DELETE_DATA:
            {
                auto delete_data = reinterpret_cast<DeleteStatementData*>(data);
            } break;
        case UPDATE_DATA:
            {
                auto update_data = reinterpret_cast<UpdateStatementData*>(data);
                for(u32 i = 0; i < update_data->values_.size(); ++i)
                    accessed_fields(update_data->values_[i], fields);
            } break;
        default:
            assert(0);
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

Value evaluate_subquery_flat(QueryCTX* ctx, i32 query_idx) {
    bool used_with_exists = false;
    Executor* sub_query_executor = ctx->executors_call_stack_[query_idx]; 
    sub_query_executor->init();
    if(sub_query_executor->error_status_){
        std::cout << "[ERROR] could not initialize sub-query" << std::endl;
        ctx->error_status_ = Error::QUERY_NOT_SUPPORTED; // TODO: put a better error_status_.
        return Value();
    }

    Tuple tmp = sub_query_executor->next();
    if(tmp.size() == 0 && sub_query_executor->finished_) {
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
    return tmp.get_val_at(0);
}


// matches a value with the output of a subquery, used for IN expressions for example: SELECT 5 IN (SELECT 5 FROM table1);
Value match_with_subquery(QueryCTX* ctx, i32 query_idx, Value val) {
    //if(val.getIntVal() == 0) asm("int3");
    if(val.isNull()) return Value(NULL_TYPE);

    Executor* sub_query_executor = ctx->executors_call_stack_[query_idx]; 
    sub_query_executor->init();
    if(sub_query_executor->error_status_){
        std::cout << "[ERROR] could not initialize sub-query" << std::endl;
        ctx->error_status_ = Error::QUERY_NOT_SUPPORTED; // TODO: put a better error_status_.
        return Value();
    }

    bool answer = false;
    bool null_ret = false;
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
    if(null_ret) return Value(NULL_TYPE);
    if(answer == true) return val;
    return Value(INVALID);
}

#endif // EXPRESSION_H
