#pragma once
#include "tokenizer.cpp"
#include "error.h"
#include "catalog.cpp"
#include "utils.h"
#include "query_ctx.cpp"
#include "query_data.cpp"
#include "index_key.h"
#include <cmath>

class Parser {
    public:
        Parser(Catalog* c): catalog_(c){}
        ~Parser(){}
        ASTNode* constant(QueryCTX& ctx);
        ASTNode* table(QueryCTX& ctx);
        ASTNode* field(QueryCTX& ctx);
        // scoped field is a field of the format: tableName + "." + fieldName
        ASTNode* scoped_field(QueryCTX& ctx);
        ASTNode* case_expression(QueryCTX& ctx, ExpressionNode* expression_ctx);
        ASTNode* nullif_expression(QueryCTX& ctx, ExpressionNode* expression_ctx);
        ASTNode* type_cast(QueryCTX& ctx, ExpressionNode* expression_ctx);
        ASTNode* scalar_func(QueryCTX& ctx, ExpressionNode* expression_ctx);
        ASTNode* agg_func(QueryCTX& ctx, ExpressionNode* expression_ctx);
        ASTNode* item(QueryCTX& ctx, ExpressionNode* expression_ctx);
        ASTNode* unary(QueryCTX& ctx, ExpressionNode* expression_ctx);
        ASTNode* factor(QueryCTX& ctx, ExpressionNode* expression_ctx);
        ASTNode* term(QueryCTX& ctx,ExpressionNode* expression_ctx = nullptr);
        ASTNode* comparison(QueryCTX& ctx, ExpressionNode* expression_ctx);
        ASTNode* equality(QueryCTX& ctx, ExpressionNode* expression_ctx);
        ASTNode* in(QueryCTX& ctx, ExpressionNode* expression_ctx);
        ASTNode* between(QueryCTX& ctx,ExpressionNode* expression_ctx);
        ASTNode* logic_not(QueryCTX& ctx,ExpressionNode* expression_ctx);
        ASTNode* logic_and(QueryCTX& ctx,ExpressionNode* expression_ctx);
        ASTNode* logic_or(QueryCTX& ctx, ExpressionNode* expression_ctx);
        ExpressionNode* expression(QueryCTX& ctx, int query_idx, int id);

        Value constVal(QueryCTX& ctx);
        void table_with_rename(QueryCTX& ctx, int query_idx);
        void inner_join(QueryCTX& ctx, int query_idx);
        void outer_join(QueryCTX& ctx, int query_idx);
        void tableList(QueryCTX& ctx, int query_idx);
        void expressionList(QueryCTX& ctx, int query_idx);
        void argumentList(QueryCTX& ctx, int query_idx, std::vector<ExpressionNode*>& args);
        void selectList(QueryCTX& ctx, int query_idx);
        void groupByList(QueryCTX& ctx, int query_idx);
        void orderByList(QueryCTX& ctx, int query_idx);
        void fieldDefList(QueryCTX& ctx, int query_idx);
        void fieldList(QueryCTX& ctx, int query_idx);

        // top level statement is called with parent_idx  as -1.
        QueryData* intersect(QueryCTX& ctx, int parent_idx);
        QueryData* union_or_except(QueryCTX& ctx, int parent_idx);

        void set_operation(QueryCTX& ctx, int parent_idx);
        void selectStatement(QueryCTX& ctx, int parent_idx);
        void insertStatement(QueryCTX& ctx, int parent_idx);
        void deleteStatement(QueryCTX& ctx, int parent_idx);
        void updateStatement(QueryCTX& ctx, int parent_idx);

        // DDL
        void createTableStatement(QueryCTX& ctx, int parent_idx);
        void createIndexStatement(QueryCTX& ctx, int parent_idx);

        // query : input.
        // ctx   : output.
        void parse(std::string& query, QueryCTX& ctx);

    private:
        Tokenizer tokenizer_ {};
        Catalog* catalog_;
};

Value Parser::constVal(QueryCTX& ctx){
    if((bool)ctx.error_status_) return Value();
    Token token = ctx.getCurrentToken();
    switch(token.type_){
        case TokenType::STR_CONSTANT:{
            char* str = (char*)ctx.arena_.alloc(token.val_.size());
            memcpy(str, token.val_.c_str(), token.val_.size());
            return Value(str, (uint16_t)token.val_.size());
                                     }
        case TokenType::FLOATING_CONSTANT:
        {
            errno = 0;
            float val = str_to_float(token.val_);
            if(!errno) return Value(val);
            errno = 0;
            double dval = str_to_float(token.val_);
            assert(errno == 0);
            return Value(dval);
        }
        case TokenType::NUMBER_CONSTANT:
        {
            long long val = str_to_ll(token.val_);
            if(val < LONG_MAX && val > LONG_MIN)
                return Value((int) val);
            return Value(val);
        }
        case TokenType::TRUE:
            return Value(true);
        case TokenType::FALSE:
            return Value(false);
        case TokenType::NULL_CONST:
            return Value(NULL_TYPE);
        default:
            return Value();
    }
}


void Parser::parse(std::string& query, QueryCTX& ctx){
    if((bool)ctx.error_status_) return; 
    tokenizer_.tokenize(query, ctx.tokens_);

    if(ctx.tokens_.size() == 0)
        return;

    switch(ctx.tokens_[0].type_){
        case TokenType::SELECT:
        case TokenType::LP:
            set_operation(ctx,-1);
            break;
        case TokenType::INSERT:
            insertStatement(ctx,-1);
            break;
        case TokenType::DELETE:
            deleteStatement(ctx, -1);
            break;
        case TokenType::UPDATE:
            updateStatement(ctx, -1);
            break;
        case TokenType::CREATE:
            {
              if(ctx.tokens_.size() >= 2 && ctx.tokens_[1].type_ == TokenType::TABLE)
                createTableStatement(ctx,-1);
              else
                createIndexStatement(ctx,-1);
            }
            break;
        default:
            ctx.error_status_ = Error::QUERY_NOT_SUPPORTED;
    }

}

void Parser::expressionList(QueryCTX& ctx, int query_idx){
    if((bool)ctx.error_status_) return; 
    int cnt = 1;
    auto query = reinterpret_cast<InsertStatementData*>(ctx.queries_call_stack_[query_idx]);
    while(1){
        ExpressionNode* f = expression(ctx, query_idx, cnt);
        if(!f){
            ctx.error_status_ = Error::EXPECTED_EXPRESSION;
            return;
        }
        query->values_.push_back(f);
        if(ctx.matchTokenType(TokenType::COMMA)){
            ++ctx;
            ++cnt;
            continue;
        }
        break;
    }
}

void Parser::argumentList(QueryCTX& ctx, int query_idx, std::vector<ExpressionNode*>& args){
    if((bool)ctx.error_status_) return; 
    int cnt = 1;
    while(1){
        ExpressionNode* f = expression(ctx, query_idx, cnt);
        if(!f){
            ctx.error_status_ = Error::EXPECTED_EXPRESSION;
            return;
        }
        args.push_back(f);
        if(ctx.matchTokenType(TokenType::COMMA)){
            ++ctx;
            ++cnt;
            continue;
        }
        break;
    }
}

void Parser::selectList(QueryCTX& ctx, int query_idx){
    if((bool)ctx.error_status_) return; 
    int cnt = 1;
    auto query = reinterpret_cast<SelectStatementData*>(ctx.queries_call_stack_[query_idx]);
    while(1){
        bool star = false;
        ExpressionNode* f = nullptr;
        if(ctx.matchTokenType(TokenType::STAR)){
            ++ctx;
            star = true;
        } else f = expression(ctx, query_idx, cnt);

        if(!star && !f)
            return;

        bool rename = false;
        // optional AS keyword.
        if(!star && ctx.matchTokenType(TokenType::AS)){
            ++ctx;
            rename = true;
            if(!ctx.matchTokenType(TokenType::IDENTIFIER)){
                //delete f;
                ctx.error_status_ = Error::EXPECTED_IDENTIFIER;
                return;
            } else {
                query->field_names_.push_back(ctx.getCurrentToken().val_);
                ++ctx;
            }
        }
        // optional renaming of a field.
        if(!star && !rename && ctx.matchTokenType(TokenType::IDENTIFIER)){
            rename = true;
            query->field_names_.push_back(ctx.getCurrentToken().val_);
            ++ctx;
        }

        query->has_star_ = (query->has_star_ || star);
        // if field_ptr->field_ == nullptr, that means it's a select * statement.
        query->fields_.push_back(f);
        if(!rename)
            query->field_names_.push_back("");

        if(f != nullptr && f->aggregate_func_ != nullptr){
            //query->aggregates_.push_back(f->aggregate_func_);
        }

        if(ctx.matchTokenType(TokenType::COMMA)){
            ++ctx;
            ++cnt;
            continue;
        }
        break;
    }
}

void Parser::table_with_rename(QueryCTX& ctx, int query_idx) {
    if((bool)ctx.error_status_) return; 
    QueryData* query = ctx.queries_call_stack_[query_idx];
    if(!ctx.matchTokenType(TokenType::IDENTIFIER)){
        return;
    }

    query->tables_.push_back(ctx.getCurrentToken().val_);
    ++ctx;

    // optional AS keyword.
    bool rename = false;
    if(ctx.matchTokenType(TokenType::AS)) {
        ++ctx;
        if(!ctx.matchTokenType(TokenType::IDENTIFIER)){
            return;
        }
        query->table_names_.push_back(ctx.getCurrentToken().val_);
        ++ctx;
        rename = true;
    }

    // optional renaming of a table.
    if(!rename && ctx.matchTokenType(TokenType::IDENTIFIER)){
        query->table_names_.push_back(ctx.getCurrentToken().val_);
        ++ctx;
        rename =  true;
    }

    if(!rename)
        query->table_names_.push_back(query->tables_[query->tables_.size()-1]);
}

void Parser::inner_join(QueryCTX& ctx, int query_idx) {
    if((bool)ctx.error_status_) return; 
    auto query = ctx.queries_call_stack_[query_idx];
    if(ctx.matchTokenType(TokenType::INNER)) ++ctx; 
    // 'JOIN ... ON ...' syntax.
    // TODO: refactor to a different function.
    ++ctx; // skip join
    table_with_rename(ctx, query_idx);

    if(!ctx.matchTokenType(TokenType::ON)){
        // explicit JOIN keyword must be paired with the ON keyword.
        // TODO: set up an appropriate error.
        return;
    }
    ++ctx; // skip on
    JoinedTablesData t = {};
    t.lhs_idx_ = (query->table_names_.size() - 2);
    t.rhs_idx_ = (query->table_names_.size() - 1);
    t.condition_ = expression(ctx, query->idx_, 0);
    if(!t.condition_) {
        // TODO: replace with error message.
        return;
    }
    // TODO: put a check on the to make sure they are only scoped to the specified tables.
    query->joined_tables_.push_back(t);
}

void Parser::outer_join(QueryCTX& ctx, int query_idx) {
    if((bool)ctx.error_status_) return; 
    auto query = ctx.queries_call_stack_[query_idx];
    JoinedTablesData t = {};
    t.type_ = token_type_to_join_type(ctx.getCurrentToken().type_);
    ++ctx; // skip join type: full, left, right.
    if(ctx.matchTokenType(TokenType::OUTER)) ++ctx; // skip optional 'outer'.

    if(!ctx.matchTokenType(TokenType::JOIN)) return; // TODO: replace with error.
    ++ctx; // skip 'join'
    table_with_rename(ctx, query_idx);

    if(!ctx.matchTokenType(TokenType::ON)){
        // explicit JOIN keyword must be paired with the ON keyword.
        // TODO: set up an appropriat error.
        return;
    }
    ++ctx; // skip on
    t.lhs_idx_ = (query->table_names_.size() - 2);
    t.rhs_idx_ = (query->table_names_.size() - 1);
    t.condition_ = expression(ctx, query->idx_, 0);
    if(!t.condition_) {
        // TODO: replace with error message.
        return;
    }
    // TODO: put a check on the to make sure they are only scoped to the specified tables.
    query->joined_tables_.push_back(t);
}

void Parser::tableList(QueryCTX& ctx, int query_idx){
    if((bool)ctx.error_status_) return; 
    auto query = ctx.queries_call_stack_[query_idx];
    while(1){
        if(ctx.matchTokenType(TokenType::LP)){
            ++ctx;
            tableList(ctx, query_idx);
            if(!ctx.matchTokenType(TokenType::RP)) {
                // TODO: set up an error message.
                return;
            }
            ++ctx;
        }
        table_with_rename(ctx, query_idx);
        // TODO: refactor repeated code.
        if(ctx.matchMultiTokenType({TokenType::CROSS, TokenType::JOIN})) {
            ctx += 2;
            continue;
        }  else if(ctx.matchMultiTokenType({ TokenType::INNER ,TokenType::JOIN, TokenType::IDENTIFIER }) ||
                ctx.matchMultiTokenType({ TokenType::JOIN, TokenType::IDENTIFIER })) { 
            inner_join(ctx, query_idx);
        } else if(ctx.matchAnyTokenType({ TokenType::LEFT, TokenType::RIGHT, TokenType::FULL })) {
            outer_join(ctx, query_idx);
        }

        if(ctx.matchTokenType(TokenType::COMMA)) {
          ++ctx;
          continue;
        } 

        break;
    }
}


void Parser::orderByList(QueryCTX& ctx, int query_idx){
    if((bool)ctx.error_status_) return; 
    auto query = reinterpret_cast<SelectStatementData*>(ctx.queries_call_stack_[query_idx]);
    while(1){
        Value val = constVal(ctx);
        if(val.isInvalid())
            return;
        ++ctx;
        if(val.type_ == Type::INT) 
            query->order_by_list_.push_back(val.getIntVal() - 1); 

        if(!ctx.matchTokenType(TokenType::COMMA)) 
            break;
        ++ctx;
    }
}

void Parser::fieldDefList(QueryCTX& ctx, int query_idx){
    if((bool)ctx.error_status_) return;
    auto query = reinterpret_cast<CreateTableStatementData*>(ctx.queries_call_stack_[query_idx]);
    while(1){
        FieldDef field_def = {}; 
        if(!ctx.matchTokenType(TokenType::IDENTIFIER)) {
            ctx.error_status_ = Error::EXPECTED_IDENTIFIER;
            return;
        }
        Token token = ctx.getCurrentToken(); ++ctx;
        field_def.field_name_ = token.val_;
        if(!tokenizer_.isDataType(ctx.getCurrentToken().type_)) {
            ctx.error_status_ = Error::EXPECTED_DATA_TYPE;
            return;
        }
        token = ctx.getCurrentToken(); ++ctx;
        field_def.type_ = token.type_;
        // TODO: text and varchar should be different?.
        if(field_def.type_ == TokenType::TEXT)  field_def.type_ = TokenType::VARCHAR;
        // TODO: fix that.
        // temporary handle different varchar sizes as an unknown size.
        if(field_def.type_ == TokenType::VARCHAR && ctx.matchTokenType(TokenType::LP)){
          ctx += 3; // three tokens: "("" + n + ")" 
          // TODO: use the max char size passed in by the user.
          // TODO: support other different  character types:
          // https://www.postgresql.org/docs/current/datatype-character.html
        }
        // constraints.
        while(ctx.matchAnyTokenType({ TokenType::PRIMARY, TokenType::NOT, TokenType::UNIQUE })){
            if(ctx.matchMultiTokenType({ TokenType::PRIMARY, TokenType::KEY })){
                field_def.constraints_.push_back(PRIMARY_KEY);
                ctx += 2;
                continue;
            } else if(ctx.matchMultiTokenType({ TokenType::NOT, TokenType::NULL_CONST })){
                field_def.constraints_.push_back(NOT_NULL);
                ctx += 2;
                continue;
            } else if(ctx.matchTokenType(TokenType::UNIQUE)){
                field_def.constraints_.push_back(UNIQUE);
                ++ctx;
                continue;
            }
            // TODO: add foreign key constraints.
            // reaching this points is an error.
            ctx.error_status_ = Error::EXPECTED_FIELD_CONSTRAINTS;
            return;
        }


        query->field_defs_.push_back(field_def);

        if(!ctx.matchTokenType(TokenType::COMMA)) 
            break;
        ++ctx;
    }

}

void Parser::fieldList(QueryCTX& ctx, int query_idx){
    if((bool)ctx.error_status_) return;
    auto query = reinterpret_cast<InsertStatementData*>(ctx.queries_call_stack_[query_idx]);
    while(1){
        if(!ctx.matchTokenType(TokenType::IDENTIFIER)) {
            ctx.error_status_ = Error::EXPECTED_IDENTIFIER;
            return;
        }
        Token token = ctx.getCurrentToken(); ++ctx;
        query->fields_.push_back(token.val_);
        if(!ctx.matchTokenType(TokenType::COMMA)) 
            break;
        ++ctx;
    }

}

void Parser::groupByList(QueryCTX& ctx, int query_idx) {
    if((bool)ctx.error_status_) return; 
    auto query = reinterpret_cast<SelectStatementData*>(ctx.queries_call_stack_[query_idx]);
    while(1){
        ASTNode* f = scoped_field(ctx);
        if(!f) f = field(ctx);
        if(!f) break;
        query->group_by_.push_back(f);

        if(!ctx.matchTokenType(TokenType::COMMA)) 
            break;
        ++ctx;
    }
}

ASTNode* Parser::constant(QueryCTX& ctx){
    if((bool)ctx.error_status_) return nullptr;
    ASTNode* ret = nullptr;

    // TODO: fix the inconsistancy of mapping between tokens and ASTNodes: 
    // ( change this ) number->integer, float->floating, str->string.
    //  
    // floats
    if(ctx.matchMultiTokenType({TokenType::NUMBER_CONSTANT, TokenType::DOT, TokenType::NUMBER_CONSTANT})) { 
        std::string val = ctx.getCurrentToken().val_; ++ctx;
        val += "."; ++ctx;
        val += ctx.getCurrentToken().val_; ++ctx;
        auto t = Token(TokenType::FLOATING_CONSTANT, val);
        ALLOCATE_INIT(ctx.arena_, ret, ASTNode, FLOAT_CONSTANT, t);
      // strings
    } else  if(ctx.matchTokenType(TokenType::STR_CONSTANT)){ 
        //ret =  new ASTNode(STRING_CONSTANT, ctx.getCurrentToken()); ++ctx;  
        ALLOCATE_INIT(ctx.arena_, ret, ASTNode, STRING_CONSTANT, ctx.getCurrentToken()); ++ctx;
      // integers
    } else if(ctx.matchTokenType(TokenType::NUMBER_CONSTANT)){ 
        //ret = new ASTNode(INTEGER_CONSTANT, ctx.getCurrentToken()); ++ctx;
        ALLOCATE_INIT(ctx.arena_, ret, ASTNode, INTEGER_CONSTANT, ctx.getCurrentToken()); ++ctx;
      // null
    } else if(ctx.matchTokenType(TokenType::NULL_CONST)){ 
        //ret = new ASTNode(NULL_CONSTANT, ctx.getCurrentToken()); ++ctx;
        ALLOCATE_INIT(ctx.arena_, ret, ASTNode, NULL_CONSTANT, ctx.getCurrentToken()); ++ctx;
    }
    return ret;
}

ASTNode* Parser::table(QueryCTX& ctx){
    if((bool)ctx.error_status_) return nullptr;
    if(ctx.matchTokenType(TokenType::IDENTIFIER)){
        auto token = ctx.getCurrentToken(); ++ctx;
        ASTNode* ret = nullptr;
        //return new ASTNode(TABLE, token);
        ALLOCATE_INIT(ctx.arena_, ret, ASTNode, TABLE, token);
        return ret;
    }
    return nullptr;
}

ASTNode* Parser::field(QueryCTX& ctx){
    if((bool)ctx.error_status_) return nullptr;
    ASTNode* ret = nullptr;
    if(ctx.matchTokenType(TokenType::IDENTIFIER)) {
        auto token = ctx.getCurrentToken(); ++ctx;
        std::vector<std::string> possible_tables = catalog_->getTablesByField(token.val_);
        if(possible_tables.size() == 1) {
            ASTNode* t = nullptr;
            ALLOCATE_INIT(ctx.arena_, t, ASTNode, TABLE, Token(TokenType::IDENTIFIER, possible_tables[0]));
            ALLOCATE_INIT(ctx.arena_, ret, ScopedFieldNode, token, t);
            return ret;
            //return new ScopedFieldNode(token, new ASTNode(TABLE, Token(TokenType::IDENTIFIER, possible_tables[0])));
        }
        ALLOCATE_INIT(ctx.arena_, ret, ASTNode, FIELD, token);
        //return new ASTNode(FIELD, token);
        return ret;
    }
    return ret;
}

ASTNode* Parser::scoped_field(QueryCTX& ctx){
    if((bool)ctx.error_status_) return nullptr;
    if(ctx.matchMultiTokenType({TokenType::IDENTIFIER, TokenType::DOT, TokenType::IDENTIFIER})) {
        ASTNode* t = table(ctx);
        if(!t) return nullptr;
        ++ctx;
        auto field_name = ctx.getCurrentToken();++ctx;
        ASTNode* ret = nullptr;
        ALLOCATE_INIT(ctx.arena_, ret, ScopedFieldNode, field_name, t);
        return ret;
        //return new ScopedFieldNode(field_name, t);
    }
    return nullptr;
}

ASTNode* Parser::case_expression(QueryCTX& ctx, ExpressionNode* expression_ctx){
    if((bool)ctx.error_status_) return nullptr;
    if(!expression_ctx) {
        ctx.error_status_ = Error::CANT_HAVE_CASE_EXPRESSION; 
        // TODO: use logger.
        std::cout << "[ERROR] Cannot have case expression in this context" << std::endl;
        return nullptr;
    }
    if(!ctx.matchTokenType(TokenType::CASE)) 
        return nullptr;
    // only eat the "CASE" token.
    ++ctx;
    std::vector<std::pair<ExpressionNode*, ExpressionNode*>> when_then_pairs;
    ExpressionNode* else_exp = nullptr;
    int id = expression_ctx->id_;
    int query_idx = expression_ctx->query_idx_;
    // this value converts the case into something similar to a switch statement, read the following for contextx:
    // https://www.postgresql.org/docs/current/functions-conditional.html#FUNCTIONS-CASE
    // optional initial value if it does not exists it should be null by default from the expression function.
    ExpressionNode* initial_value = expression(ctx, query_idx, id);
    // WHEN + expression + THEN + expression.
    while(true){
        if(!ctx.matchTokenType(TokenType::WHEN)) 
            break;
        ++ctx;
        auto when = expression(ctx, query_idx, id);
        if(!when) {
            //delete initial_value;
            return nullptr;
        }
        if(!ctx.matchTokenType(TokenType::THEN)) {
            //delete initial_value;
            //delete when;
            return nullptr;
        }
        ++ctx;
        auto then = expression(ctx, query_idx, id);
        if(!then) {
            //delete initial_value;
            //delete when;
            return nullptr;
        }

        when_then_pairs.push_back({when, then});
    }
    // optional ELSE
    if(ctx.matchTokenType(TokenType::ELSE)) {
        ++ctx;
        else_exp = expression(ctx, query_idx, id);
        if(!else_exp) {
            //delete initial_value;
            return nullptr;
        }
    }
    // must have END
    if(!ctx.matchTokenType(TokenType::END)) {
        //delete initial_value;
        //delete else_exp;
        return nullptr;
    }
    ++ctx;
    if(when_then_pairs.size() == 0){
        ctx.error_status_ = Error::INCORRECT_CASE_EXPRESSION; 
        // TODO: use logger.
        std::cout << "[ERROR] incorrect case expression" << std::endl;
        //delete else_exp;
        //delete initial_value;
        return nullptr;
    }

    ASTNode* ret = nullptr;
    ALLOCATE_INIT(ctx.arena_, ret, CaseExpressionNode, when_then_pairs, else_exp, initial_value);
    return ret;
    //return new CaseExpressionNode(when_then_pairs, else_exp, initial_value);
}

ASTNode* Parser::nullif_expression(QueryCTX& ctx, ExpressionNode* expression_ctx){
    if((bool)ctx.error_status_) return nullptr;
    if(!expression_ctx) {
        // TODO: use logger.
        std::cout << "[ERROR] Cannot have nullif expression in this context" << std::endl;
        return nullptr;
    }
    if(!ctx.matchMultiTokenType({TokenType::NULLIF, TokenType::LP})) 
        return nullptr;
    ctx+= 2; // eat 'NULLIF', '('
    ExpressionNode* lhs = nullptr;
    ExpressionNode* rhs = nullptr;
    int id = expression_ctx->id_;
    int query_idx = expression_ctx->query_idx_;
    lhs = expression(ctx, query_idx, id);
    if(!ctx.matchTokenType(TokenType::COMMA) || !lhs) 
        return nullptr;
    ++ctx; // eat ','
    rhs = expression(ctx, query_idx, id);
    if(!ctx.matchTokenType(TokenType::RP) || !rhs) {
        return nullptr;
    }
    ++ctx; // eat ')'

    ASTNode* ret = nullptr;
    ALLOCATE_INIT(ctx.arena_, ret, NullifExpressionNode, lhs, rhs);
    return ret;
    //return new NullifExpressionNode(lhs, rhs);
}

ASTNode* Parser::type_cast(QueryCTX& ctx, ExpressionNode* expression_ctx){
    if((bool)ctx.error_status_) return nullptr;
    if(!expression_ctx) {
        ctx.error_status_ = Error::CANT_CAST_TYPE; 
        // TODO: use logger.
        std::cout << "[ERROR] Cannot have type cast calls in this context" << std::endl;
        return nullptr;
    }
    if(!ctx.matchMultiTokenType({TokenType::CAST, TokenType::LP})) 
        return nullptr;
    ctx+=2;
    ExpressionNode* exp = expression(ctx, expression_ctx->query_idx_, expression_ctx->id_);
    if(!exp) return nullptr;
    if(!ctx.matchTokenType(TokenType::AS)) return nullptr;
    ++ctx;
    if(!tokenizer_.isDataType(ctx.getCurrentToken().type_)) {
        std::cout << "[ERROR] Invalid type" << std::endl;
        return nullptr;
    }
    Type t = tokenTypeToDBType(ctx.getCurrentToken().type_);
    ++ctx;
    if(!ctx.matchTokenType(TokenType::RP)) return nullptr;
    ++ctx;
    ASTNode* ret = nullptr;
    ALLOCATE_INIT(ctx.arena_, ret, TypeCastNode, exp, t);
    return ret;
    //return new TypeCastNode(exp, t);
}

ASTNode* Parser::scalar_func(QueryCTX& ctx, ExpressionNode* expression_ctx){
    if((bool)ctx.error_status_) return nullptr;
    if(!expression_ctx) {
        ctx.error_status_ = Error::CANT_CALL_FUNCTION; 
        // TODO: use logger.
        std::cout << "[ERROR] Cannot have scalar function calls in this context" << std::endl;
        return nullptr;
    }
    if(!ctx.matchMultiTokenType({TokenType::IDENTIFIER, TokenType::LP})) 
        return nullptr;
    std::string name = ctx.getCurrentToken().val_; 
    ctx+=2;
    std::vector<ExpressionNode*> args = {};
    argumentList(ctx, expression_ctx->query_idx_, args);
    if(args.size() == 0) return nullptr;
    if(!ctx.matchTokenType(TokenType::RP)) return nullptr;
    ++ctx;
    ASTNode* ret = nullptr;
    ALLOCATE_INIT(ctx.arena_, ret, ScalarFuncNode, args, name, expression_ctx->id_);
    return ret;
    //return new ScalarFuncNode(args, name, expression_ctx->id_);
}

ASTNode* Parser::agg_func(QueryCTX& ctx, ExpressionNode* expression_ctx){
    if((bool)ctx.error_status_) return nullptr;
    if(!expression_ctx) {
        ctx.error_status_ = Error::CANT_HAVE_AGGREGATION; 
        // TODO: use logger.
        std::cout << "[ERROR] Cannot have aggregate functions in this context" << std::endl;
        return nullptr;
    }
    if(!ctx.matchAnyTokenType({TokenType::MIN, TokenType::MAX, TokenType::AVG, TokenType::COUNT, TokenType::SUM})) 
        return nullptr;
    AggregateFuncType type = getAggFuncType(ctx.getCurrentToken().type_);
    if(type == NOT_DEFINED) return nullptr;
    ++ctx;
    if(!ctx.matchTokenType(TokenType::LP)) 
        return nullptr;
    ++ctx;
    ExpressionNode* exp = nullptr;
    bool distinct = false;
    // TODO: Provide real support instead skipping.
    if(ctx.matchTokenType(TokenType::ALL)) ++ctx; // e.g: count (ALL b) 
    else if(ctx.matchTokenType(TokenType::DISTINCT)){
        distinct = true;
        ++ctx; // e.g: count (DISTINCT a) 
    }
    // only count can have a star parameter.
    if(type == COUNT && ctx.matchTokenType(TokenType::STAR)){
        ++ctx;
    } else {
        exp = expression(ctx, expression_ctx->query_idx_, 0);
        if(!exp) return nullptr;
        if(exp->aggregate_func_) {
            ctx.error_status_ = Error::CANT_NEST_AGGREGATION; 
            std::cout << "[ERROR] Cannot nest aggregate functions" << std::endl; // TODO: use logger.
            return nullptr;
        }
    }
    //expression_ctx->aggregate_func_ =  new AggregateFuncNode(exp, type, expression_ctx->id_);
    expression_ctx->aggregate_func_ =  nullptr; 
    ALLOCATE_INIT(ctx.arena_, expression_ctx->aggregate_func_, AggregateFuncNode, exp, type, expression_ctx->id_);
    expression_ctx->aggregate_func_->distinct_ = distinct;
    auto query = (SelectStatementData*)ctx.queries_call_stack_[expression_ctx->query_idx_];
    query->aggregates_.push_back(expression_ctx->aggregate_func_);

    if(!ctx.matchTokenType(TokenType::RP)) return nullptr;
    ++ctx;
    std::string tmp = AGG_FUNC_IDENTIFIER_PREFIX;
    //tmp += intToStr(expression_ctx->id_);
    tmp += intToStr(query->aggregates_.size());
    ASTNode* ret = nullptr;
    ALLOCATE_INIT(ctx.arena_, ret, ASTNode, FIELD, Token(TokenType::IDENTIFIER, tmp));
    return ret;
    //return new ASTNode(FIELD, Token (TokenType::IDENTIFIER, tmp));
}

ASTNode* Parser::item(QueryCTX& ctx, ExpressionNode* expression_ctx){
    if((bool)ctx.error_status_) return nullptr;
    if(!expression_ctx) {
        ctx.error_status_ = Error::NO_EXPRESSION_CONTEXT;
        std::cout << "[ERROR] No expression context provided" << std::endl; // TODO: move error logging to it's module.
        return nullptr;
    }

    ASTNode* i = nullptr;
    // check for sub-queries.
    // EXISTS(sub-query), (sub-query) 
    if( ctx.matchMultiTokenType({TokenType::EXISTS, TokenType::LP, TokenType::SELECT }) ||
            ctx.matchMultiTokenType({TokenType::LP, TokenType::SELECT})){
        bool exists = (ctx.getCurrentToken().type_ == TokenType::EXISTS);
        if(exists) ++ctx;
        ctx += 1;
        int sub_query_id = ctx.queries_call_stack_.size();
        selectStatement(ctx, expression_ctx->query_idx_);
        SelectStatementData* sub_query = reinterpret_cast<SelectStatementData*>(ctx.queries_call_stack_[sub_query_id]);
        if(!sub_query) {
            return nullptr;
        }
        if(!sub_query->is_corelated_)
            sub_query->is_corelated_ = is_corelated_subquery(ctx, sub_query, catalog_);
        std::cout << "is corelated: " << sub_query->is_corelated_ << "\n";
        if(sub_query->is_corelated_){
            auto parent = ctx.queries_call_stack_[sub_query->parent_idx_];
            if(parent->parent_idx_ != -1)
                parent->is_corelated_ = true;
        }
        //SubQueryNode* sub_query_node = new SubQueryNode(sub_query->idx_, sub_query->parent_idx_);
        SubQueryNode* sub_query_node = nullptr; 
        ALLOCATE_INIT(ctx.arena_, sub_query_node, SubQueryNode, sub_query->idx_, sub_query->parent_idx_);
        if(!ctx.matchTokenType(TokenType::RP)){
            return nullptr;
        }
        ++ctx;
        sub_query_node->used_with_exists_ = exists;
        return sub_query_node;
    }
    // sub-query, without () might accur with the 'IN' operator : IN (sub-query).
    if(ctx.matchTokenType(TokenType::SELECT)){
        int sub_query_id = ctx.queries_call_stack_.size();
        selectStatement(ctx, expression_ctx->query_idx_);
        SelectStatementData* sub_query = reinterpret_cast<SelectStatementData*>(ctx.queries_call_stack_[sub_query_id]);
        if(!sub_query) {
            return nullptr;
        }
        if(!sub_query->is_corelated_)
            sub_query->is_corelated_ = is_corelated_subquery(ctx, sub_query, catalog_);
        sub_query->is_corelated_ = is_corelated_subquery(ctx, sub_query, catalog_);
        std::cout << "(IN)is corelated: " << sub_query->is_corelated_ << "\n";
        if(sub_query->is_corelated_){
            auto parent = ctx.queries_call_stack_[sub_query->parent_idx_];
            if(parent->parent_idx_ != -1)
                parent->is_corelated_ = true;
        }
        //SubQueryNode* sub_query_node = new SubQueryNode(sub_query->idx_, sub_query->parent_idx_);
        SubQueryNode* sub_query_node = nullptr; 
        ALLOCATE_INIT(ctx.arena_, sub_query_node, SubQueryNode, sub_query->idx_, sub_query->parent_idx_);
        return sub_query_node;
    }
    // nested expressions.
    if(ctx.matchTokenType(TokenType::LP)){
        ++ctx;
        int id = expression_ctx->id_;
        auto ex = expression(ctx, expression_ctx->query_idx_ ,id+1);
        if(!ex) return nullptr;
        if(!ctx.matchTokenType(TokenType::RP)){
            return nullptr;
        }
        ++ctx;
        return ex;
    }     
    if(expression_ctx)
        i = case_expression(ctx, expression_ctx);
    if(!i && expression_ctx)
        i = nullif_expression(ctx, expression_ctx);
    if(!i && expression_ctx)
        i = type_cast(ctx , expression_ctx);
    if(!i && expression_ctx && expression_ctx->id_ != 0)
        i = agg_func(ctx , expression_ctx);
    if(!i && expression_ctx)
        i = scalar_func(ctx , expression_ctx);
    if(!i)
        i = scoped_field(ctx);
    if(!i)
        i = field(ctx);
    if(!i)
        i = constant(ctx);
    return i;
}

ASTNode* Parser::unary(QueryCTX& ctx, ExpressionNode* expression_ctx){
    if((bool)ctx.error_status_) return nullptr;
    if(ctx.matchAnyTokenType({TokenType::PLUS, TokenType::MINUS})) { 
        UnaryNode* u = nullptr;
        //u = new UnaryNode(nullptr, ctx.getCurrentToken());
        ALLOCATE_INIT(ctx.arena_, u, UnaryNode, nullptr, ctx.getCurrentToken());
        ++ctx;
        ASTNode* val = unary(ctx , expression_ctx);
        if(!val) return nullptr;
        u->cur_ = val;
        return u;
    }
    // no need to wrap it in a unary if we don't find any unary operators.
    return item(ctx, expression_ctx);
}

ASTNode* Parser::factor(QueryCTX& ctx, ExpressionNode* expression_ctx){
    if((bool)ctx.error_status_) return nullptr;
    ASTNode* cur = unary(ctx, expression_ctx);
    if(!cur) return nullptr;
    if(ctx.matchAnyTokenType({TokenType::STAR, TokenType::SLASH})) { 
        FactorNode* f = nullptr;
        ALLOCATE_INIT(ctx.arena_, f, FactorNode, (UnaryNode*)cur);
        ASTNode* next = nullptr;
        f->token_ = ctx.getCurrentToken(); ++ctx; 
        next = factor(ctx, expression_ctx);
        if(!next) {
            return nullptr;
        }
        f->next_ = next;
        return f;
    }
    return cur;
}

ASTNode* Parser::term(QueryCTX& ctx,ExpressionNode* expression_ctx){
    if((bool)ctx.error_status_) return nullptr;
    ASTNode* cur = factor(ctx, expression_ctx);
    if(!cur) return nullptr;
    if(ctx.matchAnyTokenType({TokenType::PLUS, TokenType::MINUS})) { 
        TermNode* t = nullptr;
        ALLOCATE_INIT(ctx.arena_, t, TermNode, (FactorNode*)cur);
        t->token_ = ctx.getCurrentToken(); ++ctx; 
        ASTNode* next = term(ctx, expression_ctx);
        if(!next) {
            return nullptr;
        }
        t->next_ = next;
        return t;
    }
    return cur;
}

ASTNode* Parser::comparison(QueryCTX& ctx, ExpressionNode* expression_ctx){
    if((bool)ctx.error_status_) return nullptr;
    ASTNode* cur = term(ctx, expression_ctx);
    if(!cur) return nullptr;
    if(ctx.matchAnyTokenType({TokenType::LT, TokenType::LTE, TokenType::GT, TokenType::GTE })) { 
        ComparisonNode* c = nullptr; 
        ALLOCATE_INIT(ctx.arena_, c, ComparisonNode, (TermNode*)cur);
        c->token_ = ctx.getCurrentToken(); ++ctx; 
        ASTNode* next = comparison(ctx, expression_ctx);
        if(!next) {
            return nullptr;
        }
        c->next_ = next;
        return c;
    }
    return cur;
}

ASTNode* Parser::equality(QueryCTX& ctx, ExpressionNode* expression_ctx){
    if((bool)ctx.error_status_) return nullptr;
    ASTNode* cur = comparison(ctx, expression_ctx);
    if(!cur) return nullptr;
    if(ctx.matchAnyTokenType({TokenType::EQ, TokenType::NEQ, TokenType::IS})) { 
        EqualityNode* eq = nullptr; 
        ALLOCATE_INIT(ctx.arena_, eq, EqualityNode, (ComparisonNode*)cur);
        eq->token_ = ctx.getCurrentToken(); ++ctx; 
        if(ctx.getCurrentToken().type_ == TokenType::NOT){
          eq->token_ = Token(TokenType::ISNOT); ++ctx;
        }
        eq->next_ = equality(ctx, expression_ctx);
        if(!eq->next_) 
            return nullptr;
        return eq;
    }
    return cur;
}

ASTNode* Parser::in(QueryCTX& ctx, ExpressionNode* expression_ctx){
    if((bool)ctx.error_status_) return nullptr;
    ASTNode* ret = nullptr;
    ASTNode* val = equality(ctx, expression_ctx);
    if(!val) return nullptr;
    if(ctx.matchTokenType(TokenType::IN) || ctx.matchMultiTokenType({TokenType::NOT, TokenType::IN})) {
        bool negated = (ctx.getCurrentToken().type_ == TokenType::NOT);
        if(negated) ++ctx;
        ++ctx;
        if(!ctx.matchTokenType(TokenType::LP)) 
            return nullptr;
        ++ctx;
        if(ctx.matchTokenType(TokenType::RP)){  // empty list: select 1 in ();
            ++ctx;
            ALLOCATE_INIT(ctx.arena_, ret, InNode, val, {}, negated);
            return ret;
            //return new InNode(val, {}, negated);
        }
        std::vector<ASTNode*> args;
        while(1){
            ASTNode* eq = equality(ctx, expression_ctx);
            if(!eq){
                ctx.error_status_ = Error::EXPECTED_EXPRESSION;
                return nullptr;
            }
            args.push_back(eq);
            if(ctx.matchTokenType(TokenType::COMMA)){
                ++ctx;
                continue;
            }
            break;
        }
        if(!args.size() && !ctx.matchTokenType(TokenType::RP)) 
            return nullptr;
        ++ctx;
        ALLOCATE_INIT(ctx.arena_, ret, InNode, val, args, negated);
        return ret;
        //return new InNode(val, args, negated);
    }
    return val;
}

ASTNode* Parser::between(QueryCTX& ctx, ExpressionNode* expression_ctx){
    if((bool)ctx.error_status_) return nullptr;
    ASTNode* val = in(ctx, expression_ctx);
    if(!val) return nullptr;
    if(ctx.matchTokenType(TokenType::BETWEEN) || ctx.matchMultiTokenType({TokenType::NOT, TokenType::BETWEEN})) { 
        bool negated = (ctx.getCurrentToken().type_ == TokenType::NOT);
        if(negated) ++ctx;
        ++ctx;
        ASTNode* lhs = in(ctx, expression_ctx);
        if(!lhs || !ctx.matchTokenType(TokenType::AND))
            return nullptr;
        ++ctx;
        ASTNode* rhs = in(ctx, expression_ctx);
        if(!rhs) {
            return nullptr;
        }
        ASTNode* ret = nullptr;
        ALLOCATE_INIT(ctx.arena_, ret, BetweenNode, val, lhs, rhs, negated);
        return ret;
        //return new BetweenNode(val, lhs, rhs, negated);
    }
    return val;
}

ASTNode* Parser::logic_not(QueryCTX& ctx,ExpressionNode* expression_ctx){
    if((bool)ctx.error_status_) return nullptr;
    if(ctx.matchTokenType(TokenType::NOT)) { 
        //NotNode* lnot = new NotNode();
        NotNode* lnot = nullptr;
        ALLOCATE_INIT(ctx.arena_, lnot, NotNode, nullptr);
        lnot->token_ = ctx.getCurrentToken(); ++ctx;
        lnot->effective_ = true;
        while(ctx.matchTokenType(TokenType::NOT)){
            lnot->effective_ = !lnot->effective_;
            ++ctx;
        }
        lnot->cur_ = reinterpret_cast<BetweenNode*>(between(ctx, expression_ctx));
        return lnot;
    }
    return between(ctx, expression_ctx);
}


ASTNode* Parser::logic_and(QueryCTX& ctx,ExpressionNode* expression_ctx){
    if((bool)ctx.error_status_) return nullptr;
    ASTNode* cur = logic_not(ctx, expression_ctx);
    if(!cur) return nullptr;
    if(ctx.matchTokenType(TokenType::AND)) { 
        AndNode* land = nullptr;
        ALLOCATE_INIT(ctx.arena_, land, AndNode, (NotNode*)cur);
        land->token_ = ctx.getCurrentToken(); ++ctx;
        ASTNode* next = logic_and(ctx, expression_ctx);
        if(!next) {
            return nullptr;
        }
        land->next_ = next;
        return land;
    }
    return cur;
}

ASTNode* Parser::logic_or(QueryCTX& ctx, ExpressionNode* expression_ctx){
    if((bool)ctx.error_status_) return nullptr;
    ASTNode* cur = logic_and(ctx, expression_ctx);
    if(!cur) return nullptr;
    if(ctx.matchTokenType(TokenType::OR)) { 
        OrNode* lor = nullptr; 
        ALLOCATE_INIT(ctx.arena_, lor, OrNode, (AndNode*)cur);

        lor->token_ = ctx.getCurrentToken(); ++ctx;
        ASTNode* next = logic_or(ctx, expression_ctx);
        if(!next) {
            return nullptr;
        }
        lor->next_ = next;
        return lor;
    }
    return cur;
}

ExpressionNode* Parser::expression(QueryCTX& ctx, int query_idx, int id){
    if((bool)ctx.error_status_) return nullptr;
    //ExpressionNode* ex = new ExpressionNode(ctx.queries_call_stack_[0], query_idx);
    ExpressionNode* ex = nullptr;
    ALLOCATE_INIT(ctx.arena_, ex, ExpressionNode, ctx.queries_call_stack_[0], query_idx);
    ex->id_ = id;
    ASTNode* cur = logic_or(ctx, ex);
    if(!cur) {
        //delete ex;
        return nullptr;
    }
    ex->cur_ = cur;
    return ex;
}

void Parser::set_operation(QueryCTX& ctx, int parent_idx){
    if((bool)ctx.error_status_) return;
    QueryData* operation = union_or_except(ctx, parent_idx);
    if(!operation) return;
    if(operation->type_ >= UNION)
        ctx.set_operations_.push_back(operation);
}

QueryData* Parser::intersect(QueryCTX& ctx, int parent_idx){
    if((bool)ctx.error_status_) return nullptr;
    int idx = ctx.queries_call_stack_.size();
    selectStatement(ctx, -1);
    if(idx >= ctx.queries_call_stack_.size()) return nullptr;
    if(ctx.matchTokenType(TokenType::INTERSECT)) { 
        ++ctx;

        Intersect* i  = nullptr;
        ALLOCATE_INIT(ctx.arena_, i,
                Intersect,
                parent_idx, ctx.queries_call_stack_[idx],
                nullptr,
                false);
        //Intersect* i  = new Intersect(parent_idx, ctx.queries_call_stack_[idx], nullptr, false);
        QueryData* next = nullptr;
        if(ctx.matchTokenType(TokenType::ALL)){
            i->all_ = true;
            ++ctx;
        }
        next = intersect(ctx, parent_idx);
        if(!next) {
            return nullptr;
        }
        i->next_ = next;
        return i;
    }
    return ctx.queries_call_stack_[idx]; 
}

QueryData* Parser::union_or_except(QueryCTX& ctx, int parent_idx){
    if((bool)ctx.error_status_) return nullptr;
    QueryData* cur = intersect(ctx, parent_idx);
    if(!cur) return nullptr;
    if(ctx.matchAnyTokenType({TokenType::UNION, TokenType::EXCEPT})) { 
        auto t = QueryType::UNION;
        if(ctx.getCurrentToken().type_ == TokenType::EXCEPT) t = EXCEPT;
        ++ctx;
        //UnionOrExcept* uoe = new UnionOrExcept(t, parent_idx, cur, nullptr, false);
        UnionOrExcept* uoe = nullptr; 
        ALLOCATE_INIT(ctx.arena_, uoe,
                UnionOrExcept,
                t, parent_idx, cur,
                nullptr,
                false);
        if(ctx.matchTokenType(TokenType::ALL)){
            uoe->all_ = true;
            ++ctx;
        }
        QueryData* next = union_or_except(ctx, parent_idx);
        if(!next) {
            return nullptr;
        }
        uoe->next_ = next;
        return uoe;
    }
    return cur;
}

void Parser::selectStatement(QueryCTX& ctx, int parent_idx){
    if((bool)ctx.error_status_) return; 
    ++ctx;
    //SelectStatementData* statement = new SelectStatementData(parent_idx);
    SelectStatementData* statement = nullptr; 
    ALLOCATE_INIT(ctx.arena_, statement,
                SelectStatementData,
                parent_idx);
    statement->idx_ = ctx.queries_call_stack_.size();
    ctx.queries_call_stack_.push_back(statement);

    if(ctx.matchTokenType(TokenType::DISTINCT)){
        ++ctx;
        statement->distinct_ = true;
    } else if(ctx.matchTokenType(TokenType::ALL)){
        ++ctx;
        statement->distinct_ = false;
    }

    // parse selectlist (fields) for the current query.
    selectList(ctx, statement->idx_);
    if(!statement->fields_.size()){
        ctx.error_status_ = Error::EXPECTED_FIELDS;
        return;
    }

    std::vector<ASTNode*> group_by = {};
    if(ctx.matchTokenType(TokenType::FROM)){
        ++ctx;
        tableList(ctx, statement->idx_);
        if(!statement->tables_.size() || statement->table_names_.size() != statement->tables_.size()){
            ctx.error_status_ = Error::EXPECTED_TABLE_LIST;
            return;
        }
    }

    if(ctx.matchTokenType(TokenType::WHERE)){
        ++ctx;
        statement->where_ = expression(ctx,statement->idx_, 0);
        if(!statement->where_) {
            ctx.error_status_ = Error::EXPECTED_EXPRESSION_IN_WHERE_CLAUSE;
            return;
        }
    }

    if(ctx.matchMultiTokenType({TokenType::ORDER, TokenType::BY})){
        ctx += 2;
        orderByList(ctx, statement->idx_);
        if(!statement->order_by_list_.size()){
            ctx.error_status_ = Error::EXPECTED_ORDER_BY_LIST;
            return;
        }
    }

    if(ctx.matchMultiTokenType({TokenType::GROUP, TokenType::BY})){
        ctx += 2;
        groupByList(ctx, statement->idx_);
        if(!statement->group_by_.size()){
            ctx.error_status_ = Error::EXPECTED_GROUP_BY_LIST;
            return;
        }
    }

    if(ctx.matchTokenType(TokenType::HAVING)){
        ++ctx;
        statement->having_ = expression(ctx,statement->idx_, 1);
        if(!statement->having_) {
            ctx.error_status_ = Error::EXPECTED_EXPRESSION_IN_HAVING_CLAUSE;
            return;
        }
    }

}

void Parser::createTableStatement(QueryCTX& ctx, int parent_idx){
    if((bool)ctx.error_status_) return; 
    if(!ctx.matchMultiTokenType({TokenType::CREATE , TokenType::TABLE}))
        return;
    ctx += 2;
    //CreateTableStatementData* statement = new CreateTableStatementData(parent_idx);
    CreateTableStatementData* statement = nullptr; 
    ALLOCATE_INIT(ctx.arena_, statement,
                CreateTableStatementData,
                parent_idx);
    statement->idx_ = ctx.queries_call_stack_.size();
    ctx.queries_call_stack_.push_back(statement);

    if(!ctx.matchTokenType(TokenType::IDENTIFIER)){
        ctx.error_status_ = Error::EXPECTED_IDENTIFIER; 
        return;
    }
    statement->table_name_ = ctx.getCurrentToken().val_; ++ctx;
    if(!ctx.matchTokenType(TokenType::LP)){
        ctx.error_status_ = Error::EXPECTED_LEFT_PARANTH; 
        return;
    }
    ++ctx;
    fieldDefList(ctx, statement->idx_);
    if(!statement->field_defs_.size()){
        ctx.error_status_ = Error::EXPECTED_FIELD_DEFS;
        return;
    }
    if(!ctx.matchTokenType(TokenType::RP)){
        ctx.error_status_ = Error::EXPECTED_RIGHT_PARANTH; 
        return;
    }
    ++ctx;
    ctx.direct_execution_ = 1;
}

void Parser::createIndexStatement(QueryCTX& ctx, int parent_idx){
    if((bool)ctx.error_status_) return; 
    if(ctx.matchMultiTokenType({TokenType::CREATE , TokenType::INDEX})){
      ctx += 2;
    } else if(ctx.matchMultiTokenType({TokenType::CREATE, TokenType::UNIQUE , TokenType::INDEX})){
      ctx += 3;
    } else {
      return;
    }
    //CreateIndexStatementData* statement = new CreateIndexStatementData(parent_idx);
    CreateIndexStatementData* statement = nullptr; 
    ALLOCATE_INIT(ctx.arena_, statement,
                CreateIndexStatementData,
                parent_idx);
    statement->idx_ = ctx.queries_call_stack_.size();
    ctx.queries_call_stack_.push_back(statement);

    if(!ctx.matchTokenType(TokenType::IDENTIFIER)){
        ctx.error_status_ = Error::EXPECTED_IDENTIFIER; 
        return;
    }
    statement->index_name_ = ctx.getCurrentToken().val_; ++ctx;
    if(!ctx.matchMultiTokenType({TokenType::ON, TokenType::IDENTIFIER})){
        ctx.error_status_ = Error::EXPECTED_ON_TABLE_NAME; 
        return;
    }
    ++ctx;
    statement->table_name_ = ctx.getCurrentToken().val_; ++ctx;
    if(!ctx.matchTokenType(TokenType::LP)){
        ctx.error_status_ = Error::EXPECTED_LEFT_PARANTH; 
        return;
    }
    ++ctx;
    while(1){
        if(!ctx.matchTokenType(TokenType::IDENTIFIER)) {
            ctx.error_status_ = Error::EXPECTED_IDENTIFIER;
            return;
        }
        Token token = ctx.getCurrentToken(); ++ctx;
        IndexField f = {
          .name_ = token.val_,
          .desc_ = false // ascending order is the default.
        };
        statement->fields_.push_back(f);
        // TODO: handle desc and asc orders.
        // opptional ASC or DESC
        if(ctx.matchAnyTokenType({TokenType::ASC, TokenType::DESC})) {
            if(ctx.matchTokenType(TokenType::DESC)) 
              statement->fields_[statement->fields_.size()-1].desc_ = true;
            ++ctx;
        }
        if(!ctx.matchTokenType(TokenType::COMMA)) 
            break;
        ++ctx;
    }
    if(!ctx.matchTokenType(TokenType::RP)){
        ctx.error_status_ = Error::EXPECTED_RIGHT_PARANTH; 
        return;
    }
    ++ctx;
    ctx.direct_execution_ = 1;
}

void Parser::insertStatement(QueryCTX& ctx, int parent_idx){
  if((bool)ctx.error_status_) return; 
  if(!ctx.matchMultiTokenType({TokenType::INSERT , TokenType::INTO})){
    ctx.error_status_ = Error::EXPECTED_INSERT_INTO; 
    return;
  }
  ctx += 2;
  //InsertStatementData* statement = new InsertStatementData(parent_idx);
  InsertStatementData* statement = nullptr; 
    ALLOCATE_INIT(ctx.arena_, statement,
                InsertStatementData,
                parent_idx);
  statement->idx_ = ctx.queries_call_stack_.size();
  ctx.queries_call_stack_.push_back(statement);

  if(!ctx.matchTokenType(TokenType::IDENTIFIER)){
    ctx.error_status_ = Error::EXPECTED_IDENTIFIER; 
    return;
  }
  statement->table_name_ = ctx.getCurrentToken().val_; ++ctx;

  // field list is optional.
  if(ctx.matchTokenType(TokenType::LP)){
    ++ctx;
    fieldList(ctx, statement->idx_);
    if(!statement->fields_.size()){
      ctx.error_status_ = Error::EXPECTED_FIELDS;
      return;
    }
    if(!ctx.matchTokenType(TokenType::RP)){
      ctx.error_status_ = Error::EXPECTED_RIGHT_PARANTH; 
      return;
    }
    ++ctx;
  }

  if(ctx.matchTokenType(TokenType::VALUES) ){ // insert into ... values ... syntax
    ++ctx;
    if(!ctx.matchTokenType(TokenType::LP)){
      ctx.error_status_ = Error::EXPECTED_LEFT_PARANTH; 
      return;
    }
    ++ctx;

    expressionList(ctx,statement->idx_);

    if(!statement->values_.size()){
      ctx.error_status_ = Error::EXPECTED_VALUES;
      return;
    }

    if(!ctx.matchTokenType(TokenType::RP)){
      ctx.error_status_ = Error::EXPECTED_RIGHT_PARANTH; 
      return;
    }
    ++ctx;
  }
  else if(ctx.matchTokenType(TokenType::SELECT)){ // insert into ... select from ... syntax
    int select_statement_idx = ctx.queries_call_stack_.size();
    selectStatement(ctx, statement->idx_);
    if((bool)ctx.error_status_) return;
    if(select_statement_idx >= ctx.queries_call_stack_.size()){
      ctx.error_status_ = Error::QUERY_NOT_SUPPORTED;
    }
    statement->select_idx_ = select_statement_idx; 
  } else {
    ctx.error_status_ = Error::EXPECTED_VALUES; 
    return;
  }

}

void Parser::deleteStatement(QueryCTX& ctx, int parent_idx){
    if((bool)ctx.error_status_) return; 
    if(!ctx.matchMultiTokenType({TokenType::DELETE , TokenType::FROM})){
        ctx.error_status_ = Error::QUERY_NOT_SUPPORTED; // TODO: better error.
        return;
    }
    ctx += 2;
    DeleteStatementData* statement = nullptr; 
    ALLOCATE_INIT(ctx.arena_, statement,
            DeleteStatementData,
            parent_idx);
    statement->idx_ = ctx.queries_call_stack_.size();
    ctx.queries_call_stack_.push_back(statement);

    table_with_rename(ctx, statement->idx_);

    // optional using
    if(ctx.matchTokenType(TokenType::USING)) {
        ++ctx;
        tableList(ctx, statement->idx_);
        if(!statement->tables_.size() || statement->table_names_.size() != statement->tables_.size()){
            ctx.error_status_ = Error::EXPECTED_TABLE_LIST;
            return;
        }
    }

    // optional filter
    if(ctx.matchTokenType(TokenType::WHERE)) {
        ++ctx;
        statement->where_ = expression(ctx, statement->idx_, 0);
        if(!statement->where_) {
            ctx.error_status_ = Error::EXPECTED_EXPRESSION_IN_WHERE_CLAUSE;
            return;
        }
    }
}


void Parser::updateStatement(QueryCTX& ctx, int parent_idx){
    if((bool)ctx.error_status_) return; 
    if(!ctx.matchTokenType(TokenType::UPDATE)){
        ctx.error_status_ = Error::QUERY_NOT_SUPPORTED; // TODO: better error.
        return;
    }
    ++ctx;

    UpdateStatementData* statement = nullptr; 
    ALLOCATE_INIT(ctx.arena_, statement,
            UpdateStatementData,
            parent_idx);
    statement->idx_ = ctx.queries_call_stack_.size();
    ctx.queries_call_stack_.push_back(statement);

    table_with_rename(ctx, statement->idx_);

    if(!ctx.matchTokenType(TokenType::SET)){
        ctx.error_status_ = Error::QUERY_NOT_SUPPORTED; // TODO: better error.
        return;
    }
    ++ctx;

    while(true) {
        if(!ctx.matchTokenType(TokenType::IDENTIFIER)){
            ctx.error_status_ = Error::EXPECTED_IDENTIFIER; 
            return;
        }
        statement->fields_.push_back(ctx.getCurrentToken().val_); ++ctx;

        if(!ctx.matchTokenType(TokenType::EQ)){
            ctx.error_status_ = Error::EXPECTED_IDENTIFIER; // TODO: better error.
            return;
        }
        ++ctx;
        auto expr = expression(ctx, statement->idx_, 0);
        if(!expr) {
            ctx.error_status_ = Error::EXPECTED_VALUES; // TODO: better error.
            return;
        }
        statement->values_.push_back(expr);
        if(!ctx.matchTokenType(TokenType::COMMA))
            break;
        ++ctx; // skip ','
    }

    // optional from
    if(ctx.matchTokenType(TokenType::FROM)) {
        ++ctx;
        tableList(ctx, statement->idx_);
        if(!statement->tables_.size() || statement->table_names_.size() != statement->tables_.size()){
            ctx.error_status_ = Error::EXPECTED_TABLE_LIST;
            return;
        }
    }
    // optional filter
    if(ctx.matchTokenType(TokenType::WHERE)) {
        ++ctx;
        statement->where_ = expression(ctx, statement->idx_, 0);
        if(!statement->where_) {
            ctx.error_status_ = Error::EXPECTED_EXPRESSION_IN_WHERE_CLAUSE;
            return;
        }
    }
}
