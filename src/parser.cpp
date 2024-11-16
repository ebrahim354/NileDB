#pragma once
#include "tokenizer.cpp"
#include "error.cpp"
#include "catalog.cpp"
#include "utils.cpp"
#include "query_ctx.cpp"
#include <cmath>

struct ExpressionNode;
struct QueryData;


// The grammer rules are defined as structures, each struct is following the name convention: CategoryNameNode,
// for example: the constant category the struct is named ConstantNode.
// The reason they are followed by node is because the end result of the parser is suppose to 
// return an abstract syntax tree, the tree is returned by the root node (not as a stand alone AST class for example),
// the reason for this is to allow nested queries and recusion (or at least that's how I view it for now).
// different kinds of category are just structs that inherit the ASTNode's main attributes with 
// additional specialized attributes.
// inheritance is just an easy solution for now, It should be removed entirely when it starts to cause problems.
// The current implementation is going to cause a lot of memory leaks because we are using the heap with the "new"
// keyword without deleting nodes after we are done or in cases of an error, That is bad but we will replace heap
// allocations with some sort of an arena allocator or just handle the leaks later.

#define AGG_FUNC_IDENTIFIER_PREFIX "AGG_FUNC_NUM_";
enum AggregateFuncType {
    NOT_DEFINED,
    COUNT,
    SUM,
    MIN,
    MAX,
    AVG,
};

Type tokenTypeToDBType(TokenType tt){
    // enum Type { INVALID = -1, BOOLEAN, INT, BIGINT, FLOAT, DOUBLE, TIMESTAMP, VARCHAR, NULL_TYPE}; 
    switch(tt){
        case TokenType::BOOLEAN:
            return BOOLEAN;
        case TokenType::INTEGER: // TODO: token types and internal system types should be the same.
            return INT;
        case TokenType::BIGINT:
            return BIGINT;
        case TokenType::FLOAT:
            return FLOAT;
        case TokenType::REAL:
            return DOUBLE;
        case TokenType::TIMESTAMP:
            return TIMESTAMP;
        case TokenType::VARCHAR:
            return VARCHAR;
        default: 
            return INVALID;
    }
}

AggregateFuncType getAggFuncType(TokenType func){
    if(func == TokenType::COUNT) return COUNT;
    if(func == TokenType::SUM)   return SUM;
    if(func == TokenType::MIN)   return MIN;
    if(func == TokenType::MAX)   return MAX;
    if(func == TokenType::AVG)   return AVG;
    return NOT_DEFINED;
}

AggregateFuncType getAggFuncType(std::string& func){
    if(func == "COUNT") return COUNT;
    if(func == "SUM")   return SUM;
    if(func == "MIN")   return MIN;
    if(func == "MAX")   return MAX;
    if(func == "AVG")   return AVG;
    return NOT_DEFINED;
}



enum QueryType {
    SELECT_DATA,
    CREATE_TABLE_DATA,
    INSERT_DATA,
    DELETE_DATA,
    UPDATE_DATA,
};

enum CategoryType {
    FIELD,       
    CASE_EXPRESSION,
                // scoped field is a field of the format: tableName + "." + fieldName
    SCOPED_FIELD,
    STRING_CONSTANT,
    INTEGER_CONSTANT,
    NULL_CONSTANT,
                // grammer of expressions is copied with some modifications from the following link :
                // https://craftinginterpreters.com/appendix-i.html
                // item will be extendted later to have booleans and null
                //
                //
                // scalar functions are either reserved functions or user defined functions. 
                // TODO: user defined functions are not supported yet.
    SCALAR_FUNC,// scalar_func     := identifier "(" expression ")", and expression can contain another function.
                //
                // Nested aggregations are not allowed: count(count(*)).
    AGG_FUNC,   // agg_func     := func_name "(" expression ")", and expression can not contain another aggregation.
    ITEM,       // item         := field  | STRING_CONSTANT | INTEGER_CONSTANT  | "(" expression ")" | agg_func | "(" sub_query ")"
    UNARY,      // unary        := item   | ("-" | "+") uneray
    FACTOR,     // factor       := unary  ( ( "/" | "*" ) unary  )*
    TERM,       // term         := factor ( ( "+" | "-" ) factor )*
    COMPARISON, // comparison   := term   ( ( "<" | ">" | "<=" | ">=" ) term )*
    EQUALITY,   // equality     := comparison   ( ( "=" | "!=" ) comparison )*
    BETWEEN,    // between      := equality | ( equality ("BETWEEN" | "NOT BETWEEN") equality "AND" equality )*
    NOT,        // not          := ( "NOT" )* equality
    AND,        // and          := not     ( ( "AND" ) not )*
    OR,         // or           := and     ( ( "OR" ) and )*
    EXPRESSION, // expression   := or
    PREDICATE,  // sub_query    := select_statment,  only select sub-queries are allowed (for now)
    SUB_QUERY,
    TYPE_CAST,

    TABLE,
    SELECT_STATEMENT,
    SELECT_LIST,
    TABLE_LIST,

    UPDATE_STATEMENT,

    INSERT_STATEMENT,
    FIELD_LIST,
    CONST_LIST,

    DELETE_STATEMENT,


    TYPE_DEF,
    FIELD_DEF,
    FIELD_DEF_LIST,
    CREATE_TABLE_STATEMENT,
    // add views and indexes later.
};

struct ASTNode {
    ASTNode(CategoryType ct, Token val = {}): category_(ct), token_(val)
    {}
    CategoryType category_;
    Token token_; 
};

struct CaseExpressionNode : ASTNode {
    CaseExpressionNode(std::vector<std::pair<ExpressionNode*, ExpressionNode*>> when_then_pairs, ExpressionNode* else_exp, ExpressionNode* initial_value):
        ASTNode(CASE_EXPRESSION), when_then_pairs_(when_then_pairs), else_(else_exp), initial_value_(initial_value)
    {}
    ExpressionNode* initial_value_ = nullptr;
    std::vector<std::pair<ExpressionNode*, ExpressionNode*>> when_then_pairs_; // should be evaluated in order.
    ExpressionNode* else_ = nullptr;
};


// scoped field is a field of the format: tableName + "." + fieldName
struct ScopedFieldNode : ASTNode {
    ScopedFieldNode(Token f, ASTNode* table): ASTNode(SCOPED_FIELD, f), table_(table)
    {}
    ASTNode* table_ = nullptr;
};

struct SubQueryNode : ASTNode {
    SubQueryNode(int idx, int parent_idx): 
        ASTNode(SUB_QUERY), idx_(idx), parent_idx_(parent_idx)
    {}
    int idx_ = -1;
    int parent_idx_ = -1;
    bool used_with_exists_ = false;
};

struct TypeCastNode : ASTNode {
    // TODO: exp should be changed to an argument list of expressions. 
    TypeCastNode(ExpressionNode* exp, Type new_type): 
        ASTNode(TYPE_CAST), exp_(exp), type_(new_type)
    {}
    Type type_ = INVALID;
    ExpressionNode* exp_ = nullptr;
};

struct ScalarFuncNode : ASTNode {
    ScalarFuncNode(std::vector<ExpressionNode*> arguments, std::string name, int parent_id = 0): 
        ASTNode(SCALAR_FUNC), args_(arguments), name_(name), parent_id_(parent_id)
    {}
    std::string name_;
    int parent_id_ = 0;
    std::vector<ExpressionNode*> args_ = {};
};

struct AggregateFuncNode : ASTNode {
    AggregateFuncNode(ExpressionNode* exp, AggregateFuncType type, int parent_id = 0): 
        ASTNode(AGG_FUNC), exp_(exp), type_(type), parent_id_(parent_id)
    {}
    int parent_id_ = 0;
    ExpressionNode* exp_ = nullptr;
    AggregateFuncType type_;
};

struct UnaryNode : ASTNode {
    UnaryNode(ASTNode* val, Token op={}): ASTNode(UNARY, op), cur_(val)
    {}
    ASTNode* cur_ = nullptr;
};

struct FactorNode : ASTNode {
    FactorNode(UnaryNode* lhs, FactorNode* rhs = nullptr, Token op={}): ASTNode(FACTOR, op), cur_(lhs), next_(rhs)
    {}
    UnaryNode* cur_ = nullptr;
    ASTNode* next_ = nullptr;
};

struct TermNode : ASTNode {
    TermNode(FactorNode* lhs, TermNode* rhs = nullptr, Token op={}): ASTNode(TERM, op), cur_(lhs), next_(rhs)
    {}
    FactorNode* cur_ = nullptr;
    ASTNode* next_ = nullptr;
};

struct ComparisonNode : ASTNode {
    ComparisonNode(TermNode* lhs, ComparisonNode* rhs = nullptr, Token op={}): ASTNode(COMPARISON, op), cur_(lhs), next_(rhs)
    {}
    TermNode* cur_ = nullptr;
    ASTNode* next_ = nullptr;
};

struct EqualityNode : ASTNode {
    EqualityNode(ComparisonNode* lhs, EqualityNode* rhs = nullptr, Token op={}): ASTNode(EQUALITY, op), cur_(lhs), next_(rhs)
    {}
    ComparisonNode* cur_ = nullptr;
    ASTNode* next_ = nullptr;
};

struct BetweenNode : ASTNode {
    BetweenNode(ASTNode* val, ASTNode* lhs, ASTNode* rhs, bool negated): 
        ASTNode(BETWEEN), lhs_(lhs), rhs_(rhs), val_(val), negated_(negated)
    {}
    ASTNode* val_ = nullptr;
    ASTNode* lhs_ = nullptr;
    ASTNode* rhs_ = nullptr;
    bool negated_ = false;
};

struct NotNode : ASTNode {
    NotNode(BetweenNode* cur=nullptr, Token op={}): ASTNode(NOT, op), cur_(cur)
    {}
    BetweenNode* cur_ = nullptr;
    // only works if an odd number of NOT operators are listed: NOT 1, NOT NOT NOT 1 = false, but NOT NOT 1 = true,
    // the effective_ variable captures if it works or not.
    bool effective_ = true; 
};


struct AndNode : ASTNode {
    AndNode(NotNode* lhs, AndNode* rhs = nullptr, Token op={}): ASTNode(AND, op), cur_(lhs), next_(rhs)
    {}
    NotNode* cur_ = nullptr;
    ASTNode* next_ = nullptr;
};

struct OrNode : ASTNode {
    OrNode(AndNode* lhs, OrNode* rhs = nullptr, Token op={}): ASTNode(OR, op), cur_(lhs), next_(rhs)
    {}
    AndNode* cur_ = nullptr;
    ASTNode* next_ = nullptr;
};

struct ExpressionNode : ASTNode {
    ExpressionNode(QueryData* top_level_statement, int query_idx, ASTNode* val = nullptr): 
        ASTNode(EXPRESSION), cur_(val), query_idx_(query_idx), top_level_statement_(top_level_statement)
    {}
    int id_ = 0; // 0 means it's a single expression => usually used in a where clause and can't have aggregations.
    int query_idx_ = -1; // the index of the query that this expression belongs to, -1 means top level query
    QueryData* top_level_statement_ = nullptr;
    ASTNode* cur_ = nullptr;
    AggregateFuncNode* aggregate_func_ = nullptr; // each expression can hold at most 1 aggregate function inside of it.
                                                  // meaning that expressions with aggregate functions can't be nested.
};

// SQL statement data wrappers.
struct QueryData {
    QueryData(QueryType type, int parent_idx): type_(type), parent_idx_(parent_idx)
    {}
    QueryType type_;
    int idx_ = -1;          // every query must have an id starting from 0 even the top level query.
    int parent_idx_ = -1;   // -1 means this query is the top level query.
};

struct SelectStatementData : QueryData {

    SelectStatementData(int parent_idx): QueryData(SELECT_DATA, parent_idx)
    {}
    ~SelectStatementData() {}

    std::vector<ExpressionNode*> fields_ = {};
    std::vector<std::string> field_names_ = {};
    std::vector<std::string> table_names_ = {};
    std::vector<AggregateFuncNode*> aggregates_;  
    bool has_star_ = false;
    std::vector<std::string> tables_ = {};
    ExpressionNode* where_ = nullptr;
    ExpressionNode* having_ = nullptr;
    std::vector<int> order_by_list_ = {};
    std::vector<ASTNode*> group_by_ = {}; 
    bool distinct_ = false;
};

// more data will be added
struct FieldDef {
    std::string field_name_;
    TokenType type_; 
};

struct CreateTableStatementData : QueryData {

    CreateTableStatementData(int parent_idx): QueryData(CREATE_TABLE_DATA, parent_idx){}
    ~CreateTableStatementData() {}


    std::vector<FieldDef> field_defs_ = {};
    std::string table_name_ = {};
};

struct InsertStatementData : QueryData {

    InsertStatementData(int parent_idx): QueryData(INSERT_DATA, parent_idx){}
    ~InsertStatementData() {}


    std::string table_name_ = {};
    std::vector<std::string> fields_ = {};
    std::vector<ExpressionNode*> values_ = {};
};

struct DeleteStatementData : QueryData {

    DeleteStatementData(int parent_idx): QueryData(DELETE_DATA, parent_idx){}
    ~DeleteStatementData() {}


    std::string table_name_ = {};
    ExpressionNode* where_ = nullptr;
};

struct UpdateStatementData : QueryData {

    UpdateStatementData(int parent_idx): QueryData(UPDATE_DATA, parent_idx){}
    ~UpdateStatementData() {}


    std::string table_name_ = {};
    std::string  field_ = {}; 
    ExpressionNode* value_ = nullptr;
    ExpressionNode* where_ = nullptr;
};

class Parser {
    public:
        Parser(Catalog* c): catalog_(c)
        {}
        ~Parser(){}
        /*
        // the user of this method is the one who should check to see if it's ok to use, this table name or not
        // using the catalog.
        // for example: a create statement should check if this table name is used before or not to avoid duplication,
        // however a select statement should check if this table name exists to search inside of it.
        // this usage is applied for all premitive Categories.
        

        PredicateNode* predicate(){
            ASTNode* t = term();
            if(!t) return nullptr;
            PredicateNode* nw_p = new PredicateNode();
            nw_p->term_ = reinterpret_cast<TermNode*>(t);
            // add support for different predicates later.
            if(cur_pos_ < cur_size_ && tokens_[cur_pos_].val_ == "AND"){
                nw_p->token_ = tokens_[cur_pos_++];
                nw_p->next_ = predicate();
            }
            return nw_p;
        }




        FieldListNode* fieldList(){
            ASTNode* f = field();
            if(!f) return nullptr;
            

            FieldListNode* nw_fl = new FieldListNode();
            nw_fl->field_ = f;
            if(cur_pos_ < cur_size_ && tokens_[cur_pos_].val_ == ","){
                cur_pos_++;
                nw_fl->next_ = fieldList();
            }
            return nw_fl;
        }


        DeleteStatementNode* deleteStatement(){
            if(cur_pos_ >= cur_size_ || tokens_[cur_pos_].val_ != "FROM")
                return nullptr;
            cur_pos_++;

            // if there is no filter predicate it is just going to be null.
            DeleteStatementNode* statement = new DeleteStatementNode();

            statement->table_ = table(); 
            if(!statement->table_)
                return nullptr;
            if(cur_pos_ < cur_size_ && tokens_[cur_pos_].val_ == "WHERE"){
                cur_pos_++;
                statement->predicate_ = predicate();
            }
            return statement; 
        }

        UpdateStatementNode* updateStatement(){
            // if there is no filter predicate it is just going to be null.
            UpdateStatementNode* statement = new UpdateStatementNode();

            statement->table_ = this->table(); 
            if(!statement->table_) return nullptr;

            if(cur_pos_ >= cur_size_ || tokens_[cur_pos_].val_ != "SET"){
                statement->clean();
                return nullptr;
            }
            cur_pos_++;

            statement->field_ = field();
            if(!statement->field_) {
                statement->clean();
                return nullptr;
            }

            if(cur_pos_ >= cur_size_ || tokens_[cur_pos_].val_ != "="){
                statement->clean();
                return nullptr;
            }
            cur_pos_++;

            statement->expression_ = expression(nullptr, -1, 0);
            if(!statement->expression_) {
                statement->clean();
                return nullptr;
            }


            if(cur_pos_ < cur_size_ && tokens_[cur_pos_].val_ == "WHERE"){
                cur_pos_++;
                statement->predicate_ = predicate();
            }
            return statement; 
        }
*/

        /*
        // legacy parse command will be replaced in the future.
        ASTNode* parse(std::string& query){
            tokens_ = tokenizer_.tokenize(query);
            cur_size_ = tokens_.size();
            if(cur_size_ == 0 || tokens_[0].type_ != KEYWORD) return nullptr;
            std::string v = tokens_[0].val_;
            cur_pos_ = 1;
            ASTNode* ret = nullptr;

            if(v == "INSERT")
                ret = insertStatement();
            else if(v == "DELETE")
                ret =  deleteStatement();
            else if(v == "UPDATE")
                ret = updateStatement();
            else if(v == "CREATE")
                ret = createTableStatement();

            // invalid query even if it produces a valid AST.
            if(cur_pos_ != cur_size_) return nullptr;

            // current statement is not supported yet.
            return ret;
        }
        */
        ASTNode* constant(QueryCTX& ctx);
        ASTNode* table(QueryCTX& ctx);
        ASTNode* field(QueryCTX& ctx);
        // scoped field is a field of the format: tableName + "." + fieldName
        ASTNode* scoped_field(QueryCTX& ctx);
        ASTNode* case_expression(QueryCTX& ctx, ExpressionNode* expression_ctx);
        ASTNode* type_cast(QueryCTX& ctx, ExpressionNode* expression_ctx);
        ASTNode* scalar_func(QueryCTX& ctx, ExpressionNode* expression_ctx);
        ASTNode* agg_func(QueryCTX& ctx, ExpressionNode* expression_ctx);
        ASTNode* item(QueryCTX& ctx, ExpressionNode* expression_ctx);
        ASTNode* unary(QueryCTX& ctx, ExpressionNode* expression_ctx);
        ASTNode* factor(QueryCTX& ctx, ExpressionNode* expression_ctx);
        ASTNode* term(QueryCTX& ctx,ExpressionNode* expression_ctx = nullptr);
        ASTNode* comparison(QueryCTX& ctx, ExpressionNode* expression_ctx);
        ASTNode* equality(QueryCTX& ctx, ExpressionNode* expression_ctx);
        ASTNode* between(QueryCTX& ctx,ExpressionNode* expression_ctx);
        ASTNode* logic_not(QueryCTX& ctx,ExpressionNode* expression_ctx);
        ASTNode* logic_and(QueryCTX& ctx,ExpressionNode* expression_ctx);
        ASTNode* logic_or(QueryCTX& ctx, ExpressionNode* expression_ctx);
        ExpressionNode* expression(QueryCTX& ctx, int query_idx, int id);

        Value constVal(QueryCTX& ctx);
        void tableList(QueryCTX& ctx, int query_idx);
        void expressionList(QueryCTX& ctx, int query_idx);
        void argumentList(QueryCTX& ctx, int query_idx, std::vector<ExpressionNode*>& args);
        void selectList(QueryCTX& ctx, int query_idx);
        void groupByList(QueryCTX& ctx, int query_idx);
        void orderByList(QueryCTX& ctx, int query_idx);
        void fieldDefList(QueryCTX& ctx, int query_idx);
        void fieldList(QueryCTX& ctx, int query_idx);

        // top level statement is called with parent_idx  as -1.
        void selectStatement(QueryCTX& ctx, int parent_idx);

        void createTableStatement(QueryCTX& ctx, int parent_idx);


        void insertStatement(QueryCTX& ctx, int parent_idx){
            if((bool)ctx.error_status_) return; 
            if(!ctx.matchMultiTokenType({TokenType::INSERT , TokenType::INTO})){
                ctx.error_status_ = Error::EXPECTED_INSERT_INTO; 
                return;
            }
            ctx += 2;
            InsertStatementData* statement = new InsertStatementData(parent_idx);
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

            if(!ctx.matchTokenType(TokenType::VALUES)){
                ctx.error_status_ = Error::EXPECTED_VALUES; 
                return;
            }
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
        case TokenType::STR_CONSTANT:
            return Value(token.val_);
        // TODO: handle floats.
        case TokenType::NUMBER_CONSTANT:
            return Value(str_to_int(token.val_));
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
            selectStatement(ctx,-1);
            break;
        case TokenType::INSERT:
            insertStatement(ctx,-1);
            break;
        // TODO: separate create table and create index.
        case TokenType::CREATE:
            createTableStatement(ctx,-1);
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
            query->aggregates_.push_back(f->aggregate_func_);
        }

        if(ctx.matchTokenType(TokenType::COMMA)){
            ++ctx;
            ++cnt;
            continue;
        }
        break;
    }
}


void Parser::tableList(QueryCTX& ctx, int query_idx){
    if((bool)ctx.error_status_) return; 
    auto query = reinterpret_cast<SelectStatementData*>(ctx.queries_call_stack_[query_idx]);
    while(1){
        if(!ctx.matchTokenType(TokenType::IDENTIFIER)){
          return;
        }

        query->tables_.push_back(ctx.getCurrentToken().val_);
        ++ctx;

        // optional AS keyword.
        bool rename = false;
        if(ctx.matchTokenType(TokenType::AS)){
            ++ctx;
            if(!ctx.matchTokenType(TokenType::IDENTIFIER)){
                return;
            }
            query->table_names_.push_back(ctx.getCurrentToken().val_);
            ++ctx;
            rename = true;
        }

        // optional renaming of a field.
        if(!rename && ctx.matchTokenType(TokenType::IDENTIFIER)){
            query->table_names_.push_back(ctx.getCurrentToken().val_);
            ++ctx;
            rename =  true;
        }

        if(!rename)
            query->table_names_.push_back("");

        if(ctx.matchTokenType(TokenType::COMMA)) {
          ++ctx;
          continue;
        } 

        if(ctx.matchMultiTokenType({TokenType::CROSS, TokenType::JOIN})) {
          ctx += 2;
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
    if(ctx.matchTokenType(TokenType::STR_CONSTANT)){
        ret =  new ASTNode(STRING_CONSTANT, ctx.getCurrentToken()); ++ctx;
    } else if(ctx.matchTokenType(TokenType::NUMBER_CONSTANT)){
        ret = new ASTNode(INTEGER_CONSTANT, ctx.getCurrentToken()); ++ctx;
    } else if(ctx.matchTokenType(TokenType::NULL_CONST)){
        ret = new ASTNode(NULL_CONSTANT, ctx.getCurrentToken()); ++ctx;
    }
    return ret;
}

ASTNode* Parser::table(QueryCTX& ctx){
    if((bool)ctx.error_status_) return nullptr;
    if(ctx.matchTokenType(TokenType::IDENTIFIER)){
        auto token = ctx.getCurrentToken(); ++ctx;
        return new ASTNode(TABLE, token);
    }
    return nullptr;
}

ASTNode* Parser::field(QueryCTX& ctx){
    if((bool)ctx.error_status_) return nullptr;
    if(ctx.matchTokenType(TokenType::IDENTIFIER)) {
        auto token = ctx.getCurrentToken(); ++ctx;
        return new ASTNode(FIELD, token);
    }
    return nullptr;
}

ASTNode* Parser::scoped_field(QueryCTX& ctx){
    if((bool)ctx.error_status_) return nullptr;
    if(ctx.matchMultiTokenType({TokenType::IDENTIFIER, TokenType::DOT, TokenType::IDENTIFIER})) {
        ASTNode* t = table(ctx);
        if(!t) return nullptr;
        ++ctx;
        auto field_name = ctx.getCurrentToken();++ctx;
        return new ScopedFieldNode(field_name, t);
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
        if(!when) return nullptr;
        if(!ctx.matchTokenType(TokenType::THEN)) 
            return nullptr;
        ++ctx;
        auto then = expression(ctx, query_idx, id);
        if(!then) return nullptr;

        when_then_pairs.push_back({when, then});
    }
    // optional ELSE
    if(ctx.matchTokenType(TokenType::ELSE)) {
        ++ctx;
        else_exp = expression(ctx, query_idx, id);
        if(!else_exp) return nullptr;
    }
    // must have END
    if(!ctx.matchTokenType(TokenType::END)) 
        return nullptr;
    ++ctx;
    if(when_then_pairs.size() == 0){
        ctx.error_status_ = Error::INCORRECT_CASE_EXPRESSION; 
        // TODO: use logger.
        std::cout << "[ERROR] incorrect case expression" << std::endl;
        return nullptr;
    }

    return new CaseExpressionNode(when_then_pairs, else_exp, initial_value);
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
    ExpressionNode* exp = expression(ctx, expression_ctx->query_idx_, 0);
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
    return new TypeCastNode(exp, t);
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
    return new ScalarFuncNode(args, name, expression_ctx->id_);
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
    expression_ctx->aggregate_func_ =  new AggregateFuncNode(exp, type, expression_ctx->id_);

    if(!ctx.matchTokenType(TokenType::RP)) return nullptr;
    ++ctx;
    std::string tmp = AGG_FUNC_IDENTIFIER_PREFIX;
    tmp += intToStr(expression_ctx->id_);
    return new ASTNode(FIELD, Token (TokenType::IDENTIFIER, tmp));
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
        SubQueryNode* sub_query_node = new SubQueryNode(sub_query->idx_, sub_query->parent_idx_);
        if(!ctx.matchTokenType(TokenType::RP)){
            return nullptr;
        }
        ++ctx;
        sub_query_node->used_with_exists_ = exists;
        return sub_query_node;
    }
    // nested expressions.
    if(ctx.matchTokenType(TokenType::LP)){
        ++ctx;
        int id = expression_ctx->id_;
        auto ex = expression(ctx, expression_ctx->query_idx_ ,id);
        if(!ex) return nullptr;
        if(!ctx.matchTokenType(TokenType::RP)){
            return nullptr;
        }
        ++ctx;
        return ex;
    }
    if(expression_ctx)
        i = case_expression(ctx, expression_ctx);
    if(!i && expression_ctx && expression_ctx->id_ != 0)
        i = type_cast(ctx , expression_ctx);
    if(!i && expression_ctx && expression_ctx->id_ != 0)
        i = scalar_func(ctx , expression_ctx);
    if(!i && expression_ctx && expression_ctx->id_ != 0)
        i = agg_func(ctx , expression_ctx);
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
        u = new UnaryNode(nullptr, ctx.getCurrentToken());
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
        FactorNode* f = new FactorNode(reinterpret_cast<UnaryNode*>(cur));
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
        TermNode* t = new TermNode(reinterpret_cast<FactorNode*>(cur));
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
        ComparisonNode* c = new ComparisonNode(reinterpret_cast<TermNode*>(cur));
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
        EqualityNode* eq = new EqualityNode(reinterpret_cast<ComparisonNode*>(cur));
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

ASTNode* Parser::between(QueryCTX& ctx,ExpressionNode* expression_ctx){
    if((bool)ctx.error_status_) return nullptr;
    ASTNode* val = equality(ctx, expression_ctx);
    if(!val) return nullptr;
    if(ctx.matchTokenType(TokenType::BETWEEN) || ctx.matchMultiTokenType({TokenType::NOT, TokenType::BETWEEN})) { 
        bool negated = (ctx.getCurrentToken().type_ == TokenType::NOT);
        if(negated) ++ctx;
        ++ctx;
        ASTNode* lhs = equality(ctx, expression_ctx);
        if(!lhs || !ctx.matchTokenType(TokenType::AND))
            return nullptr;
        ++ctx;
        ASTNode* rhs = equality(ctx, expression_ctx);
        if(!rhs) {
            return nullptr;
        }
        return new BetweenNode(val, lhs, rhs, negated);
    }
    return val;
}

ASTNode* Parser::logic_not(QueryCTX& ctx,ExpressionNode* expression_ctx){
    if((bool)ctx.error_status_) return nullptr;
    if(ctx.matchTokenType(TokenType::NOT)) { 
        NotNode* lnot = new NotNode();
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
        AndNode* land = new AndNode(reinterpret_cast<NotNode*>(cur));
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
        OrNode* lor = new OrNode(reinterpret_cast<AndNode*>(cur));
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
    ExpressionNode* ex = new ExpressionNode(ctx.queries_call_stack_[0], query_idx);
    ex->id_ = id;
    ASTNode* cur = logic_or(ctx, ex);
    if(!cur) return nullptr;
    ex->cur_ = cur;
    return ex;
}

void Parser::selectStatement(QueryCTX& ctx, int parent_idx){
    if((bool)ctx.error_status_) return; 
    ++ctx;
    SelectStatementData* statement = new SelectStatementData(parent_idx);
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
        statement->having_ = expression(ctx,statement->idx_, 0);
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
    CreateTableStatementData* statement = new CreateTableStatementData(parent_idx);
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
