#pragma once
#include "tokenizer.cpp"
#include "catalog.cpp"
#include "utils.cpp"
#include <cmath>
#include <cstdint>

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

AggregateFuncType getAggFuncType(std::string& func){
    if(func == "COUNT") return COUNT;
    if(func == "SUM")   return SUM;
    if(func == "MIN")   return MIN;
    if(func == "MAX")   return MAX;
    if(func == "AVG")   return AVG;
    return NOT_DEFINED;
}



enum QueryType {
    SELECT_DATA
};

enum CategoryType {
    FIELD,       
                // scoped field is a field of the format: tableName + "." + fieldName
    SCOPED_FIELD,
    STRING_CONSTANT,
    INTEGER_CONSTANT,
                // grammer of expressions is copied with some modifications from the following link :
                // https://craftinginterpreters.com/appendix-i.html
                // item will be extendted later to have booleans and null
                //
                //
                //
                // func_name    := "COUNT" | "MIN" | "MAX" | "SUM" | "AVG" | ...
                // Nested aggregations are not allowed: count(count(*)).
    AGG_FUNC,   // agg_func     := func_name "(" expression ")", and expression can not contain another aggregation.
    ITEM,       // item         := field  | STRING_CONSTANT | INTEGER_CONSTANT  | "(" expression ")" | agg_func | "(" sub_query ")"
    UNARY,      // unary        := item   | ("-") uneray
    FACTOR,     // factor       := unary  ( ( "/" | "*" ) unary  )*
    TERM,       // term         := factor ( ( "+" | "-" ) factor )*
    COMPARISON, // comparison   := term   ( ( "<" | ">" | "<=" | ">=" ) term )*
    EQUALITY,   // equality     := comparison   ( ( "=" | "!=" ) comparison )*
    AND,        // and          := equality     ( ( "AND" ) equality )*
    OR,         // or           := and     ( ( "OR" ) and )*
    EXPRESSION, // expression   := or
    PREDICATE,  // sub_query    := select_statment,  only select sub-queries are allowed (for now)
    SUB_QUERY,

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

struct ExpressionNode;
struct QueryData;

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
    void clean(){
        delete cur_;
    }
    ASTNode* cur_ = nullptr;
};

struct FactorNode : ASTNode {
    FactorNode(UnaryNode* lhs, FactorNode* rhs = nullptr, Token op={}): ASTNode(FACTOR, op), cur_(lhs), next_(rhs)
    {}
    void clean(){
        delete cur_;
        delete next_;
    }
    UnaryNode* cur_ = nullptr;
    ASTNode* next_ = nullptr;
};

struct TermNode : ASTNode {
    TermNode(FactorNode* lhs, TermNode* rhs = nullptr, Token op={}): ASTNode(TERM, op), cur_(lhs), next_(rhs)
    {}
    void clean(){
        delete cur_;
        delete next_;
    }
    FactorNode* cur_ = nullptr;
    ASTNode* next_ = nullptr;
};

struct ComparisonNode : ASTNode {
    ComparisonNode(TermNode* lhs, ComparisonNode* rhs = nullptr, Token op={}): ASTNode(COMPARISON, op), cur_(lhs), next_(rhs)
    {}
    void clean(){
        delete cur_;
        delete next_;
    }
    TermNode* cur_ = nullptr;
    ASTNode* next_ = nullptr;
};

struct EqualityNode : ASTNode {
    EqualityNode(ComparisonNode* lhs, EqualityNode* rhs = nullptr, Token op={}): ASTNode(EQUALITY, op), cur_(lhs), next_(rhs)
    {}
    void clean(){
        delete cur_;
        delete next_;
    }
    ComparisonNode* cur_ = nullptr;
    ASTNode* next_ = nullptr;
};
struct AndNode : ASTNode {
    AndNode(EqualityNode* lhs, AndNode* rhs = nullptr, Token op={}): ASTNode(AND, op), cur_(lhs), next_(rhs)
    {}
    void clean(){
        delete cur_;
        delete next_;
    }
    EqualityNode* cur_ = nullptr;
    ASTNode* next_ = nullptr;
};
struct OrNode : ASTNode {
    OrNode(AndNode* lhs, OrNode* rhs = nullptr, Token op={}): ASTNode(OR, op), cur_(lhs), next_(rhs)
    {}
    void clean(){
        delete cur_;
        delete next_;
    }
    AndNode* cur_ = nullptr;
    ASTNode* next_ = nullptr;
};



struct ExpressionNode : ASTNode {
    ExpressionNode(QueryData* top_level_statement, int query_idx, ASTNode* val = nullptr): 
        ASTNode(EXPRESSION), cur_(val), query_idx_(query_idx), top_level_statement_(top_level_statement)
    {}
    void clean(){
        delete cur_;
    }
    int id_ = 0; // 0 means it's a single expression => usually used in a where clause and can't have aggregations.
    int query_idx_ = -1; // the index of the query that this expression belongs to, -1 means top level query
    QueryData* top_level_statement_ = nullptr;
    ASTNode* cur_ = nullptr;
    AggregateFuncNode* aggregate_func_ = nullptr; // each expression can hold at most 1 aggregate function inside of it.
                                                  // meaning that expressions with aggregate functions can't be nested.
};


struct FieldDefNode : ASTNode {
    FieldDefNode(): ASTNode(FIELD_DEF)
    {}
    void clean() {
        delete field_;
        delete type_;
    }
    ASTNode* field_;
    ASTNode* type_;
};




// this amound of specific list types is why inheritance is just bad. or maybe I'm doing it the wrong way.


// a linked list of constants linked by the ',' sympol.
struct FieldDefListNode : ASTNode {
    FieldDefListNode(): ASTNode(FIELD_DEF_LIST)
    {}
    void clean (){
        if(field_def_) field_def_->clean();
        if(next_) next_->clean();
        delete field_def_;
        delete next_;
    }
    FieldDefNode* field_def_ = nullptr;
    FieldDefListNode* next_ = nullptr;
};

// a linked list of constants linked by the ',' sympol.
struct ConstListNode : ASTNode {
    ConstListNode(): ASTNode(CONST_LIST)
    {}
    void clean (){
        if(next_) next_->clean();
        delete next_;
    }
    ConstListNode* next_ = nullptr;
};

// a linked list of table identifiers linked with the ',' sympol.
struct TableListNode : ASTNode {
    TableListNode(): ASTNode(TABLE_LIST)
    {}
    void clean (){
        if(next_) {
            next_->clean();
            delete as_;
            delete next_;
        }
    }
    ASTNode* as_ = nullptr;
    TableListNode* next_ = nullptr;
};

// a linked list of fields linked with the ',' sympol.
struct SelectListNode : ASTNode {
    SelectListNode(): ASTNode(SELECT_LIST)
    {}
    void clean(){
        if(next_)  next_->clean();
        if(field_) field_->clean();
        delete field_;
        delete next_;
    }
    bool star_ = false;
    ASTNode* as_ = nullptr;
    ExpressionNode* field_ = nullptr;
    SelectListNode* next_ = nullptr;
};

struct FieldListNode : ASTNode {
    FieldListNode(): ASTNode(FIELD_LIST)
    {}
    void clean(){
        if(next_)  next_->clean();
        delete field_;
        delete next_;
    }
    ASTNode* field_ = nullptr;
    FieldListNode* next_ = nullptr;
};

// a linked list of terms linked with keywords (and, or etc...).
struct PredicateNode : ASTNode {
    PredicateNode(Token val = {}): ASTNode(PREDICATE, val)
    {}
    void clean () {
        if(next_) next_->clean();
        if(term_) term_->clean();
        delete term_;
        delete next_;
    }
    TermNode* term_ = nullptr;
    PredicateNode* next_ = nullptr;
};


struct QueryData {
    QueryData(QueryType type = SELECT_DATA): type_(type)
    {}
    QueryType type_;
    // implement later.
};

// SQL statement data wrapper.
struct SelectStatementData : QueryData {

    SelectStatementData(int parent_idx): QueryData(SELECT_DATA), parent_idx_(parent_idx) 
    {}
    ~SelectStatementData() {}

    void init(TableListNode* tables, SelectListNode* fields,
                        ExpressionNode* where, ConstListNode* order_by_list, std::vector<ASTNode*> group_by, bool distinct) {
        distinct_ = distinct;
        group_by_ = group_by;
        where_ = where;
        SelectListNode* field_ptr = fields;
        while(field_ptr != nullptr){
            has_star_ = (has_star_ || field_ptr->star_);
            // if field_ptr->field_ == nullptr, that means it's a select * statement.
            fields_.push_back(field_ptr->field_);
            if(field_ptr->as_ != nullptr)
                field_names_.push_back(field_ptr->as_->token_.val_);
            else 
                field_names_.push_back("");
            if(field_ptr->field_ != nullptr && field_ptr->field_->aggregate_func_ != nullptr){
                aggregates_.push_back(field_ptr->field_->aggregate_func_);
            }
            field_ptr = field_ptr->next_;
        }

        TableListNode* table_ptr = tables; 
        while(table_ptr != nullptr){
            std::string table_name = table_ptr->token_.val_;

            if(table_ptr->as_ != nullptr)
                table_names_.push_back(table_ptr->as_->token_.val_);
            else 
                table_names_.push_back("");

            tables_.push_back(table_name);
            table_ptr = table_ptr->next_;
        }

        ConstListNode* order_by_ptr = order_by_list;
        while(order_by_ptr != nullptr){
            std::string val = order_by_ptr->token_.val_;
            int cur = str_to_int(val);
            order_by_list_.push_back(cur-1); 
            order_by_ptr = order_by_ptr->next_;
        }
    }


    std::vector<ExpressionNode*> fields_ = {};
    std::vector<std::string> field_names_ = {};
    std::vector<std::string> table_names_ = {};
    std::vector<AggregateFuncNode*> aggregates_;  
    bool has_star_ = false;
    std::vector<std::string> tables_ = {};
    ExpressionNode* where_ = nullptr;
    std::vector<int> order_by_list_ = {};
    int idx_ = -1;          // every query must have an id starting from 0 even the top level query.
    int parent_idx_ = -1;   // -1 means this query is the top level query.
    std::vector<QueryData*> queries_call_stack_ = {}; // only used when this query is the top level. 
    std::vector<ASTNode*> group_by_ = {}; 
    bool distinct_ = false;
};

struct CreateTableStatementNode : ASTNode {

    CreateTableStatementNode(): ASTNode(CREATE_TABLE_STATEMENT)
    {}
    void clean() {
        if(field_defs_) field_defs_->clean();
        delete field_defs_;
        delete table_;
    }
    FieldDefListNode* field_defs_ = nullptr;
    ASTNode* table_ = nullptr;
};

struct DeleteStatementNode : ASTNode {

    DeleteStatementNode(): ASTNode(DELETE_STATEMENT)
    {}
    void clean (){
        if(predicate_) predicate_->clean();
        delete table_;
        delete predicate_;
    }
    ASTNode*  table_ = nullptr;
    PredicateNode* predicate_ = nullptr;
};

struct UpdateStatementNode : ASTNode {

    UpdateStatementNode(): ASTNode(UPDATE_STATEMENT)
    {}
    void clean (){
        if(predicate_) predicate_->clean();
        delete field_;
        delete table_;
        delete predicate_;
    }
    ASTNode*  table_ = nullptr;
    ASTNode*  field_ = nullptr;
    ASTNode*  expression_ = nullptr;
    PredicateNode* predicate_ = nullptr;
};

struct InsertStatementNode : ASTNode {

    InsertStatementNode(): ASTNode(INSERT_STATEMENT)
    {}
    void clean (){
        if(fields_) fields_->clean();
        if(values_) values_->clean();
        delete fields_;
        delete values_;
        delete table_;
    }
    ASTNode*  table_;
    FieldListNode* fields_ = nullptr;
    ConstListNode*  values_ = nullptr;
};




class Parser {
    public:
        Parser(Catalog* c): catalog_(c)
        {}
        ~Parser(){}

        ASTNode* field(){
            if(cur_pos_ < cur_size_ && tokens_[cur_pos_].type_ == IDENTIFIER) {
                return new ASTNode(FIELD, tokens_[cur_pos_++]);
            }
            return nullptr;
        }

        // scoped field is a field of the format: tableName + "." + fieldName
        ASTNode* scoped_field(){
            if(cur_pos_+2 < cur_size_ 
                    && tokens_[cur_pos_].type_ == IDENTIFIER 
                    && tokens_[cur_pos_+1].val_ == "."
                    && tokens_[cur_pos_+2].type_ == IDENTIFIER) {
                ASTNode* t = table();
                if(!t) return nullptr;
                cur_pos_++;
                return new ScopedFieldNode(tokens_[cur_pos_++], t);
            }
            return nullptr;
        }

        ASTNode* agg_func(ExpressionNode* expression_ctx){
            if(!expression_ctx) {
                std::cout << "[ERROR] Cannot have aggregate functions in this context" << std::endl;
                return nullptr;
            }
            if(cur_pos_ + 1 >= cur_size_ || tokens_[cur_pos_].type_ != KEYWORD || tokens_[cur_pos_+1].val_ != "(") 
                return nullptr;
            AggregateFuncType type = getAggFuncType(tokens_[cur_pos_].val_);
            if(type == NOT_DEFINED) return nullptr;
            cur_pos_+= 2;
            ExpressionNode* exp = nullptr;
            // only count can have a star parameter.
            if(type == COUNT && cur_pos_ < cur_size_ && tokens_[cur_pos_].val_ == "*"){
                cur_pos_++;
            } else {
                exp = expression(expression_ctx->top_level_statement_, expression_ctx->query_idx_, 0);
                if(!exp) return nullptr;
                if(exp->aggregate_func_) {
                    std::cout << "[ERROR] Cannot nest aggregate functions" << std::endl;
                    return nullptr;
                }
            }
            expression_ctx->aggregate_func_ =  new AggregateFuncNode(exp, type, expression_ctx->id_);

            if(cur_pos_ >= cur_size_ || tokens_[cur_pos_].val_ != ")") return nullptr;
            cur_pos_++; // ")"
            std::string tmp = AGG_FUNC_IDENTIFIER_PREFIX;
            tmp += intToStr(expression_ctx->id_);
            return new ASTNode(FIELD, Token {.val_ = tmp, .type_ = IDENTIFIER });
        }

        ASTNode* type(){
            if(cur_pos_ < cur_size_ && tokens_[cur_pos_].type_ == KEYWORD 
                    && tokenizer_.isDataType(tokens_[cur_pos_].val_)) {
                return new ASTNode(TYPE_DEF, tokens_[cur_pos_++]);
            }
            return nullptr;
        }

        FieldDefNode* fieldDef(){
            auto f = field();
            auto t = type();
            if(!f || !t){ 
                delete f;
                delete t;
                return  nullptr;
            }
            FieldDefNode* fd = new FieldDefNode();
            fd->field_ = f;
            fd->type_  = t;
            return fd;
        }


        // the user of this method is the one who should check to see if it's ok to use, this table name or not
        // using the catalog.
        // for example: a create statement should check if this table name is used before or not to avoid duplication,
        // however a select statement should check if this table name exists to search inside of it.
        // this usage is applied for all premitive Categories.
        ASTNode* table(){
            if(cur_pos_ < cur_size_ && tokens_[cur_pos_].type_ == IDENTIFIER){
                return new ASTNode(TABLE, tokens_[cur_pos_++]);
            }
            return nullptr;
        }
        
        ASTNode* constant(){
            if(cur_pos_ >= cur_size_)
                return nullptr;
            if(tokens_[cur_pos_].type_ == STR_CONSTANT)
                return new ASTNode(STRING_CONSTANT, tokens_[cur_pos_++]);
            if(tokens_[cur_pos_].type_ == INT_CONSTANT)
                return new ASTNode(INTEGER_CONSTANT, tokens_[cur_pos_++]);
            return nullptr;
        }

        ASTNode* item(ExpressionNode* expression_ctx){
            if(!expression_ctx) {
                std::cout << "[ERROR] No expression context provided" << std::endl;
                return nullptr;
            }

            ASTNode* i = nullptr;
            if(cur_pos_ >= cur_size_) return nullptr;
            // check for sub-queries.
            // TODO: extend to no only support select sub-queries.
            if(cur_pos_ + 1 < cur_size_ && tokens_[cur_pos_].val_ == "(" && tokens_[cur_pos_+1].val_ == "SELECT"){
                cur_pos_+=2;
                SelectStatementData* sub_query = 
                    selectStatement(expression_ctx->query_idx_, expression_ctx->top_level_statement_);
                if(!sub_query) {
                    return nullptr;
                }
                SubQueryNode* sub_query_node = new SubQueryNode(sub_query->idx_, sub_query->parent_idx_);
                if(cur_pos_ >= cur_size_ || tokens_[cur_pos_].val_ != ")") {
                    return nullptr;
                }
                cur_pos_++;
                return sub_query_node;
            }
            // nested expressions.
            if(tokens_[cur_pos_].val_ == "("){
                cur_pos_++;
                int id = expression_ctx->id_;
                auto ex = expression(expression_ctx->top_level_statement_, expression_ctx->query_idx_ ,id);
                if(!ex) return nullptr;
                if(cur_pos_ >= cur_size_ || tokens_[cur_pos_].val_ != ")") {
                    return nullptr;
                }
                cur_pos_++;
                return ex;
            }
            if(expression_ctx && expression_ctx->id_ != 0)
                i = agg_func(expression_ctx);
            if(!i)
                i = scoped_field();
            if(!i)
                i = field();
            if(!i)
                i = constant();
            return i;
        }

        ASTNode* unary(ExpressionNode* expression_ctx){
            if(cur_pos_ >= cur_size_) return nullptr;
            if(tokens_[cur_pos_].val_ == "-"){
                UnaryNode* u = nullptr;
                u = new UnaryNode(nullptr, tokens_[cur_pos_]);
                cur_pos_++;
                ASTNode* val = item(expression_ctx);
                if(!val) return nullptr;
                u->cur_ = val;
                
                return u;
            }
            // no need to wrap it in a unary if we don't find any unary operators.
            return item(expression_ctx);
        }

        ASTNode* factor(ExpressionNode* expression_ctx){
            ASTNode* cur = unary(expression_ctx);
            if(!cur) return nullptr;
            if(cur_pos_ < cur_size_ && (tokens_[cur_pos_].val_ == "*" || tokens_[cur_pos_].val_ == "/")) {
                FactorNode* f = new FactorNode(reinterpret_cast<UnaryNode*>(cur));
                ASTNode* next = nullptr;
                f->token_ = tokens_[cur_pos_++];
                next = factor(expression_ctx);
                if(!next) {
                    f->clean();
                    delete cur;
                    return nullptr;
                }
                f->next_ = next;
                return f;
            }
            return cur;
        }

        ASTNode* term(ExpressionNode* expression_ctx = nullptr){
            ASTNode* cur = factor(expression_ctx);
            if(!cur) return nullptr;
            if(cur_pos_ < cur_size_ && (tokens_[cur_pos_].val_ == "+" || tokens_[cur_pos_].val_ == "-")) { 
                TermNode* t = new TermNode(reinterpret_cast<FactorNode*>(cur));
                t->token_ = tokens_[cur_pos_++];
                ASTNode* next = term(expression_ctx);
                if(!next) {
                    t->clean();
                    delete cur;
                    return nullptr;
                }
                t->next_ = next;
                return t;
            }
            return cur;
        }

        ASTNode* comparison(ExpressionNode* expression_ctx){
            ASTNode* cur = term(expression_ctx);
            if(!cur) return nullptr;
            if(cur_pos_ < cur_size_ && tokenizer_.isCompareOP(tokens_[cur_pos_].val_)) { 
                ComparisonNode* c = new ComparisonNode(reinterpret_cast<TermNode*>(cur));
                c->token_ = tokens_[cur_pos_++];
                ASTNode* next = comparison(expression_ctx);
                if(!next) {
                    c->clean();
                    delete cur;
                    return nullptr;
                }
                c->next_ = next;
                return c;
            }
            return cur;
        }

        ASTNode* equality(ExpressionNode* expression_ctx){
            ASTNode* cur = comparison(expression_ctx);
            if(!cur) return nullptr;
            if(cur_pos_ < cur_size_ && tokenizer_.isEqOP(tokens_[cur_pos_].val_)) { 
                EqualityNode* eq = new EqualityNode(reinterpret_cast<ComparisonNode*>(cur));
                eq->token_ = tokens_[cur_pos_++];
                ASTNode* next = equality(expression_ctx);
                if(!next) {
                    eq->clean();
                    delete cur;
                    return nullptr;
                }
                eq->next_ = next;
                return eq;
            }
            return cur;
        }

        ASTNode* logic_and(ExpressionNode* expression_ctx){
            ASTNode* cur = equality(expression_ctx);
            if(!cur) return nullptr;
            if(cur_pos_ < cur_size_ && (tokens_[cur_pos_].val_ == "AND")) { 
                AndNode* land = new AndNode(reinterpret_cast<EqualityNode*>(cur));
                land->token_ = tokens_[cur_pos_++];
                ASTNode* next = logic_and(expression_ctx);
                if(!next) {
                    land->clean();
                    delete cur;
                    return nullptr;
                }
                land->next_ = next;
                return land;
            }
            return cur;
        }

        ASTNode* logic_or(ExpressionNode* expression_ctx){
            ASTNode* cur = logic_and(expression_ctx);
            if(!cur) return nullptr;
            if(cur_pos_ < cur_size_ && (tokens_[cur_pos_].val_ == "OR")) { 
                OrNode* lor = new OrNode(reinterpret_cast<AndNode*>(cur));
                lor->token_ = tokens_[cur_pos_++];
                ASTNode* next = logic_or(expression_ctx);
                if(!next) {
                    lor->clean();
                    delete cur;
                    return nullptr;
                }
                lor->next_ = next;
                return lor;
            }
            return cur;
        }

        ExpressionNode* expression(QueryData* top_level_statement, int query_idx, int id){
            ExpressionNode* ex = new ExpressionNode(top_level_statement, query_idx);
            ex->id_ = id;
            ASTNode* cur = logic_or(ex);
            if(!cur) return nullptr;
            ex->cur_ = cur;
            return ex;
        }

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


        FieldDefListNode* fieldDefList(){
            FieldDefNode* f = fieldDef();
            if(!f) return nullptr;
            FieldDefListNode* nw_fdl = new FieldDefListNode();
            nw_fdl->field_def_ = f;
            if(cur_pos_ < cur_size_ && tokens_[cur_pos_].val_ == ","){
                cur_pos_++;
                nw_fdl->next_ = fieldDefList();
            }
            return nw_fdl;
        }

        SelectListNode* selectList(QueryData* top_level_statement, int query_idx, int expression_id = 1){
            bool star = false;
            int cnt = expression_id;
            ExpressionNode* f = nullptr;
            if(cur_pos_ < cur_size_ && tokens_[cur_pos_].val_ == "*"){
                cur_pos_++;
                star = true;
            } else f = expression(top_level_statement,query_idx,cnt);

            if(!star && !f) {
                return nullptr;
            }
            ASTNode* name = nullptr;

            // optional AS keyword.
            if(!star && cur_pos_ < cur_size_ && tokens_[cur_pos_].val_ == "AS"){
                cur_pos_++;
                name = field();
                if(!name){
                    return nullptr;
                }
            }
            // optional renaming of a field.
            if(!star && !name && cur_pos_ < cur_size_ && tokens_[cur_pos_].type_ == IDENTIFIER){
                name = field();
                if(!name){
                    return nullptr;
                }
            }

            SelectListNode* nw_fl = new SelectListNode();
            nw_fl->field_ = f;
            nw_fl->star_ = star;
            nw_fl->as_ = name;
            if(cur_pos_ < cur_size_ && tokens_[cur_pos_].val_ == ","){
                cur_pos_++;
                nw_fl->next_ = selectList(top_level_statement, query_idx, cnt+1);
            }
            return nw_fl;
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

        TableListNode* tableList(){
            ASTNode* table = this->table();
            if(!table || !catalog_->isValidTable(table->token_.val_)) {
                delete table;
                return nullptr;
            }
            TableListNode* nw_tl = new TableListNode();
            nw_tl->token_ = table->token_;
            delete table;

            // optional AS keyword.
            ASTNode* name = nullptr;
            if(cur_pos_ < cur_size_ && tokens_[cur_pos_].val_ == "AS"){
                cur_pos_++;
                name = field();
                if(!name){
                    return nullptr;
                }
            }
            // optional renaming of a field.
            if(!name && cur_pos_ < cur_size_ && tokens_[cur_pos_].type_ == IDENTIFIER){
                name = field();
                if(!name){
                    return nullptr;
                }
            }
            nw_tl->as_ = name;

            if(cur_pos_ < cur_size_ && tokens_[cur_pos_].val_ == ","){
                nw_tl->token_ = tokens_[++cur_pos_];
                nw_tl->next_ = tableList();
                if(!nw_tl->next_){
                    nw_tl->clean();
                    return nullptr;
                }
            }
            return nw_tl;
        }

        ConstListNode* constList(){
            ASTNode* c = this->constant();
            if(!c) return nullptr;
            ConstListNode* nw_cl = new ConstListNode();
            nw_cl->token_ = c->token_;
            nw_cl->category_ = c->category_;
            if(cur_pos_ < cur_size_ && tokens_[cur_pos_].val_ == ","){
                cur_pos_++;
                nw_cl->next_ = constList();
            }
            return nw_cl;
        }
        // predicate, selectlist, tablelist, constlist and fieldlist 
        // can be improved by using just one list node struct for example. (later)
        //
        std::vector<ASTNode*> groupByList(){
            std::vector<ASTNode*> group_by = {};
            while(true){
                ASTNode* f = scoped_field();
                if(!f) f = field();
                if(!f) return {};
                group_by.push_back(f);
                
                if(cur_pos_ < cur_size_ && tokens_[cur_pos_].val_ == ","){
                    cur_pos_++;
                    continue;
                }
            }
            return group_by;
        }


        
        // top level statement is called with the following args (-1, nullptr).
        SelectStatementData* selectStatement(int parent_idx, QueryData* top_level_statement){

            SelectStatementData* statement = new SelectStatementData(parent_idx);
            QueryData* top_level = nullptr;

            // if current query is the top level we need to pass it if it's not pass the top level one instead.
            if(top_level_statement == nullptr) top_level = statement;
            else top_level = top_level_statement;
            // push current query to the call stack.
            statement->idx_ = reinterpret_cast<SelectStatementData*>(top_level)->queries_call_stack_.size();
            reinterpret_cast<SelectStatementData*>(top_level)->queries_call_stack_.push_back(statement);

            bool distinct = false;
            if(cur_pos_ < cur_size_ && tokens_[cur_pos_].val_ == "DISTINCT"){
                cur_pos_++;
                distinct = true;
            }
            
            SelectListNode* fields = selectList(top_level, statement->idx_);
            TableListNode* tables = nullptr;
            ExpressionNode*  where = nullptr;
            ConstListNode* order_by = nullptr;
            std::vector<ASTNode*> group_by = {};
            if(!fields){
                return nullptr;
            }
            if(cur_pos_ < cur_size_ && tokens_[cur_pos_].val_ == "FROM"){
                cur_pos_++;
                tables = tableList();
                if(!tables){
                    return nullptr;
                }
            }

            if(cur_pos_ < cur_size_ && tokens_[cur_pos_].val_ == "WHERE"){
                cur_pos_++;
                where = expression(top_level, statement->idx_, 0);
            }

            if(cur_pos_+1 < cur_size_ && tokens_[cur_pos_].val_ == "ORDER" && tokens_[cur_pos_+1].val_ == "BY"){
                cur_pos_+=2;
                order_by = constList();
            }

            if(cur_pos_+1 < cur_size_ && tokens_[cur_pos_].val_ == "GROUP" && tokens_[cur_pos_+1].val_ == "BY"){
                cur_pos_+=2;
                group_by = groupByList();
            }

            statement->init(tables, fields, where, order_by, group_by, distinct);
            return statement; 
        }

        CreateTableStatementNode* createTableStatement(){
            if(cur_pos_ >= cur_size_ || tokens_[cur_pos_].val_ != "TABLE")
                return nullptr;
            cur_pos_++;
            auto statement = new CreateTableStatementNode();

            statement->table_ = table();

            if(!statement->table_   || cur_pos_ >= cur_size_ 
                    || tokens_[cur_pos_++].val_ != "(" 
                    || catalog_->isValidTable(statement->table_->token_.val_)){
                statement->clean();
                return nullptr;
            }

            statement->field_defs_ = fieldDefList();


            if(!statement->field_defs_ || cur_pos_ >= cur_size_ 
                       || tokens_[cur_pos_++].val_ != ")"){
                statement->clean();
                return nullptr;
            }
            return statement;
        }



        InsertStatementNode* insertStatement(){
            if(cur_pos_ >= cur_size_ || tokens_[cur_pos_].val_ != "INTO")
                return nullptr;
            cur_pos_++;

            InsertStatementNode* statement = new InsertStatementNode();

            statement->table_ = table();
            if(!statement->table_ || cur_pos_ >= cur_size_ 
                    || tokens_[cur_pos_++].val_ != "(" 
                    || !catalog_->isValidTable(statement->table_->token_.val_)){
                statement->clean();
                return nullptr;
            }

            
            statement->fields_ = fieldList();
            if(!statement->fields_ || cur_pos_ >= cur_size_ 
                       || tokens_[cur_pos_].val_ != ")"){
                statement->clean();
                return nullptr;
            }
            cur_pos_++;

            if(cur_pos_+1 < cur_size_ 
                    && tokens_[cur_pos_].val_ == "VALUES" 
                    && tokens_[cur_pos_+1].val_ == "("){
                cur_pos_+=2;
                statement->values_ = constList();
                if(!statement->values_ || cur_pos_ >= cur_size_ || tokens_[cur_pos_].val_ != ")"){
                    statement->clean();
                    return nullptr;
                }
                cur_pos_++;
            }
            return statement; 
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

        QueryData* new_parse(std::string& query){
            tokens_ = tokenizer_.tokenize(query);
            cur_size_ = tokens_.size();
            if(cur_size_ == 0 || tokens_[0].type_ != KEYWORD) return nullptr;
            std::string v = tokens_[0].val_;
            cur_pos_ = 1;
            QueryData* ret = nullptr;
            if(v == "SELECT")
                ret = selectStatement(-1, nullptr);

            // invalid query even if it produces a valid AST.
            if(cur_pos_ != cur_size_) return nullptr;

            return ret;
        }

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
    private:
        Tokenizer tokenizer_ {};
        std::vector<Token> tokens_;
        Catalog* catalog_;
        uint32_t cur_pos_;
        uint32_t cur_size_;
};


