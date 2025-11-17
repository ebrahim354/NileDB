#pragma once
#include "defines.h"

struct ExpressionNode;
struct AggregateFuncNode;
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
            return FLOAT; // TODO: this should be double.
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


enum CategoryType {
    FIELD,       
    CASE_EXPRESSION,
    NULLIF_EXPRESSION,
                // scoped field is a field of the format: tableName + "." + fieldName
    SCOPED_FIELD,
    STRING_CONSTANT,
    INTEGER_CONSTANT,
    FLOAT_CONSTANT,
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
    EQUALITY,   // equality     := comparison   ( ( "=" | "!=" | "IS" | "IS NOT") comparison )*
    IN,         // IN           := equality | ( equality "IN" "(" (expression)+ ")" )*
    BETWEEN,    // between      := in | ( in ("BETWEEN" | "NOT BETWEEN") in "AND" in )*
    NOT,        // not          := ( "NOT" )* between
    AND,        // and          := not     ( ( "AND" ) not )*
    OR,         // or           := and     ( ( "OR" ) and )*
    EXPRESSION, // expression   := or
    PREDICATE,  // sub_query    := select_statment
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
    // TODO: add views and indexes later.
};

struct ASTNode {
    /*
    ASTNode(CategoryType ct, Token val = {}): 
        category_(ct), token_(val)
    {}
    virtual ~ASTNode(){};
    */
    void init(CategoryType ct,Token val = {}) {
        category_ = ct; 
        token_ = val;
    }

    CategoryType category_;
    Token token_; 
};

struct AggregateFuncNode : ASTNode {
    /*
    AggregateFuncNode(ExpressionNode* exp, AggregateFuncType type, int parent_id = 0): 
        ASTNode(AGG_FUNC), exp_(exp), type_(type), parent_id_(parent_id)
    {}
    ~AggregateFuncNode(){
        delete exp_;
    }
    */
    void init(ExpressionNode* exp, AggregateFuncType type, int parent_id = 0) {
        category_ = AGG_FUNC; 
        exp_ = exp;
        type_ = type;
        parent_id_ = parent_id;
    }
    int parent_id_ = 0;
    ExpressionNode* exp_ = nullptr;
    AggregateFuncType type_;
    bool distinct_ = false;
};

struct ExpressionNode : ASTNode {
    /*ExpressionNode(QueryData* top_level_statement, int query_idx, ASTNode* val = nullptr): 
        ASTNode(EXPRESSION), cur_(val), query_idx_(query_idx), top_level_statement_(top_level_statement)
    {}
    ~ExpressionNode(){
      // TODO: commented for now should not be a comment.
        //delete cur_;
        // belongs to statement can't delete.
        // delete aggregate_func_;
    }*/

    void init(QueryData* top_level_statement, int query_idx, ASTNode* val = nullptr) {
        category_ = EXPRESSION; 
        cur_ = val;
        query_idx_ = query_idx;
        top_level_statement_ = top_level_statement;
    }
    int id_ = 0; // 0 means it's a single expression => usually used in a where clause and can't have aggregations.
    int query_idx_ = -1; // the index of the query that this expression belongs to, -1 means top level query
    QueryData* top_level_statement_ = nullptr;
    ASTNode* cur_ = nullptr;
    AggregateFuncNode* aggregate_func_ = nullptr; // each expression can hold at most 1 aggregate function inside of it.
                                                  // meaning that expressions with aggregate functions can't be nested.
};


struct CaseExpressionNode : ASTNode {
    /*
    CaseExpressionNode(std::vector<std::pair<ExpressionNode*, ExpressionNode*>> when_then_pairs, ExpressionNode* else_exp, ExpressionNode* initial_value):
        ASTNode(CASE_EXPRESSION), when_then_pairs_(when_then_pairs), else_(else_exp), initial_value_(initial_value)
    {}
    ~CaseExpressionNode(){
        delete initial_value_; 
        delete else_;
        for(int i = 0; i < when_then_pairs_.size(); ++i){
            delete when_then_pairs_[i].first;
            delete when_then_pairs_[i].second;
        }
    }*/
    void init (
            std::vector<std::pair<ExpressionNode*, ExpressionNode*>> when_then_pairs,
            ExpressionNode* else_exp, 
            ExpressionNode* initial_value) {
        category_ = CASE_EXPRESSION;
        when_then_pairs_ = when_then_pairs;
        else_ = else_exp;
        initial_value_ = initial_value;
    }
    ExpressionNode* initial_value_ = nullptr;
    std::vector<std::pair<ExpressionNode*, ExpressionNode*>> when_then_pairs_; // should be evaluated in order.
    ExpressionNode* else_ = nullptr;
};

struct NullifExpressionNode : ASTNode {
    /*
    NullifExpressionNode(ExpressionNode* lhs, ExpressionNode* rhs):
        ASTNode(NULLIF_EXPRESSION),
        lhs_(lhs),
        rhs_(rhs)
    {}
    ~NullifExpressionNode(){}
    */
    void init (ExpressionNode* lhs, ExpressionNode* rhs) {
        category_ = NULLIF_EXPRESSION;
        lhs_ = lhs;
        rhs_ = rhs;
    }
    ExpressionNode* lhs_ = nullptr;
    ExpressionNode* rhs_ = nullptr;
};


// scoped field is a field of the format: tableName + "." + fieldName
struct ScopedFieldNode : ASTNode {
    /*
    ScopedFieldNode(Token f, ASTNode* table): ASTNode(SCOPED_FIELD, f), table_(table)
    {}
    ~ScopedFieldNode(){
        delete table_;
    }*/
    void init (Token f, ASTNode* table) {
        category_ = SCOPED_FIELD; 
        token_ = f;
        table_ = table;
    }

    ASTNode* table_ = nullptr;
};

struct SubQueryNode : ASTNode {
    /*
    SubQueryNode(int idx, int parent_idx): 
        ASTNode(SUB_QUERY), idx_(idx), parent_idx_(parent_idx)
    {}*/
    void init (int idx, int parent_idx) {
        category_ = SUB_QUERY;
        idx_ = idx;
        parent_idx_ = parent_idx;
    }
    int idx_ = -1;
    int parent_idx_ = -1;
    bool used_with_exists_ = false;
};

struct TypeCastNode : ASTNode {
    // TODO: exp should be changed to an argument list of expressions. 
    /*
    TypeCastNode(ExpressionNode* exp, Type new_type): 
        ASTNode(TYPE_CAST), exp_(exp), type_(new_type)
    {}
    ~TypeCastNode(){
        delete exp_;
    }*/
    void init (ExpressionNode* exp, Type new_type) {
        category_ = TYPE_CAST;
        exp_ = exp;
        type_ = new_type;
    }
    ExpressionNode* exp_ = nullptr;
    Type type_ = INVALID;
};

struct ScalarFuncNode : ASTNode {
    /*
    ScalarFuncNode(std::vector<ExpressionNode*> arguments, std::string name, int parent_id = 0): 
        ASTNode(SCALAR_FUNC), args_(arguments), name_(name), parent_id_(parent_id)
    {}
    ~ScalarFuncNode(){
        for(int i = 0; i < args_.size(); ++i)
            delete args_[i];
    }*/
    void init (std::vector<ExpressionNode*> arguments, std::string name, int parent_id = 0) {
        category_ = SCALAR_FUNC;
        args_ = arguments;
        name_ = name;
        parent_id_ = parent_id;
    }

    std::vector<ExpressionNode*> args_ = {};
    std::string name_;
    int parent_id_ = 0;
};


struct UnaryNode : ASTNode {
    /*
    UnaryNode(ASTNode* val, Token op={}): ASTNode(UNARY, op), cur_(val)
    {}
    ~UnaryNode(){
        delete cur_;
    }*/
    void init (ASTNode* val, Token op={}) {
        category_ = UNARY;
        token_ = op;
        cur_ = val;
    }

    ASTNode* cur_ = nullptr;
};

struct FactorNode : ASTNode {
    /*
    FactorNode(UnaryNode* lhs, FactorNode* rhs = nullptr, Token op={}): ASTNode(FACTOR, op), cur_(lhs), next_(rhs)
    {}
    ~FactorNode(){
        delete cur_;
        delete next_;
    }*/
    void init (UnaryNode* lhs, FactorNode* rhs = nullptr, Token op={}) {
        category_ = FACTOR;
        token_ = op;
        cur_ = lhs;
        next_ = rhs;
    }

    UnaryNode* cur_ = nullptr;
    ASTNode* next_ = nullptr;
};

struct TermNode : ASTNode {
    /*
    TermNode(FactorNode* lhs, TermNode* rhs = nullptr, Token op={}): ASTNode(TERM, op), cur_(lhs), next_(rhs)
    {}
    ~TermNode(){
        delete cur_;
        delete next_;
    }*/
    void init (FactorNode* lhs, TermNode* rhs = nullptr, Token op={}) {
        category_ = TERM;
        token_ = op;
        cur_ = lhs;
        next_ = rhs;
    }

    FactorNode* cur_ = nullptr;
    ASTNode* next_ = nullptr;
};

struct ComparisonNode : ASTNode {
    /*
    ComparisonNode(TermNode* lhs, ComparisonNode* rhs = nullptr, Token op={}): ASTNode(COMPARISON, op), cur_(lhs), next_(rhs)
    {}
    ~ComparisonNode(){
        delete cur_;
        delete next_;
    }*/

    void init (TermNode* lhs, ComparisonNode* rhs = nullptr, Token op={}) {
        category_ = COMPARISON;
        token_ = op;
        cur_ = lhs;
        next_ = rhs;
    }
    TermNode* cur_ = nullptr;
    ASTNode* next_ = nullptr;
};

struct EqualityNode : ASTNode {
/*
    EqualityNode(ComparisonNode* lhs, EqualityNode* rhs = nullptr, Token op={}): ASTNode(EQUALITY, op), cur_(lhs), next_(rhs)
    {}
    ~EqualityNode(){
        delete cur_;
        delete next_;
    }*/

    void init (ComparisonNode* lhs, EqualityNode* rhs = nullptr, Token op={}) {
        category_ = EQUALITY;
        token_ = op;
        cur_ = lhs;
        next_ = rhs;
    }

    ComparisonNode* cur_ = nullptr;
    ASTNode* next_ = nullptr;
};

struct InNode : ASTNode {
    /*
    InNode(ASTNode* val, std::vector<ASTNode*> list, bool negated): 
        ASTNode(IN), val_(val), list_(list), negated_(negated) 
    {}
    ~InNode(){
        delete val_;
        for(int i = 0; i < list_.size(); ++i){
            delete list_[i];
        }
    }*/
    void init (ASTNode* val, std::vector<ASTNode*> list, bool negated) {
        category_ = IN;
        val_ = val;
        list_ = list;
        negated_ = negated;
    }
    ASTNode* val_ = nullptr;
    std::vector<ASTNode*> list_;
    bool negated_ = false;
};

struct BetweenNode : ASTNode {
    /*
    BetweenNode(ASTNode* val, ASTNode* lhs, ASTNode* rhs, bool negated): 
        ASTNode(BETWEEN), lhs_(lhs), rhs_(rhs), val_(val), negated_(negated)
    {}
    ~BetweenNode(){
        delete val_;
        delete lhs_;
        delete rhs_;
    }*/

    void init (ASTNode* val, ASTNode* lhs, ASTNode* rhs, bool negated) {
        category_ = BETWEEN;
        lhs_ = lhs;
        rhs_ = rhs;
        val_ = val;
        negated_ = negated;
    }
    ASTNode* val_ = nullptr;
    ASTNode* lhs_ = nullptr;
    ASTNode* rhs_ = nullptr;
    bool negated_ = false;
};

struct NotNode : ASTNode {
    /*
    NotNode(BetweenNode* cur=nullptr, Token op={}): ASTNode(NOT, op), cur_(cur)
    {}
    ~NotNode(){
        delete cur_;
    }*/

    void init (BetweenNode* cur = nullptr, Token op = {}) {
        category_ = NOT;
        token_ = op;
        cur_ = cur;
    }
    BetweenNode* cur_ = nullptr;
    // only works if an odd number of NOT operators are listed: NOT 1, NOT NOT NOT 1 = false, but NOT NOT 1 = true,
    // the effective_ variable captures if it works or not.
    bool effective_ = true; 
};


struct AndNode : ASTNode {
    /*
    AndNode(NotNode* lhs, AndNode* rhs = nullptr, Token op={}): ASTNode(AND, op), cur_(lhs), next_(rhs)
    {}
    ~AndNode(){
        delete cur_;
        delete next_;
    }*/

    void init (NotNode* lhs, AndNode* rhs = nullptr, Token op={}) {
        category_ = AND;
        token_ = op;
        cur_ =  lhs;
        next_ =  rhs;
    }
    NotNode* cur_ = nullptr;
    ASTNode* next_ = nullptr;
};

struct OrNode : ASTNode {
    /*
    OrNode(AndNode* lhs, OrNode* rhs = nullptr, Token op={}): ASTNode(OR, op), cur_(lhs), next_(rhs)
    {}
    ~OrNode(){
        delete cur_;
        delete next_;
    }*/

    void init (AndNode* lhs, OrNode* rhs = nullptr, Token op={}) {
        category_ = OR;
        token_ = op;
        cur_ = lhs;
        next_ = rhs;
    }
    AndNode* cur_ = nullptr;
    ASTNode* next_ = nullptr;
};

