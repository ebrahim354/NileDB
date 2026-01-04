#ifndef AST_NODES_H
#define AST_NODES_H

#include "defines.h"
#include "tokenizer.h"

struct ExpressionNode;
struct AggregateFuncNode;
struct QueryData;
struct ASTNode;
void accessed_fields(ASTNode* expression, Vector<ASTNode*>& fields, bool only_one);

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

Type tokenTypeToDBType(TokenType tt);

AggregateFuncType getAggFuncType(TokenType func);

AggregateFuncType getAggFuncType(String& func);

enum JoinType {
    INNER_JOIN = 0,
    LEFT_JOIN,
    RIGHT_JOIN,
    FULL_JOIN
};

struct JoinedTablesData {
    int lhs_idx_ = -1;
    int rhs_idx_ = -1;
    ExpressionNode* condition_ = nullptr;
    JoinType type_ = INNER_JOIN; // default is inner.
};

// more data will be added
struct FieldDef {
    String field_name_;
    TokenType type_; 
    ConstraintType constraints_;
};

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
    void init(CategoryType ct, Token val = {});

    CategoryType category_;
    Token token_; 
};

struct AggregateFuncNode : ASTNode {
    void init(ExpressionNode* exp, AggregateFuncType type, int parent_id = 0);

    int parent_id_ = 0;
    ExpressionNode* exp_ = nullptr;
    AggregateFuncType type_;
    bool distinct_ = false;
};

struct ExpressionNode : ASTNode {
    void init(QueryData* top_level_statement, int query_idx, ASTNode* val = nullptr);

    int id_ = 0; // 0 means it's a single expression => usually used in a where clause and can't have aggregations.
    int query_idx_ = -1; // the index of the query that this expression belongs to, -1 means top level query
    QueryData* top_level_statement_ = nullptr;
    ASTNode* cur_ = nullptr;
    AggregateFuncNode* aggregate_func_ = nullptr; // each expression can hold at most 1 aggregate function inside of it.
                                                  // meaning that expressions with aggregate functions can't be nested.
};


struct CaseExpressionNode : ASTNode {
    void init (
            Vector<std::pair<ExpressionNode*, ExpressionNode*>> when_then_pairs,
            ExpressionNode* else_exp, 
            ExpressionNode* initial_value);

    ExpressionNode* initial_value_ = nullptr;
    Vector<std::pair<ExpressionNode*, ExpressionNode*>> when_then_pairs_; // should be evaluated in order.
    ExpressionNode* else_ = nullptr;
};

struct NullifExpressionNode : ASTNode {
    void init (ExpressionNode* lhs, ExpressionNode* rhs);

    ExpressionNode* lhs_ = nullptr;
    ExpressionNode* rhs_ = nullptr;
};


// scoped field is a field of the format: tableName + "." + fieldName
struct ScopedFieldNode : ASTNode {
    void init (Token f, ASTNode* table);

    ASTNode* table_ = nullptr;
};

struct SubQueryNode : ASTNode {
    void init (int idx, int parent_idx);

    int idx_ = -1;
    int parent_idx_ = -1;
    bool used_with_exists_ = false;
};

struct TypeCastNode : ASTNode {
    // TODO: exp should be changed to an argument list of expressions. 
    void init (ExpressionNode* exp, Type new_type);

    ExpressionNode* exp_ = nullptr;
    Type type_ = INVALID;
};

struct ScalarFuncNode : ASTNode {
    void init (Vector<ExpressionNode*> arguments, String name, int parent_id = 0);

    Vector<ExpressionNode*> args_ = {};
    String name_;
    int parent_id_ = 0;
};


struct UnaryNode : ASTNode {
    void init (ASTNode* val, Token op={});

    ASTNode* cur_ = nullptr;
};

struct FactorNode : ASTNode {
    void init (UnaryNode* lhs, FactorNode* rhs = nullptr, Token op={});

    UnaryNode* cur_ = nullptr;
    ASTNode* next_ = nullptr;
};

struct TermNode : ASTNode {
    void init (FactorNode* lhs, TermNode* rhs = nullptr, Token op={});

    FactorNode* cur_ = nullptr;
    ASTNode* next_ = nullptr;
};

struct ComparisonNode : ASTNode {
    void init (TermNode* lhs, ComparisonNode* rhs = nullptr, Token op={});

    TermNode* cur_ = nullptr;
    ASTNode* next_ = nullptr;
};

struct EqualityNode : ASTNode {
    void init (ComparisonNode* lhs, EqualityNode* rhs = nullptr, Token op={});

    ComparisonNode* cur_ = nullptr;
    ASTNode* next_ = nullptr;
};

struct InNode : ASTNode {
    void init (ASTNode* val, Vector<ASTNode*> list, bool negated);

    ASTNode* val_ = nullptr;
    Vector<ASTNode*> list_;
    bool negated_ = false;
};

struct BetweenNode : ASTNode {
    void init (ASTNode* val, ASTNode* lhs, ASTNode* rhs, bool negated);

    ASTNode* val_ = nullptr;
    ASTNode* lhs_ = nullptr;
    ASTNode* rhs_ = nullptr;
    bool negated_ = false;
};

struct NotNode : ASTNode {
    void init (BetweenNode* cur = nullptr, Token op = {});

    BetweenNode* cur_ = nullptr;
    // only works if an odd number of NOT operators are listed: NOT 1, NOT NOT NOT 1 = false, but NOT NOT 1 = true,
    // the effective_ variable captures if it works or not.
    bool effective_ = true; 
};


struct AndNode : ASTNode {
    void init (NotNode* lhs, AndNode* rhs = nullptr, Token op={});

    NotNode* cur_ = nullptr;
    ASTNode* next_ = nullptr;
};

struct OrNode : ASTNode {
    void init (AndNode* lhs, OrNode* rhs = nullptr, Token op={});

    AndNode* cur_ = nullptr;
    ASTNode* next_ = nullptr;
};

#endif // AST_NODES_H
