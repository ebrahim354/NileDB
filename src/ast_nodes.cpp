#pragma once
#include "defines.h"


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

AggregateFuncType getAggFuncType(String& func){
    if(func == "COUNT") return COUNT;
    if(func == "SUM")   return SUM;
    if(func == "MIN")   return MIN;
    if(func == "MAX")   return MAX;
    if(func == "AVG")   return AVG;
    return NOT_DEFINED;
}



void ASTNode::init(CategoryType ct,Token val) {
    category_ = ct; 
    token_ = val;
}

void AggregateFuncNode::init(ExpressionNode* exp, AggregateFuncType type, int parent_id) {
    category_ = AGG_FUNC; 
    exp_ = exp;
    type_ = type;
    parent_id_ = parent_id;
}

void ExpressionNode::init(QueryData* top_level_statement, int query_idx, ASTNode* val) {
    category_ = EXPRESSION; 
    cur_ = val;
    query_idx_ = query_idx;
    top_level_statement_ = top_level_statement;
}

void CaseExpressionNode::init (
        Vector<std::pair<ExpressionNode*, ExpressionNode*>> when_then_pairs,
        ExpressionNode* else_exp, 
        ExpressionNode* initial_value) {
    category_ = CASE_EXPRESSION;
    when_then_pairs_ = when_then_pairs;
    else_ = else_exp;
    initial_value_ = initial_value;
}

void NullifExpressionNode::init (ExpressionNode* lhs, ExpressionNode* rhs) {
    category_ = NULLIF_EXPRESSION;
    lhs_ = lhs;
    rhs_ = rhs;
}

void ScopedFieldNode::init(Token f, ASTNode* table) {
    category_ = SCOPED_FIELD; 
    token_ = f;
    table_ = table;
}

void SubQueryNode::init(int idx, int parent_idx) {
    category_ = SUB_QUERY;
    idx_ = idx;
    parent_idx_ = parent_idx;
}

void TypeCastNode::init(ExpressionNode* exp, Type new_type) {
    category_ = TYPE_CAST;
    exp_ = exp;
    type_ = new_type;
}

void ScalarFuncNode::init(Vector<ExpressionNode*> arguments, String name, int parent_id) {
    category_ = SCALAR_FUNC;
    args_ = arguments;
    name_ = name;
    parent_id_ = parent_id;
}

void UnaryNode::init(ASTNode* val, Token op) {
    category_ = UNARY;
    token_ = op;
    cur_ = val;
}

void FactorNode::init(UnaryNode* lhs, FactorNode* rhs, Token op) {
    category_ = FACTOR;
    token_ = op;
    cur_ = lhs;
    next_ = rhs;
}

void TermNode::init(FactorNode* lhs, TermNode* rhs, Token op) {
    category_ = TERM;
    token_ = op;
    cur_ = lhs;
    next_ = rhs;
}

void ComparisonNode::init(TermNode* lhs, ComparisonNode* rhs, Token op) {
    category_ = COMPARISON;
    token_ = op;
    cur_ = lhs;
    next_ = rhs;
}

void EqualityNode::init(ComparisonNode* lhs, EqualityNode* rhs, Token op) {
    category_ = EQUALITY;
    token_ = op;
    cur_ = lhs;
    next_ = rhs;
}

void InNode::init(ASTNode* val, Vector<ASTNode*> list, bool negated) {
    category_ = IN;
    val_ = val;
    list_ = list;
    negated_ = negated;
}

void BetweenNode::init(ASTNode* val, ASTNode* lhs, ASTNode* rhs, bool negated) {
    category_ = BETWEEN;
    lhs_ = lhs;
    rhs_ = rhs;
    val_ = val;
    negated_ = negated;
}

void NotNode::init(BetweenNode* cur, Token op) {
    category_ = NOT;
    token_ = op;
    cur_ = cur;
}

void AndNode::init(NotNode* lhs, AndNode* rhs, Token op) {
    category_ = AND;
    token_ = op;
    cur_ =  lhs;
    next_ =  rhs;
}

void OrNode::init(AndNode* lhs, OrNode* rhs, Token op) {
    category_ = OR;
    token_ = op;
    cur_ = lhs;
    next_ = rhs;
}
