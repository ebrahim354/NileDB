#ifndef TOKENIZER_H
#define TOKENIZER_H

#include <unordered_map>
#include "data_structures.h"
#include <vector>

enum class TokenType {
    // only these require the val_  property.
	NUMBER_CONSTANT=0,
	STR_CONSTANT,
	FLOATING_CONSTANT,
	IDENTIFIER,
    TOKENS_WITH_VAL, // ----- placeholder to check if members have value or not.
    LT, // symbols
    LTE,
    GT,
    GTE,
    EQ,
    NEQ,
    LP,
    RP,
    SEMICOLON,
    PLUS,
    MINUS,
    STAR,
    SLASH,
    PERCENT,
    DOT,
    COMMA, 
    VARCHAR,// datatypes
    TEXT,
    INTEGER,
    BIGINT,
    FLOAT,// float4
    REAL, // float8
    TIMESTAMP,
    BOOLEAN,
    SELECT, // keywords
    ORDER,
    GROUP,
    CROSS,
    JOIN,
    LEFT,
    RIGHT,
    FULL,
    OUTER,
    INNER,
    INDEX,
    PRIMARY,
    KEY,
    UNIQUE,
    UNION,
    EXCEPT,
    INTERSECT,
    ALL,
    ASC,
    DESC,
    EXISTS,
    CAST,
    CASE,
    NOT,
    IN,
    IS,
    ISNOT,
    WHEN,
    THEN,
    ELSE,
    NULLIF,
    END,
    NULL_CONST,
    TRUE,
    FALSE,
    BY,
    AS,
    ON,
    HAVING,
    DISTINCT,
    INSERT,
    FROM,
    USING,
    WHERE,
    BETWEEN,
    AND,
    OR,
    INTO,
    VALUES,
    DELETE,
    UPDATE,
    SET,
    CREATE,
    DROP,
    TABLE,
    SUM,
    COUNT,
    AVG,
    MIN,
    MAX,
    INVALID_TOKEN,
};

struct Token {
    String8 val_;
    TokenType type_ = TokenType::INVALID_TOKEN;
    Token(TokenType type = TokenType::INVALID_TOKEN, String8 val = {});
};

class Tokenizer {
    public:
        Tokenizer();
        ~Tokenizer(){}

        bool isKeyword(String8 t);
        bool isDataType(String8 t);
        bool isSymbol(String8 t);
        bool isAggFunc(String8 func);
        bool isMathOp(String8 op);
        bool isCompareOP(String8 op);
        bool isEqOP(String8 op);
        bool isStrConst(String8 t);
        bool isNumberConst (String8 t);
        bool isWhitespace(char ch);

        bool isDataType(TokenType type);
        bool isAggFunc(TokenType func);

        TokenType getTokenType(String8 t);

        void flush_token(String8 cur_token, Vector<Token>& output);
        i32 advance_string_literal(String8 input, u64 starting_offset, u64* str_sz);
        i32 tokenize(String8 input, Vector<Token>& output);

    private:
        std::unordered_map<String8, TokenType, String_ihash, String_ieq> keywords_;
        std::unordered_map<String8, TokenType, String_ihash, String_ieq> data_types_;
        std::unordered_map<String8, TokenType, String_ihash, String_eq> symbols_;
};

#endif // TOKENIZER_H
