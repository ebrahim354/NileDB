#ifndef TOKENIZER_H
#define TOKENIZER_H

#include <map>
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
    String val_;
    TokenType type_ = TokenType::INVALID_TOKEN;
    Token(TokenType type = TokenType::INVALID_TOKEN, String val = "");
};

class Tokenizer {
    public:
        Tokenizer();
        ~Tokenizer(){}

        bool isKeyword(String& t);
        bool isDataType(String& t);
        bool isSymbol(String& t);
        bool isAggFunc(String& func);
        bool isMathOp(String& op);
        bool isCompareOP(String& op);
        bool isEqOP(String& op);
        bool isStrConst(String& t);
        bool isNumberConst (String& t);
        bool isWhitespace(char ch);

        bool isDataType(TokenType type);
        bool isAggFunc(TokenType func);

        TokenType getTokenType(String& t);

        void tokenize(String& input, Vector<Token>& output);

    private:
        std::map<String, TokenType> keywords_;
        std::map<String, TokenType> symbols_;
        std::map<String, TokenType> data_types_;
};

#endif // TOKENIZER_H
