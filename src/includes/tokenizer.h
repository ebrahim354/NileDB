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
    TABLE,
    SUM,
    COUNT,
    AVG,
    MIN,
    MAX,
    INVALID_TOKEN,
};

struct Token {
    std::string val_ = "";
    TokenType type_ = TokenType::INVALID_TOKEN;
    Token(TokenType type, std::string val);
};

class Tokenizer {
    public:
        Tokenizer();
        ~Tokenizer(){}

        bool isKeyword(std::string& t);
        bool isDataType(std::string& t);
        bool isSymbol(std::string& t);
        bool isAggFunc(std::string& func);
        bool isMathOp(std::string& op);
        bool isCompareOP(std::string& op);
        bool isEqOP(std::string& op);
        bool isStrConst(std::string& t);
        bool isNumberConst (std::string& t);
        bool isWhitespace(char ch);

        bool isDataType(TokenType type);
        bool isAggFunc(TokenType func);

        TokenType getTokenType(std::string& t);

        void tokenize(std::string& input, std::vector<Token>& output);

    private:
        std::map<std::string, TokenType> keywords_;
        std::map<std::string, TokenType> symbols_;
        std::map<std::string, TokenType> data_types_;
};

#endif // TOKENIZER_H
