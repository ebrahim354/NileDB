#pragma once
#include "utils.cpp"
#include <map>

enum class TokenType {
    // only these require the val_  property.
	NUMBER_CONSTANT=0,
	STR_CONSTANT,
	FLOATING_CONSTANT,
	IDENTIFIER,
    TOKENS_WITH_VAL,
    LT, // symbols
    LTE,
    GT,
    GTE,
    EQ,
    NEQ,
    NOT,
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
    INTEGER,
    BIGINT,
    FLOAT,
    DOUBLE,
    TIMESTAMP,
    BOOLEAN,
	SELECT, // keywords
    ORDER,
    GROUP,
    JOIN,
    INDEX,
    EXISTS,
    CAST,
    CASE,
    WHEN,
    THEN,
    ELSE,
    END,
    NULL_CONST,
    TRUE,
    FALSE,
    BY,
    AS,
    ON,
    HAVING,
    DISTINCT,
    ALL,
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
    TokenType type_ = TokenType::INVALID_TOKEN;
    std::string val_ = "";
    Token(TokenType type = TokenType::INVALID_TOKEN, std::string val = ""): type_(type), val_(val)
    {}
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

Tokenizer::Tokenizer(){
    // reserved keywords
    keywords_.insert({"NULL"    , TokenType::NULL_CONST   });
    keywords_.insert({"SELECT"  , TokenType::SELECT  });
    keywords_.insert({"ORDER"   , TokenType::ORDER   });
    keywords_.insert({"GROUP"   , TokenType::GROUP   });
    keywords_.insert({"JOIN"    , TokenType::JOIN    });
    keywords_.insert({"DISTINCT", TokenType::DISTINCT});
    keywords_.insert({"ALL"     , TokenType::ALL     });
    keywords_.insert({"BY"      , TokenType::BY      });
    keywords_.insert({"AS"      , TokenType::AS      });
    keywords_.insert({"ON"      , TokenType::ON      });
    keywords_.insert({"HAVING"  , TokenType::HAVING  });
    keywords_.insert({"INSERT"  , TokenType::INSERT  });
    keywords_.insert({"FROM"    , TokenType::FROM    });
    keywords_.insert({"WHERE"   , TokenType::WHERE   });
    keywords_.insert({"BETWEEN" , TokenType::BETWEEN });
    keywords_.insert({"NOT"     , TokenType::NOT     });
    keywords_.insert({"AND"     , TokenType::AND     });
    keywords_.insert({"OR"      , TokenType::OR      });
    keywords_.insert({"INTO"    , TokenType::INTO    });
    keywords_.insert({"VALUES"  , TokenType::VALUES  });
    keywords_.insert({"DELETE"  , TokenType::DELETE  });
    keywords_.insert({"UPDATE"  , TokenType::UPDATE  });
    keywords_.insert({"SET"     , TokenType::SET     });
    keywords_.insert({"INDEX"   , TokenType::INDEX   });
    keywords_.insert({"CAST"    , TokenType::CAST    });
    keywords_.insert({"CASE"    , TokenType::CASE    });
    keywords_.insert({"EXISTS"  , TokenType::EXISTS  });
    keywords_.insert({"WHEN"    , TokenType::WHEN    });
    keywords_.insert({"THEN"    , TokenType::THEN    });
    keywords_.insert({"ELSE"    , TokenType::ELSE    });
    keywords_.insert({"END"     , TokenType::END     });
    keywords_.insert({"FALSE"   , TokenType::FALSE   });
    keywords_.insert({"TRUE"    , TokenType::TRUE    });
    keywords_.insert({"CREATE"  , TokenType::CREATE  });
    keywords_.insert({"TABLE"   , TokenType::TABLE   });
    // aggregate functions
    keywords_.insert({"SUM"  , TokenType::SUM  });
    keywords_.insert({"COUNT", TokenType::COUNT});
    keywords_.insert({"AVG"  , TokenType::AVG  });
    keywords_.insert({"MIN"  , TokenType::MIN  });
    keywords_.insert({"MAX"  , TokenType::MAX  });
    // datatypes
    data_types_.insert({"VARCHAR"  , TokenType::VARCHAR  });
    data_types_.insert({"INTEGER"  , TokenType::INTEGER  });
    data_types_.insert({"BIGINT"   , TokenType::BIGINT   });
    data_types_.insert({"FLOAT"    , TokenType::FLOAT    });
    data_types_.insert({"DOUBLE"   , TokenType::DOUBLE   });
    data_types_.insert({"TIMESTAMP", TokenType::TIMESTAMP});
    data_types_.insert({"BOOLEAN"  , TokenType::BOOLEAN  });
    // reserved symbols 
    symbols_.insert({"<" , TokenType::LT       });
    symbols_.insert({"<=", TokenType::LTE      });
    symbols_.insert({">" , TokenType::GT       });
    symbols_.insert({">=", TokenType::GTE      });
    symbols_.insert({"=" , TokenType::EQ       });
    symbols_.insert({"!=", TokenType::NEQ      });
    symbols_.insert({"(" , TokenType::LP       });
    symbols_.insert({")" , TokenType::RP       });
    symbols_.insert({";" , TokenType::SEMICOLON});
    symbols_.insert({"+" , TokenType::PLUS     });
    symbols_.insert({"-" , TokenType::MINUS    });
    symbols_.insert({"*" , TokenType:: STAR    });
    symbols_.insert({"/" , TokenType::SLASH    });
    symbols_.insert({"%" , TokenType::PERCENT  });
    symbols_.insert({"." , TokenType::DOT      });
    symbols_.insert({"," , TokenType::COMMA    });
}

bool Tokenizer::isKeyword(std::string& t){
    return keywords_.count(t);
}

bool Tokenizer::isDataType(std::string& t){
    return data_types_.count(t);
}

bool Tokenizer::isSymbol(std::string& t){
    return symbols_.count(t);
}

bool Tokenizer::isDataType(TokenType t){
    switch(t){
        case TokenType::VARCHAR:
        case TokenType::INTEGER:
        case TokenType::BIGINT:
        case TokenType::FLOAT:
        case TokenType::DOUBLE:
        case TokenType::TIMESTAMP:
        case TokenType::BOOLEAN:
            return true;
        default:
            return false;
    }
}

bool Tokenizer::isAggFunc(TokenType func){
    switch(func){
        case TokenType::SUM:
        case TokenType::COUNT:
        case TokenType::MIN:
        case TokenType::MAX:
        case TokenType::AVG:
            return true;
        default:
            return false;
    }
}

bool Tokenizer::isAggFunc(std::string& func){
    if(func == "SUM" || func == "COUNT" || func == "MIN" || func == "MAX" || func == "AVG") return true;
    return false;
}

bool Tokenizer::isMathOp(std::string& op){
    if(op == "+" || op == "-" || op == "*" || op == "/") return true;
    return false;
}

bool Tokenizer::isCompareOP(std::string& op){
    if(op == ">" || op == "<" || op == ">=" || op == "<=") return true;
    return false;
}

bool Tokenizer::isEqOP(std::string& op){
    if(op == "=" || op == "!=") return true;
    return false;
}

bool Tokenizer::isStrConst(std::string& t){
    return t.size() >= 2 && t[0] == '"' && t[t.size()-1] == '"';
}

bool Tokenizer::isNumberConst (std::string& t){
    return t.size() > 0 && areDigits(t);
}

TokenType Tokenizer::getTokenType(std::string& t) {
    if(isKeyword(t))     return keywords_[t];
    if(isDataType(t))    return data_types_[t];
    if(isSymbol(t))      return symbols_[t];
    if(isStrConst(t))    return TokenType::STR_CONSTANT;
    if(isNumberConst(t)) return TokenType::NUMBER_CONSTANT;
    return TokenType::IDENTIFIER;
}

bool Tokenizer::isWhitespace(char ch) {
    if (ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r' || ch == '\f')
        return true;
    return false;
}

void Tokenizer::tokenize(std::string& input, std::vector<Token>& output){
    size_t pos = 0;
    bool inside_string_literal = false;
    std::string cur_token = "";
    while(pos < input.size()){
        while(pos < input.size() && isWhitespace(input[pos]) && !inside_string_literal){
            pos++;
        }
        while(pos < input.size() && !isWhitespace(input[pos])){
            if(input[pos] == '"'){
                inside_string_literal = !inside_string_literal;
                cur_token += input[pos++];
                continue;
            }
            std::string s = "";
            s += input[pos];
            if(isSymbol(s)){
                std::string tmp; 
                tmp = input[pos++];
                if(pos < input.size()){
                    std::string t = "";
                    t += tmp;
                    t += input[pos];
                    if(isSymbol(t)) tmp += input[pos++];
                }

                if(!cur_token.empty()){
                    TokenType type = getTokenType(cur_token);

                    output.emplace_back(type, (type < TokenType::TOKENS_WITH_VAL ? cur_token: ""));
                    cur_token.clear();
                }

                TokenType type = getTokenType(tmp);
                output.emplace_back(type, (type < TokenType::TOKENS_WITH_VAL ? tmp : ""));
                continue;
            }
            cur_token += input[pos++];
        }

        if(!cur_token.empty()){
            TokenType type = getTokenType(cur_token);

            output.emplace_back(type, (type < TokenType::TOKENS_WITH_VAL ? cur_token: ""));
            cur_token.clear();
        }
    }
}
