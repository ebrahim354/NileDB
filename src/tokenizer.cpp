#pragma once
#include "utils.cpp"
#include <map>

#define AVG_TOKENS_COUNT 20

enum TokenType {
    // only these require the val_  property.
	NUMBER_CONSTANT=0,
	STR_CONSTANT,
	IDENTIFIER,
    TOKENS_WITH_VAL,
    // symbols
    LT,
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
    // datatypes
    VARCHAR,
    INTEGER,
    BIGINT,
    FLOAT,
    DOUBLE,
    TIMESTAMP,
    BOOLEAN,
    // keywords
	SELECT,
    ORDER,
    GROUP,
    JOIN,
    INDEX,
    BY,
    ON,
    HAVING,
    DISTINCT,
    INSERT,
    FROM,
    WHERE,
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
    TokenType type_ = INVALID_TOKEN;
    std::string val_ = "";
    Token(TokenType type = INVALID_TOKEN, std::string val = ""): type_(type), val_(val)
    {}
};


class Tokenizer {
    public:
        Tokenizer();
        ~Tokenizer();

        bool isKeyword(std::string& t);
        bool isDataType(std::string& t);
        bool isSymbol(std::string& t);
        bool isAggFunc(std::string& func);
        bool isMathOp(std::string& op);
        bool isCompareOP(std::string& op);
        bool isEqOP(std::string& op);
        bool isStrConst(std::string& t);
        bool isNumberConst (std::string& t);
        TokenType getTokenType(std::string& t);
        bool isWhitespace(char ch);
        void tokenize(std::string& input, std::vector<Token>& output);

    private:
        std::map<std::string, TokenType> keywords_;
        std::map<std::string, TokenType> symbols_;
        std::map<std::string, TokenType> data_types_;
};

Tokenizer::Tokenizer(){
    // reserved keywords
    keywords_.insert({"SELECT"  , SELECT  });
    keywords_.insert({"ORDER"   , ORDER   });
    keywords_.insert({"GROUP"   , GROUP   });
    keywords_.insert({"JOIN"    , JOIN    });
    keywords_.insert({"DISTINCT", DISTINCT});
    keywords_.insert({"BY"      , BY      });
    keywords_.insert({"ON"      , ON      });
    keywords_.insert({"HAVING"  , HAVING  });
    keywords_.insert({"INSERT"  , INSERT  });
    keywords_.insert({"FROM"    , FROM    });
    keywords_.insert({"WHERE"   , WHERE   });
    keywords_.insert({"AND"     , AND     });
    keywords_.insert({"OR"      , OR      });
    keywords_.insert({"INTO"    , INTO    });
    keywords_.insert({"VALUES"  , VALUES  });
    keywords_.insert({"DELETE"  , DELETE  });
    keywords_.insert({"UPDATE"  , UPDATE  });
    keywords_.insert({"SET"     , SET     });
    keywords_.insert({"INDEX"   , INDEX   });
    keywords_.insert({"CREATE"  , CREATE  });
    keywords_.insert({"TABLE"   , TABLE   });
    // aggregate functions
    keywords_.insert({"SUM"  , SUM  });
    keywords_.insert({"COUNT", COUNT});
    keywords_.insert({"AVG"  , AVG  });
    keywords_.insert({"MIN"  , MIN  });
    keywords_.insert({"MAX"  , MAX  });
    // datatypes
    data_types_.insert({"VARCHAR"  , VARCHAR  });
    data_types_.insert({"INTEGER"  , INTEGER  });
    data_types_.insert({"BIGINT"   , BIGINT   });
    data_types_.insert({"FLOAT"    , FLOAT    });
    data_types_.insert({"DOUBLE"   , DOUBLE   });
    data_types_.insert({"TIMESTAMP", TIMESTAMP});
    data_types_.insert({"BOOLEAN"  , BOOLEAN  });
    // reserved symbols 
    symbols_.insert({"<" , LT       });
    symbols_.insert({"<=", LTE      });
    symbols_.insert({">" , GT       });
    symbols_.insert({">=", GTE      });
    symbols_.insert({"=" , EQ       });
    symbols_.insert({"!=", NEQ      });
    symbols_.insert({"!" , NOT      });
    symbols_.insert({"(" , LP       });
    symbols_.insert({")" , RP       });
    symbols_.insert({";" , SEMICOLON});
    symbols_.insert({"+" , PLUS     });
    symbols_.insert({"-" , MINUS    });
    symbols_.insert({"*" ,  STAR    });
    symbols_.insert({"/" , SLASH    });
    symbols_.insert({"%" , PERCENT  });
    symbols_.insert({"." , DOT      });
    symbols_.insert({"," , COMMA    });
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
    if(isSymbol(t))      return symbols_[t];
    if(isStrConst(t))    return STR_CONSTANT;
    if(isNumberConst(t)) return NUMBER_CONSTANT;
    return IDENTIFIER;
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

                    output.emplace_back(type, (type < TOKENS_WITH_VAL ? cur_token: ""));
                    cur_token.clear();
                }

                TokenType type = getTokenType(tmp);
                output.emplace_back(type, (type < TOKENS_WITH_VAL ? tmp : ""));
                continue;
            }
            cur_token += input[pos++];
        }

        if(!cur_token.empty()){
            TokenType type = getTokenType(cur_token);

            output.emplace_back(type, (type < TOKENS_WITH_VAL ? cur_token: ""));
            cur_token.clear();
        }
    }
}
