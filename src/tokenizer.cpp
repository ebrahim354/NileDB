#pragma once
#include "utils.h"
#include "tokenizer.h"
#include <map>

Token::Token(TokenType type, String8 val): type_(type), val_(val)
{}

Tokenizer::Tokenizer() {
    // reserved keywords
    keywords_.insert({str_lit("NULL"), TokenType::NULL_CONST   });
    keywords_.insert({str_lit("SELECT"), TokenType::SELECT  });
    keywords_.insert({str_lit("ORDER"), TokenType::ORDER   });
    keywords_.insert({str_lit("GROUP"), TokenType::GROUP   });
    keywords_.insert({str_lit("CROSS"), TokenType::CROSS   });
    keywords_.insert({str_lit("JOIN"), TokenType::JOIN    });
    keywords_.insert({str_lit("LEFT"), TokenType::LEFT    });
    keywords_.insert({str_lit("RIGHT"), TokenType::RIGHT   });
    keywords_.insert({str_lit("FULL"), TokenType::FULL    });
    keywords_.insert({str_lit("OUTER"), TokenType::OUTER   });
    keywords_.insert({str_lit("INNER"), TokenType::INNER   });
    keywords_.insert({str_lit("DISTINCT"), TokenType::DISTINCT});
    keywords_.insert({str_lit("ALL"), TokenType::ALL     });
    keywords_.insert({str_lit("BY"), TokenType::BY      });
    keywords_.insert({str_lit("AS"), TokenType::AS      });
    keywords_.insert({str_lit("ON"), TokenType::ON      });
    keywords_.insert({str_lit("HAVING"), TokenType::HAVING  });
    keywords_.insert({str_lit("INSERT"), TokenType::INSERT  });
    keywords_.insert({str_lit("FROM"), TokenType::FROM    });
    keywords_.insert({str_lit("USING"), TokenType::USING   });
    keywords_.insert({str_lit("WHERE"), TokenType::WHERE   });
    keywords_.insert({str_lit("BETWEEN"), TokenType::BETWEEN });
    keywords_.insert({str_lit("NOT"), TokenType::NOT     });
    keywords_.insert({str_lit("IS"), TokenType::IS      });
    keywords_.insert({str_lit("IN"), TokenType::IN      });
    keywords_.insert({str_lit("AND"), TokenType::AND     });
    keywords_.insert({str_lit("OR"), TokenType::OR      });
    keywords_.insert({str_lit("INTO"), TokenType::INTO    });
    keywords_.insert({str_lit("VALUES"), TokenType::VALUES  });
    keywords_.insert({str_lit("DELETE"), TokenType::DELETE  });
    keywords_.insert({str_lit("UPDATE"), TokenType::UPDATE  });
    keywords_.insert({str_lit("SET"), TokenType::SET     });
    keywords_.insert({str_lit("INDEX"), TokenType::INDEX   });
    keywords_.insert({str_lit("PRIMARY"), TokenType::PRIMARY });
    keywords_.insert({str_lit("KEY"), TokenType::KEY     });
    keywords_.insert({str_lit("UNIQUE"), TokenType::UNIQUE  });
    keywords_.insert({str_lit("UNION"), TokenType::UNION   });
    keywords_.insert({str_lit("EXCEPT"), TokenType::EXCEPT  });
    keywords_.insert({str_lit("INTERSECT"),TokenType::INTERSECT});
    keywords_.insert({str_lit("ALL"), TokenType::ALL     });
    keywords_.insert({str_lit("ASC"), TokenType::ASC     });
    keywords_.insert({str_lit("DESC"), TokenType::DESC    });
    keywords_.insert({str_lit("CAST"), TokenType::CAST    });
    keywords_.insert({str_lit("CASE"), TokenType::CASE    });
    keywords_.insert({str_lit("EXISTS"), TokenType::EXISTS  });
    keywords_.insert({str_lit("WHEN"), TokenType::WHEN    });
    keywords_.insert({str_lit("THEN"), TokenType::THEN    });
    keywords_.insert({str_lit("ELSE"), TokenType::ELSE    });
    keywords_.insert({str_lit("NULLIF"), TokenType::NULLIF  });
    keywords_.insert({str_lit("END"), TokenType::END     });
    keywords_.insert({str_lit("FALSE"), TokenType::FALSE   });
    keywords_.insert({str_lit("TRUE"), TokenType::TRUE    });
    keywords_.insert({str_lit("CREATE"), TokenType::CREATE  });
    keywords_.insert({str_lit("DROP"), TokenType::DROP    });
    keywords_.insert({str_lit("TABLE"), TokenType::TABLE   });
    // aggregate functions
    keywords_.insert({str_lit("SUM"), TokenType::SUM  });
    keywords_.insert({str_lit("COUNT"), TokenType::COUNT});
    keywords_.insert({str_lit("AVG"), TokenType::AVG  });
    keywords_.insert({str_lit("MIN"), TokenType::MIN  });
    keywords_.insert({str_lit("MAX"), TokenType::MAX  });
    // datatypes
    data_types_.insert({str_lit("VARCHAR"), TokenType::VARCHAR  });
    data_types_.insert({str_lit("TEXT"), TokenType::TEXT     });
    data_types_.insert({str_lit("INTEGER"), TokenType::INTEGER  });
    data_types_.insert({str_lit("BIGINT"), TokenType::BIGINT   });
    data_types_.insert({str_lit("FLOAT"), TokenType::FLOAT    });
    data_types_.insert({str_lit("REAL"), TokenType::REAL     });
    data_types_.insert({str_lit("TIMESTAMP"), TokenType::TIMESTAMP});
    data_types_.insert({str_lit("BOOLEAN"), TokenType::BOOLEAN  });
    // reserved symbols 
    symbols_.insert({str_lit("<"), TokenType::LT       });
    symbols_.insert({str_lit("<="), TokenType::LTE      });
    symbols_.insert({str_lit(">"), TokenType::GT       });
    symbols_.insert({str_lit(">="), TokenType::GTE      });
    symbols_.insert({str_lit("="), TokenType::EQ       });
    symbols_.insert({str_lit("!="), TokenType::NEQ      });
    symbols_.insert({str_lit("<>"), TokenType::NEQ      });
    symbols_.insert({str_lit("("), TokenType::LP       });
    symbols_.insert({str_lit(")"), TokenType::RP       });
    symbols_.insert({str_lit(";"), TokenType::SEMICOLON});
    symbols_.insert({str_lit("+"), TokenType::PLUS     });
    symbols_.insert({str_lit("-"), TokenType::MINUS    });
    symbols_.insert({str_lit("*"), TokenType::STAR     });
    symbols_.insert({str_lit("/"), TokenType::SLASH    });
    symbols_.insert({str_lit("%"), TokenType::PERCENT  });
    symbols_.insert({str_lit("."), TokenType::DOT      });
    symbols_.insert({str_lit(","), TokenType::COMMA    });
}


bool Tokenizer::isKeyword(String8 t) {
    return keywords_.count(t);
}

bool Tokenizer::isDataType(String8 t) {
    return data_types_.count(t);
}

bool Tokenizer::isSymbol(String8 t) {
    return symbols_.count(t);
}

bool Tokenizer::isDataType(TokenType t) {
    switch(t){
        case TokenType::VARCHAR:
        case TokenType::TEXT:
        case TokenType::INTEGER:
        case TokenType::BIGINT:
        case TokenType::FLOAT:
        case TokenType::REAL:
        case TokenType::TIMESTAMP:
        case TokenType::BOOLEAN:
            return true;
        default:
            return false;
    }
}

bool Tokenizer::isAggFunc(TokenType func) {
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

bool Tokenizer::isAggFunc(String8 f) {
    static String_ieq eq;
             
    if(        eq(f, str_lit("COUNT")) 
            || eq(f, str_lit("SUM")) 
            || eq(f, str_lit("MIN")) 
            || eq(f, str_lit("MAX")) 
            || eq(f, str_lit("AVG"))) return true;
    return false;
}

bool Tokenizer::isMathOp(String8 op) {
    if(op == str_lit("+") || op == str_lit("-") || op == str_lit("*") || op == str_lit("/")) return true;
    return false;
}

bool Tokenizer::isCompareOP(String8 op) {
    if(op == str_lit(">") || op == str_lit("<") || op == str_lit(">=") || op == str_lit("<=")) return true;
    return false;
}

bool Tokenizer::isEqOP(String8 op) {
    if(op == str_lit("=") || op == str_lit("!=") || op == str_lit("<>")) return true;
    return false;
}

bool Tokenizer::isStrConst(String8 t) {
    return t.size_ >= 2 && t[0] == '\'' && t.last_char() == '\'';
}

bool Tokenizer::isNumberConst (String8 t) {
    if(t.size_ < 1) return false;
    // skip negative
    if(t[0] == '-') {
        t.str_++;
        t.size_--;
        if(t.size_ == 0) return false;
    }

    for(i32 i = 0; i < t.size_; ++i)
        if(!char_is_digit(t[i])) return false;

    return true;
}

TokenType Tokenizer::getTokenType(String8 t) {
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

i32 Tokenizer::advance_string_literal(String8 input, u64 starting_offset, u64* str_sz) {
    u64 cur_offset = starting_offset;
    while(cur_offset < input.size_ && input[cur_offset] != '\'') {
        cur_offset++;
    }
    // didn't find the end of the string literal.
    if(cur_offset == input.size_) {
        return -1;
    }
    *str_sz = (cur_offset - starting_offset);
    return 0;
}

void Tokenizer::flush_token(String8 cur_token, Vector<Token>& output){
    if(cur_token.size_ != 0) {
        TokenType type = getTokenType(cur_token);
        output.emplace_back(type, (type < TokenType::TOKENS_WITH_VAL ? cur_token: NULL_STRING8));
    }

}

// return: 0 on success. 
i32 Tokenizer::tokenize(String8 input, Vector<Token>& output) {
    u64 pos = 0;
    bool inside_string_literal = false;
    while(pos < input.size_) {
        if(isWhitespace(input[pos])){
            pos++;
            continue;
        }
        String8 cur_token = {
            .str_  = input.str_ + pos,
            .size_ = 0,
        };
        while(pos < input.size_ && !isWhitespace(input[pos])) {
            if(input[pos] == '\''){
                pos++;
                u64 str_sz = 0;
                i32 err = advance_string_literal(input, pos, &str_sz);
                if(err) return -1;
                output.push_back({TokenType::STR_CONSTANT, {.str_ = input.str_+pos, .size_ = str_sz}});
                pos+= str_sz;
                pos++;
                cur_token.size_ = 0;
                cur_token.str_  = input.str_+pos;
                continue;
            }

            if(isSymbol({.str_ = input.str_+pos, .size_ = 1})) {
                // if we encounter a symbol flush the cur_token first.
                flush_token(cur_token, output);
                cur_token.size_ = 1;
                cur_token.str_  = input.str_+pos;
                // if it's a one char symbol for example: =, >, <
                // check if it can be extended to a two char symbol: >=, <=, ==, <> etc...
                if(pos + 1 < input.size_) {
                    cur_token.size_++;
                    if(isSymbol(cur_token)) pos++;
                    else cur_token.size_--;
                }
                flush_token(cur_token, output);

                pos++;
                cur_token.size_ = 0;
                cur_token.str_  = input.str_+pos;
                continue;
            }
            cur_token.size_++;
            pos++;
        }
        flush_token(cur_token, output);
    }
    return !(pos == input.size_);
}
