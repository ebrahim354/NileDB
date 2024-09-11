#pragma once
#include "utils.cpp"
#include <set>


enum TokenType {
	KEYWORD,
	SYMBOL,
	INT_CONSTANT,
	STR_CONSTANT,
	IDENTIFIER,
    INVALID_TOKEN
};


struct Token {
    std::string val_ = "";
    TokenType type_ = INVALID_TOKEN;
};


class Tokenizer {
    public:

        Tokenizer()
        {
            // reserved keywords
            keywords_.insert("SELECT");
            keywords_.insert("ORDER");
            keywords_.insert("BY");
            keywords_.insert("INSERT");
            keywords_.insert("FROM");
            keywords_.insert("WHERE");
            keywords_.insert("AND");
            keywords_.insert("OR");
            keywords_.insert("INTO");
            keywords_.insert("VALUES");
            keywords_.insert("DELETE");
            keywords_.insert("UPDATE");
            keywords_.insert("SET");
            keywords_.insert("CREATE");
            keywords_.insert("TABLE");
            // datatypes
            data_types_.insert("VARCHAR");
            data_types_.insert("INTEGER");
            data_types_.insert("BIGINT");
            data_types_.insert("FLOAT");
            data_types_.insert("DOUBLE");
            data_types_.insert("TIMESTAMP");
            data_types_.insert("BOOLEAN");
            // reserved symbols 
            symboles_.insert("<");
            symboles_.insert("<=");
            symboles_.insert(">");
            symboles_.insert(">=");
            symboles_.insert("=");
            symboles_.insert("!=");
            symboles_.insert("(");
            symboles_.insert(")");
            symboles_.insert(";");
            symboles_.insert("+");
            symboles_.insert("-");
            symboles_.insert("*");
            symboles_.insert("/");
            symboles_.insert("%");
            symboles_.insert(".");
            symboles_.insert(",");
        }
        ~Tokenizer(){}

        bool isKeyword(std::string& t){
            return keywords_.count(t) || data_types_.count(t);
        }
        bool isDataType(std::string& t){
            return data_types_.count(t);
        }

        bool isMathOp(std::string& op){
            if(!isSymbol(op)) return false;
            if(op == "+" || op == "-" || op == "*" || op == "/") return true;
            return false;
        }

        bool isCompareOP(std::string& op){
            if(!isSymbol(op)) return false;
            if(op == ">" || op == "<" || op == ">=" || op == "<=") return true;
            return false;
        }
        bool isEqOP(std::string& op){
            if(!isSymbol(op)) return false;
            if(op == "=" || op == "!=") return true;
            return false;
        }

        bool isSymbol(std::string& t){
            return symboles_.count(t);
        }

        bool isStrConst(std::string& t){
            return t.size() >= 2 && t[0] == '"' && t[t.size()-1] == '"';
        }

        bool isIntConst (std::string& t){
            return t.size() > 0 && areDigits(t);
        }

        TokenType getTokenType(std::string& t) {
            if(isKeyword(t))   return KEYWORD;
            if(isSymbol(t))    return SYMBOL;
            if(isStrConst(t))  return STR_CONSTANT;
            if(isIntConst(t))  return INT_CONSTANT;
            return IDENTIFIER;
        }

        bool isWhitespace(char ch) {
            if (ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r' || ch == '\f')
                return true;
            return false;
        }

        // tokens must be separated by at least one whitespace charecter.
        // accept for '.' and ',' symbols.
        // for example: <= is one token and < = is considered two tokens and will result an error,
        // this should be fine for now and will be extended later when it becaomes a problem.
        std::vector<Token> tokenize(std::string& input){
            std::vector<Token> result;
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
                        if(!cur_token.empty()){
                            result.push_back({.val_ = cur_token, .type_ = getTokenType(cur_token)});
                            cur_token.clear();
                        }
                        result.push_back({.val_ = tmp, .type_ = getTokenType(tmp)});
                        continue;
                    }
                    cur_token += input[pos++];
                }

                if(!cur_token.empty()){
                    result.push_back({.val_ = cur_token, .type_ = getTokenType(cur_token)});
                    cur_token.clear();
                }
            }
            return result;
        }

    private:
        std::set<std::string> keywords_;
        std::set<std::string> symboles_;
        std::set<std::string> data_types_;

};

