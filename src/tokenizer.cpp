#pragma once
#include "utils.cpp"
#include <set>


enum TokenType {
	KEYWORD,
	SYMBOL,
	INT_CONSTANT,
	STR_CONSTANT,
	IDENTIFIER,
};


struct Token {
    std::string val_;
    TokenType type_;
};


class Tokenizer {
    public:

        Tokenizer()
        {
            // reserved keywords
            keywords_.insert("select");
            keywords_.insert("from");
            keywords_.insert("where");
            keywords_.insert("and");
            keywords_.insert("or");
            keywords_.insert("into");
            keywords_.insert("values");
            keywords_.insert("delete");
            keywords_.insert("update");
            keywords_.insert("set");
            keywords_.insert("create");
            keywords_.insert("table");
            // datatypes
            keywords_.insert("varchar");
            keywords_.insert("int");
            keywords_.insert("bigint");
            keywords_.insert("float");
            keywords_.insert("double");
            keywords_.insert("timestamp");
            keywords_.insert("boolean");
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
            symboles_.insert("*");
            symboles_.insert("%");
            symboles_.insert(".");
            symboles_.insert(",");
        }
        ~Tokenizer(){}

        bool isKeyword(std::string& t){
            return keywords_.count(t);
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
        std::vector<Token> tokenize(std::string& input){
            std::vector<Token> result;
            size_t pos = 0;
            while(pos < input.size()){
                while(isWhitespace(input[pos])){
                    pos++;
                }
                std::string cur_token = "";
                while(!isWhitespace(input[pos])){
                    if(input[pos] == '.' || input[pos] == ','){
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

};

