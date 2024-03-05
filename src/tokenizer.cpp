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
            data_types_.insert("varchar");
            data_types_.insert("int");
            data_types_.insert("bigint");
            data_types_.insert("float");
            data_types_.insert("double");
            data_types_.insert("timestamp");
            data_types_.insert("boolean");
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
            return keywords_.count(t) || data_types_.count(t);
        }
        bool isDataType(std::string& t){
            return data_types_.count(t);
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
                while(isWhitespace(input[pos]) && !inside_string_literal){
                    pos++;
                }
                while(!isWhitespace(input[pos])){
                    if(input[pos] == '"'){
                        inside_string_literal = !inside_string_literal;
                        // don't add the double quotes to the string literal token.
                        pos++;
                        continue;
                    }
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
        std::set<std::string> data_types_;

};

