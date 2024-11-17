#include <iostream>
#include <string>
#include "NileDB.cpp"


int main() {
    // ndb is heap allocated to allow multi-threading(TODO).    
    NileDB *ndb = new NileDB();
    bool prompt_is_running = true;
    std::string outer_query = "";
    while(prompt_is_running){
        if(!outer_query.size())
            std::cout << "> ";
        std::string tmp = "";
        std::getline(std::cin, tmp);
        if(tmp.size() < 0) continue;
        if(tmp[tmp.size()-1] != ';') {
            outer_query += tmp;
            continue;
        } 
        std::string query = outer_query;
        tmp.pop_back();
        query += tmp;
        outer_query = "";
        QueryResult result =  QueryResult();
        if(query[0] == '\\')
            std::cout << (ndb->CMD(query) ? "SUCCESS" : "FAIL") << std::endl;
        else{
            bool status = ndb->SQL(query, &result);
            if(!status){
                std::cout << "FAIL" << std::endl;
                continue;
            }
            std::cout << "rows: " << result.size() << std::endl;
            for(int i = 0; i < result.size(); i++){
                for(int j = 0; j < result[i].size(); j++){
                    std::cout << result[i][j].toString();
                    if(j+1 != result[i].size()) std::cout << "|";
                }
                std::cout << std::endl;
            }
            std::cout << "SUCCESS" << std::endl;
        }
    }
    delete ndb;
    return 0;
}



/* 
 * INITIAL PROGRAM
 * supported queries:
 * CREATE TABLE <table name> <number of columns>.
 * SELECT FROM <table name> returns the entire table.
 * SELECT FROM <table name> WHERE <column number> = "STRING LITERAL".
 * DELETE FROM <table name> deletes the entire table.
 * INSERT INTO <table name> <n values> and n = number of columns.
 */

