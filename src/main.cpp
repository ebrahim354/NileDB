#include <iostream>
#include <string>
#include "NileDB.cpp"


int main() {
    // ndb is heap allocated to allow multi-threading(TODO).    
    NileDB *ndb = new NileDB();
    bool prompt_is_running = true;
    while(prompt_is_running){
        std::cout << "> ";
        std::string query;
        std::getline(std::cin, query);
        if(query[0] == '\\')
            std::cout << (ndb->CMD(query) ? "SUCCESS" : "FAIL") << std::endl;
        else 
            std::cout << (ndb->SQL(query) ? "SUCCESS" : "FAIL") << std::endl;
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

