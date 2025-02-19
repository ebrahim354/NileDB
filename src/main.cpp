#include <iostream>
#include <string>
#include <time.h>
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
        if(query == "quit") break;
        if(query[0] == '\\')
            std::cout << (ndb->CMD(query) ? "SUCCESS" : "FAIL") << std::endl;
        else{
            // linux only timers.
            struct timespec start, finish;
            double elapsed;
            clock_gettime(CLOCK_MONOTONIC, &start);

            bool status = ndb->SQL(query, &result);
            if(!status){
                std::cout << "FAIL" << std::endl;
                continue;
            }

            // linux only timers.
            clock_gettime(CLOCK_MONOTONIC, &finish);
            elapsed = (finish.tv_nsec - start.tv_nsec);
            elapsed += (finish.tv_sec - start.tv_sec) * 1000000000.0;
            double elapsed_in_ms = elapsed / 1000000.0;
            std::cout << "rows: " << result.size() << std::endl;
            for(int i = 0; i < result.size(); i++){
                for(int j = 0; j < result[i].size(); j++){
                    std::cout << result[i][j].toString();
                    if(j+1 != result[i].size()) std::cout << "|";
                }
                std::cout << std::endl;
            }
            std::cout << "SUCCESS\n";
            std::cout << "Time: " << elapsed_in_ms << " ms" << std::endl;
        }
        result = QueryResult();
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

