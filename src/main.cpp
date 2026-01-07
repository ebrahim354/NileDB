#include <iostream>
#include <string>
#include <ctime>
#include "NileDB.cpp"


int main() {
    NileDB *ndb = new NileDB();
    bool prompt_is_running = true;
    bool show_results = true;
    bool inside_quotes = false;
    String query = "";
    query.reserve(4096);
    while(prompt_is_running){
        query.clear();
        ndb->flush(); // TODO: remove this.
        std::cout << "> ";
        int cur_char = 0;
        while((cur_char = getchar()) != EOF){
            if(cur_char == ';') break;
            if(cur_char > 0xFF) assert(0 && "TODO: SUPPORT ASCII\n");
            if(cur_char == '\'') inside_quotes = !inside_quotes;
            query += (unsigned char) cur_char;
        }
        Executor* result = nullptr; 
        if(query == "quit") break;
        if(query[0] == '\\'){
            if(query == "\\s") {// toggle show results on and off
                show_results = !show_results;
                continue;
            }
            std::cout << (ndb->CMD(query.c_str()) ? "SUCCESS" : "FAIL") << std::endl;
        } else {
            // linux only timers.
            struct timespec start, finish;
            double elapsed;
            clock_gettime(CLOCK_MONOTONIC, &start);

            QueryCTX query_ctx;
            query_ctx.init(query.c_str(), query.size());
            bool status = ndb->SQL(query_ctx, &result);
            if(!status){
                std::cout << "FAIL" << std::endl;
                query_ctx.clean();
                continue;
            }
            Vector<Vector<String>> full_result;

            u64 row_cnt = 0;
            while(result && !result->error_status_ && !result->finished_){
                Tuple res = result->next();
                if(res.size() == 0 || result->error_status_) break;
                row_cnt++;
                if(show_results){
                    size_t sz = res.size();
                    Vector<String> cur_tuple;
                    cur_tuple.resize(sz);
                    for(int i = 0; i < sz; ++i) {
                        cur_tuple[i] = res.get_val_at(i).toString();
                    }
                    full_result.push_back(cur_tuple);
                }
                query_ctx.temp_arena_.clear();
            }
            // linux only timers.
            clock_gettime(CLOCK_MONOTONIC, &finish);
            elapsed = (finish.tv_nsec - start.tv_nsec);
            elapsed += (finish.tv_sec - start.tv_sec) * 1000000000.0;
            double elapsed_in_ms = elapsed / 1000000.0;

            if(result && result->error_status_) {
                std::cout << "[ERROR] Query finished with an error!\n";
                query_ctx.clean();
                continue;
            }

            for(int j = 0; j < full_result.size(); ++j) {
                for(int i = 0; i < full_result[j].size(); ++i) {
                    std::cout << " " << full_result[j][i] << " ";
                    if(i+1 != full_result[j].size()) std::cout << "|";
                }
                std::cout << "\n";
            }
            std::cout << "rows: " << row_cnt << std::endl;
            std::cout << "SUCCESS\n";
            std::cout << "Time: " << elapsed_in_ms << " ms" << std::endl;
            query_ctx.clean();
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

