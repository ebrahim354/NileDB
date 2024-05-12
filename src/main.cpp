#include <iostream>
#include <string>
#include "cache_manager.cpp"
#include "catalog.cpp"
#include "disk_manager.cpp"
#include "parser.cpp"
#include "execution_engine.cpp"
#include "query_processor.cpp"







int main() {
    

    DiskManager* disk_manager = new DiskManager();
    CacheManager* cache_manager = new CacheManager(1024, disk_manager, 512);
    Catalog* catalog = new Catalog(cache_manager);
    Parser* parser = new Parser(catalog);
    ExecutionEngine* engine = new ExecutionEngine(catalog);
    QueryProcessor* query_processor_ = new QueryProcessor(parser, engine);
    /*
    if(query_processor_->handleQuery("CREATE TABLE TEST ( tst INT )"))
        std::cout << "SUCCESS" << std::endl;
    else
        std::cout << "FAIL" << std::endl;
        */
    bool prompt_is_running = true;
    while(prompt_is_running){
        std::cout << "> ";
        std::string query;
        std::getline(std::cin, query);
        if(query == "\\t"){
            std::vector<std::string> tables = catalog->getTableNames();
            for(int i = 0; i < tables.size(); ++i){
                std::cout << tables[i] << std::endl;
                catalog->getTableSchema(tables[i])->printSchema();
            }
        }
        else if(query_processor_->handleQuery(query))
            std::cout << "SUCCESS" << std::endl;
        else
            std::cout << "FAIL" << std::endl;
    }
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

