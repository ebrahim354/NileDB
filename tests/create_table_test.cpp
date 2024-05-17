#include "../src/execution_engine.cpp"
#include "../src/query_processor.cpp"


int main() {
    DiskManager* disk_manager = new DiskManager();
    CacheManager* cache_manager = new CacheManager(1024, disk_manager, 512);
    Catalog* catalog = new Catalog(cache_manager);
    Parser* parser = new Parser(catalog);
    ExecutionEngine* engine = new ExecutionEngine(catalog);
    QueryProcessor* query_processor = new QueryProcessor(parser, engine);
    std::string input1 = "CREATE TABLE first_table ( col1 INT, col2 VARCHAR )";

    query_processor->handleQuery(input1);


    delete disk_manager;
    delete cache_manager;
    delete catalog;
    delete parser;
    delete engine;
    delete query_processor;
    return 0;
}
