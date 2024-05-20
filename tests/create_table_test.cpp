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
    // table should exist before and after system tear down.

    if(!catalog->isValidTable("first_table")){
        std::cout << "ERROR: Could not find table before teardown" << std::endl;
        return 1;
    }

    TableSchema* sch = catalog->getTableSchema("first_table");
    if(!sch) {
        std::cout << "ERROR: Could not fetch Schema before teardown" << std::endl;
        return 1;
    }
    std::stringstream before_tear_down_schema;
    sch->printSchema(before_tear_down_schema);

    // perform the tear down.
    delete query_processor;
    delete engine;
    delete parser;
    delete catalog;
    delete cache_manager;
    delete disk_manager;

    // boot up again.

    disk_manager = new DiskManager();
    cache_manager = new CacheManager(1024, disk_manager, 512);
    catalog = new Catalog(cache_manager);
    parser = new Parser(catalog);
    engine = new ExecutionEngine(catalog);
    query_processor = new QueryProcessor(parser, engine);

    if(!catalog->isValidTable("first_table")){
        std::cout << "ERROR: Could not find table after teardown" << std::endl;
        return 1;
    }

    sch = catalog->getTableSchema("first_table");
    if(!sch) {
        std::cout << "ERROR: Could not fetch Schema after teardown" << std::endl;
        return 1;
    }
    std::stringstream after_tear_down_schema;
    sch->printSchema(after_tear_down_schema);
    if(after_tear_down_schema.str() != before_tear_down_schema.str()){
        std::cout << "ERROR: The Schema has changed after teardown" << std::endl;
        return 1;
    }

    delete query_processor;
    delete engine;
    delete parser;
    delete catalog;
    delete cache_manager;
    delete disk_manager;
    return 0;
}
