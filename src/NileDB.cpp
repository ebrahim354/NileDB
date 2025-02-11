#pragma once
#include "cache_manager.cpp"
#include "catalog.cpp"
#include "disk_manager.cpp"
#include "parser.cpp"
#include "execution_engine.cpp"
#include "query_processor.cpp"


class NileDB {
    private:
        DiskManager* disk_manager_ = new DiskManager();
        CacheManager* cache_manager_ = nullptr;
        Catalog* catalog_ = nullptr;
        Parser* parser_ = nullptr;
        ExecutionEngine* engine_ = nullptr;
        QueryProcessor* query_processor_ = nullptr;
        AlgebraEngine* algebra_engine_ = nullptr;
    public:
        NileDB(size_t pool_size = 1024, size_t k=512)
        {
            cache_manager_ = new CacheManager(pool_size, disk_manager_, k);
            catalog_ = new Catalog(cache_manager_);
            parser_ = new Parser(catalog_);
            engine_ = new ExecutionEngine(catalog_);
            algebra_engine_ = new AlgebraEngine(catalog_);
            query_processor_ = new QueryProcessor(parser_, engine_, algebra_engine_);
        }
        ~NileDB(){
            delete query_processor_;
            delete engine_;
            delete parser_;
            delete catalog_;
            delete cache_manager_;
            delete disk_manager_;
        }
        bool SQL(std::string query, QueryResult* result){
            std::cout << "[INFO] runing the following query: " << query << std::endl;
            /*
            int diff = 'A' - 'a';
            bool inside_string_literal = false;
            for(int i = 0; i < query.size(); i++){
                if(query[i] == '\'') inside_string_literal = !inside_string_literal;
                if(query[i] >= 'a' && query[i] <= 'z' && !inside_string_literal){
                    query[i] += diff;
                }
            }*/
            return query_processor_->handleQuery(query, result);
        }
        bool CMD(std::string command){
            if(command == "\\t"){
                std::vector<std::string> tables = catalog_->getTableNames();
                for(int i = 0; i < tables.size(); ++i){
                    std::cout << tables[i] << std::endl;
                    catalog_->getTableSchema(tables[i])->printSchema();
                }
                return true;
            }
            return false;
        }
        bool isValidTable(std::string table_name) {
            return catalog_->isValidTable(table_name);
        }
        TableSchema* getTableSchema(std::string table_name){
            return catalog_->getTableSchema(table_name);
        }
};

