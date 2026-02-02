#pragma once
#include "data_structures.h"

#include "cache_manager.cpp"
#include "catalog.cpp"
#include "disk_manager.cpp"
#include "parser.cpp"
#include "execution_engine.cpp"
#include "query_processor.cpp"

#include "arena.cpp"


class NileDB {
    private:
        DiskManager* disk_manager_ = new DiskManager();
        CacheManager* cache_manager_ = nullptr;
        Catalog catalog_;
        Parser* parser_ = nullptr;
        ExecutionEngine* engine_ = nullptr;
        QueryProcessor* query_processor_ = nullptr;
        AlgebraEngine* algebra_engine_ = nullptr;
    public:
        NileDB(size_t pool_size = 64, size_t k=32)
        {
            cache_manager_ = new CacheManager(pool_size, disk_manager_, k);
            //catalog_ = new Catalog(cache_manager_);
            catalog_.init(cache_manager_);
            parser_ = new Parser(&catalog_);
            engine_ = new ExecutionEngine(&catalog_);
            algebra_engine_ = new AlgebraEngine(&catalog_);
            query_processor_ = new QueryProcessor(parser_, engine_, algebra_engine_);
        }
        ~NileDB(){
            delete query_processor_;
            delete engine_;
            delete parser_;
            delete algebra_engine_;
            //delete catalog_;
            catalog_.destroy();
            delete cache_manager_;
            delete disk_manager_;
        }
        void flush(){
            cache_manager_->flushAllPages();
        }
        bool SQL(QueryCTX& query_ctx, Executor** execution_root){
            bool res =  query_processor_->handleQuery(query_ctx, execution_root);
            return res;
        }
        bool CMD(const char* command){
            if(strcmp(command, "\\cache") == 0){
                cache_manager_->show();
                return true;
            }
            return false;
        }
};

