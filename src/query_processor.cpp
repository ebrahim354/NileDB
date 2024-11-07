#include "algebra_engine.cpp"
#include "execution_engine.cpp"

class QueryProcessor{
    public:
        QueryProcessor(Parser* parser, ExecutionEngine* engine, AlgebraEngine* algebra): 
            parser_(parser),
            engine_(engine),
            algebra_(algebra)
    {}
        ~QueryProcessor(){}

        bool handleQuery(std::string& query, QueryResult* result){
            std::cout << "[INFO] Parsing query" << std::endl;
            QueryCTX query_ctx(query.size());
            parser_->parse(query, query_ctx);
            if(!query_ctx.queries_call_stack_.size() || query_ctx.error_status_ != Error::NO_ERROR){
                std::cout << "[ERROR] Invalid query data" << std::endl;
                return false;
            }
            if(query_ctx.direct_execution_){
                // DDL commands that operate directly on the system catalog such as create table, create index etc...
                return engine_->directExecute(query_ctx);
            }
            std::cout << "[INFO] Creating logical plan" << std::endl;
            algebra_->createAlgebraExpression(query_ctx);
            if(query_ctx.operators_call_stack_.size() != query_ctx.queries_call_stack_.size() || 
                    query_ctx.error_status_ != Error::NO_ERROR){
                std::cout << "[ERROR] Invalid logical algebra plan" << std::endl;
                return false;
            }

            bool status = engine_->executePlan(query_ctx, result);
            if(!status)
                std::cout << "[ERROR] Query can't be executed" << std::endl;
            return status;
        }

    private:
        Parser* parser_;
        ExecutionEngine* engine_;
        AlgebraEngine* algebra_;
};

