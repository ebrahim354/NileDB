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

        bool handleQuery(QueryCTX& query_ctx, String& query, Executor** execution_root){
            std::cout << "[INFO] Parsing query" << std::endl;
            //QueryCTX query_ctx;
            //query_ctx.init(query.size());
            parser_->parse(query, query_ctx);
            if(!query_ctx.queries_call_stack_.size() || query_ctx.error_status_ != Error::NO_ERROR || query_ctx.cursor_ != query_ctx.tokens_.size()){
                std::cout << (int)query_ctx.getCurrentToken().type_ << " " << query_ctx.cursor_ << " " << query_ctx.tokens_.size() << std::endl;
                for(int i = 0; i < query_ctx.tokens_.size(); i++){
                    std::cout << (int) query_ctx.tokens_[i].type_ << " ";
                }
                std::cout << "\n";
                std::cout << "[ERROR] Invalid query data, status: " << (int) query_ctx.error_status_ << std::endl;
                //query_ctx.clean();
                return false;
            }
            if(query_ctx.direct_execution_){
                // DDL commands that operate directly on the system catalog such as create table, create index etc...
                bool status =  engine_->directExecute(query_ctx);
                //query_ctx.clean();
                return status;
            }
            std::cout << "[INFO] Creating logical plan" << std::endl;
            algebra_->createAlgebraExpression(query_ctx);
            if(query_ctx.operators_call_stack_.size() < query_ctx.queries_call_stack_.size() || 
                    query_ctx.error_status_ != Error::NO_ERROR){
                std::cout << "[ERROR] Invalid logical algebra plan" << std::endl;
                //query_ctx.clean();
                return false;
            }

            bool status = engine_->executePlan(query_ctx, execution_root);
            if(!status)
                std::cout << "[ERROR] Query can't be executed" << std::endl;
            //query_ctx.clean();
            return status;
        }

    private:
        Parser* parser_;
        ExecutionEngine* engine_;
        AlgebraEngine* algebra_;
};

