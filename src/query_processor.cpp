#include "execution_engine.cpp"


class QueryProcessor{
    public:
        QueryProcessor(Parser* parser, ExecutionEngine* engine): 
            parser_(parser),
            engine_(engine){}
        ~QueryProcessor(){}

        bool handleQuery(std::string query, QueryResult* result){
            std::cout << "[INFO] Parsing query" << std::endl;
            auto tree = parser_->parse(query);
            if(tree == nullptr) {
                std::cout << "[ERROR] Invalid AST" << std::endl;
                return false;
            }
            std::cout << "[INFO] Executing query" << std::endl;
            bool status = engine_->execute(tree, result);
            if(!status)
                std::cout << "[ERROR] Query can't be executed" << std::endl;
            return status;
        }

    private:
        Parser* parser_;
        ExecutionEngine* engine_;
};

