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

        bool handleQuery(std::string query, QueryResult* result){
            std::cout << "[INFO] Parsing query" << std::endl;
            if(query.substr(0, 6) == "SELECT"){
                QueryData* data = parser_->new_parse(query);
                if(!data){
                    std::cout << "[ERROR] Invalid query data" << std::endl;
                    return false;
                }
                AlgebraOperation* logical_plan = algebra_->createAlgebraExpression(data);
                if(!logical_plan){
                    std::cout << "[ERROR] Invalid logical algebra plan" << std::endl;
                    return false;
                }
                bool status = engine_->executePlan(logical_plan, result);
                if(!status)
                    std::cout << "[ERROR] Query can't be executed" << std::endl;
                return status;
            }

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
        AlgebraEngine* algebra_;
};

