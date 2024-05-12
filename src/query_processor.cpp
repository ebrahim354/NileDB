#include "execution_engine.cpp"


class QueryProcessor{
    public:
        QueryProcessor(Parser* parser, ExecutionEngine* engine): 
            parser_(parser),
            engine_(engine){}
        ~QueryProcessor(){}

        bool handleQuery(std::string query){
            auto tree = parser_->parse(query);
            if(tree == nullptr) std::cout << "INVALID AST" << std::endl;
            return engine_->execute(tree);
        }

    private:
        Parser* parser_;
        ExecutionEngine* engine_;
};

