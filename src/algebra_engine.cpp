#pragma once
#include "parser.cpp"
#include <vector>


enum AlgebraOperationType {
    // single table operations.
    SCAN,
    FILTER, 
    AGGREGATION,
    PROJECTION,
    SORT,
    LIMIT,
    RENAME,

    // two table operations.
    PRODUCT,
    JOIN,
};


struct AlgebraOperation {
    public:
        AlgebraOperation (AlgebraOperationType type, std::vector<AlgebraOperation*>* call_stack) : 
            type_(type), call_stack_(call_stack)
        {}
        AlgebraOperationType type_;
        std::vector<AlgebraOperation*>* call_stack_{};
        int query_idx_ = -1;
        int query_parent_idx_ = -1;
};

struct ScanOperation: AlgebraOperation {
    public:
        ScanOperation(std::vector<AlgebraOperation*>* call_stack,std::string table_name, std::string table_rename): 
            AlgebraOperation(SCAN, call_stack),
            table_name_(table_name),
            table_rename_(table_rename)
        {}
        ~ScanOperation()
        {}
        std::string table_name_{};
        std::string table_rename_{};
};

struct FilterOperation: AlgebraOperation {
    public:
        FilterOperation(std::vector<AlgebraOperation*>* call_stack,AlgebraOperation* child, ExpressionNode* filter, 
                std::vector<ExpressionNode*>& fields, 
                std::vector<std::string>& field_names): 
            AlgebraOperation(FILTER, call_stack),
            child_(child), 
            filter_(filter),
            fields_(fields),
            field_names_(field_names)
        {}
        ~FilterOperation()
        {}
        ExpressionNode* filter_;
        std::vector<ExpressionNode*> fields_;
        std::vector<std::string> field_names_;
        AlgebraOperation* child_;
};

struct AggregationOperation: AlgebraOperation {
    public:
        AggregationOperation(std::vector<AlgebraOperation*>* call_stack, AlgebraOperation* child, std::vector<AggregateFuncNode*> aggregates): 
            AlgebraOperation(AGGREGATION, call_stack),
            child_(child), 
            aggregates_(aggregates)
        {}
        ~AggregationOperation()
        {}
        AlgebraOperation* child_ = nullptr;
        std::vector<AggregateFuncNode*> aggregates_;
};


struct ProjectionOperation: AlgebraOperation {
    public:
        ProjectionOperation(std::vector<AlgebraOperation*>* call_stack, AlgebraOperation* child, std::vector<ExpressionNode*> fields): 
            AlgebraOperation(PROJECTION, call_stack),
            child_(child), 
            fields_(fields)
        {}
        ~ProjectionOperation()
        {}
        AlgebraOperation* child_ = nullptr;
        std::vector<ExpressionNode*> fields_;
};

struct SortOperation: AlgebraOperation {
    public:
        SortOperation(std::vector<AlgebraOperation*>* call_stack, AlgebraOperation* child, std::vector<int> order_by_list): 
            AlgebraOperation(SORT, call_stack),
            child_(child), 
            order_by_list_(order_by_list)
        {}
        ~SortOperation()
        {}
        AlgebraOperation* child_ = nullptr;
        std::vector<int> order_by_list_;

};

class AlgebraEngine {
    public:
        AlgebraEngine(Catalog* catalog): catalog_(catalog)
        {}
        ~AlgebraEngine(){}

        
        AlgebraOperation* createAlgebraExpression(QueryData* data){

            switch(data->type_){

                case SELECT_DATA:
                    {
                        std::vector<AlgebraOperation*>* call_stack = new std::vector<AlgebraOperation*>();
                        auto select_data = reinterpret_cast<SelectStatementData*>(data);
                        for(size_t i = 0; i < select_data->queries_call_stack_.size(); i++){
                            auto current_data = reinterpret_cast<SelectStatementData*>(select_data->queries_call_stack_[i]);
                            AlgebraOperation* op = createSelectStatementExpression(current_data, call_stack);
                            call_stack->push_back(op);
                        }

                        if(call_stack->size() < 1) return nullptr;
                        return (*call_stack)[0]; 
                    }
            }
            return nullptr;
        }
    private:

        bool isValidSelectStatementData (SelectStatementData* data){
            for(std::string& table_name : data->tables_){
                TableSchema* schema = catalog_->getTableSchema(table_name);
                if(!schema) {
                    std::cout << "[ERROR] Invalid table name " << table_name << std::endl;
                    return false;
                }
            }
            if(data->has_star_ &&  data->tables_.size() == 0) {
                std::cout << "[ERROR] no table spicified for SELECT *";
                return false;
            }
            for(int order_by : data->order_by_list_){
                if(order_by >= data->fields_.size()) {
                        std::cout << "[ERROR] order by list should be between 1 and " <<  data->fields_.size() << std::endl;
                        return false;
                }
            }
            // TODO: provide validation for fields and filters.
            return  true;
        }

        AlgebraOperation* createSelectStatementExpression(SelectStatementData* data, std::vector<AlgebraOperation*>* call_stack){
            if(!isValidSelectStatementData(data))
                return nullptr;

            AlgebraOperation* result = nullptr;
            // only use the first table until we add support for the product operation.
            if(data->tables_.size())
                result = new ScanOperation(call_stack, data->tables_[0], data->table_names_[0]);
            if(data->where_)
                result = new FilterOperation(call_stack, result, data->where_, data->fields_, data->field_names_);
            if(data->aggregates_.size())
                result = new AggregationOperation(call_stack, result, data->aggregates_);
            if(data->fields_.size())
                result = new ProjectionOperation(call_stack, result, data->fields_);
            if(data->order_by_list_.size())
                result = new SortOperation(call_stack, result, data->order_by_list_);
            if(result) {
                result->query_idx_ = data->idx_;
                result->query_parent_idx_ = data->parent_idx_;
            }
            return result;
        }

        Catalog* catalog_ = nullptr;
};
