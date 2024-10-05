#pragma once
#include "parser.cpp"


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
        AlgebraOperation (AlgebraOperationType type) : type_(type)
        {}
        AlgebraOperationType type_;
};

struct ScanOperation: AlgebraOperation {
    public:
        ScanOperation(std::string table_name): 
            AlgebraOperation(SCAN),
            table_name_(table_name)
        {}
        ~ScanOperation()
        {}
        std::string table_name_;
};

struct FilterOperation: AlgebraOperation {
    public:
        FilterOperation(AlgebraOperation* child, ExpressionNode* filter, 
                std::vector<ExpressionNode*>& fields, 
                std::vector<std::string>& field_names): 
            AlgebraOperation(FILTER),
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
        AggregationOperation(AlgebraOperation* child, std::vector<AggregateFuncNode*> aggregates): 
            AlgebraOperation(AGGREGATION),
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
        ProjectionOperation(AlgebraOperation* child, std::vector<ExpressionNode*> fields): 
            AlgebraOperation(PROJECTION),
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
        SortOperation(AlgebraOperation* child, std::vector<int> order_by_list): 
            AlgebraOperation(SORT),
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
                    return createSelectStatementExpression(reinterpret_cast<SelectStatementData*>(data));
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

        AlgebraOperation* createSelectStatementExpression(SelectStatementData* data){
            if(!isValidSelectStatementData(data))
                return nullptr;
            AlgebraOperation* result = nullptr;
            // only use the first table until we add support for the product operation.
            if(data->tables_.size())
                result = new ScanOperation(data->tables_[0]);
            if(data->where_)
                result = new FilterOperation(result, data->where_, data->fields_, data->field_names_);
            if(data->aggregates_.size())
                result = new AggregationOperation(result, data->aggregates_);
            if(data->fields_.size())
                result = new ProjectionOperation(result, data->fields_);
            if(data->order_by_list_.size())
                result = new SortOperation(result, data->order_by_list_);
            return result;
        }

        Catalog* catalog_ = nullptr;
};
