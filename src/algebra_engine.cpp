#pragma once
#include "parser.cpp"
//#include "execution_engine.cpp"
class Executor;

enum AlgebraOperationType {
    // single table operations.
    SCAN,
    PROJECTION,
    FILTER, 
    SORT,
    LIMIT,
    RENAME,
    AGGREGATION,

    // two table operations.
    PRODUCT,
    JOIN,
};


struct AlgebraOperation {
    public:
        AlgebraOperation (AlgebraOperationType type) : type_(type)
        {}
        AlgebraOperationType type_;
         Executor* executor_ = nullptr;
};

struct ScanOperation: AlgebraOperation {
    public:
        ScanOperation(std::string table_name): 
            AlgebraOperation(SCAN),
            table_name_(table_name)
        {}
        ~ScanOperation()
        {}
    private:
        std::string table_name_;
        // the optimizer will pick the right algorithm based on the cost model 
        // and attach an executor to the physical plan.
        // for example : use the btree index for range filters and the hash index for equality filters etc..
        // and all of them implement the ScanExecutor 
};

struct FilterOperation: AlgebraOperation {
    public:
        FilterOperation(AlgebraOperation* child, ExpressionNode* filter): 
            AlgebraOperation(FILTER),
            child_(child), 
            filter_(filter)
        {}
        ~FilterOperation()
        {}
        ExpressionNode* filter_;
        AlgebraOperation* child_;
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

        bool isValidSelectStatementData (SelectStatementData* data){
            for(std::string& table_name : data->tables_){
                TableSchema* schema = catalog_->getTableSchema(table_name);
                if(!schema) {
                    std::cout << "[ERROR] Invalid table name " << table_name << std::endl;
                    return false;
                }
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
            ScanOperation* scan = nullptr;
            FilterOperation* filter = nullptr;
            ProjectionOperation* projection = nullptr;
            SortOperation* sort = nullptr;
            // only use the first table until we add support for the product operation.
            if(data->tables_.size())
                scan = new ScanOperation(data->tables_[0]);
            filter = new FilterOperation(scan, data->where_);
            projection = new ProjectionOperation(scan, data->fields_);
            sort = new SortOperation(projection, data->order_by_list_);
            return sort;
        }

        AlgebraOperation* createAlgebraExpression(QueryData* data){
            switch(data->type_){
                case SELECT_DATA:
                    return createSelectStatementExpression(reinterpret_cast<SelectStatementData*>(data));
            }
            return nullptr;
        }

        void bindExecutors(AlgebraOperation* operation){
            
        }
    private:
        Catalog* catalog_ = nullptr;
};
