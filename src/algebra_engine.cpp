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
    INSERTION,

    // two table operations.
    PRODUCT,
    JOIN,
};


struct AlgebraOperation {
    public:
        AlgebraOperation (AlgebraOperationType type, QueryCTX& ctx) : 
            type_(type), ctx_(ctx)
        {}
        QueryCTX& ctx_;
        AlgebraOperationType type_;
        int query_idx_ = -1;
        int query_parent_idx_ = -1;
        bool distinct_ = false;
};

struct ScanOperation: AlgebraOperation {
    public:
        ScanOperation(QueryCTX& ctx,std::string table_name, std::string table_rename): 
            AlgebraOperation(SCAN, ctx),
            table_name_(table_name),
            table_rename_(table_rename)
        {}
        ~ScanOperation()
        {}
        std::string table_name_{};
        std::string table_rename_{};
};

struct InsertionOperation: AlgebraOperation {
    public:
        InsertionOperation(QueryCTX& ctx): 
            AlgebraOperation(INSERTION, ctx)
        {}
        ~InsertionOperation()
        {}
};

struct FilterOperation: AlgebraOperation {
    public:
        FilterOperation(QueryCTX& ctx,AlgebraOperation* child, ExpressionNode* filter, 
                std::vector<ExpressionNode*>& fields, 
                std::vector<std::string>& field_names): 
            AlgebraOperation(FILTER, ctx),
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
        AggregationOperation(QueryCTX& ctx, AlgebraOperation* child, std::vector<AggregateFuncNode*> aggregates, std::vector<ASTNode*> group_by): 
            AlgebraOperation(AGGREGATION, ctx),
            child_(child), 
            aggregates_(aggregates),
            group_by_(group_by)
        {}
        ~AggregationOperation()
        {}
        AlgebraOperation* child_ = nullptr;
        std::vector<AggregateFuncNode*> aggregates_;
        std::vector<ASTNode*> group_by_;
};


struct ProjectionOperation: AlgebraOperation {
    public:
        ProjectionOperation(QueryCTX& ctx, AlgebraOperation* child, std::vector<ExpressionNode*> fields): 
            AlgebraOperation(PROJECTION, ctx),
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
        SortOperation(QueryCTX& ctx, AlgebraOperation* child, std::vector<int> order_by_list): 
            AlgebraOperation(SORT, ctx),
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

        
        void createAlgebraExpression(QueryCTX& ctx){
            for(auto data : ctx.queries_call_stack_){
                switch(data->type_){
                    case SELECT_DATA:
                        {
                            auto select_data = reinterpret_cast<SelectStatementData*>(data);
                            AlgebraOperation* op = createSelectStatementExpression(ctx, select_data);
                            if(!op){
                                ctx.error_status_ = Error::LOGICAL_PLAN_ERROR;
                                return;
                            }
                            op->distinct_ = select_data->distinct_;
                            ctx.operators_call_stack_.push_back(op);
                        } break;
                    case INSERT_DATA:
                        {
                            auto insert_data = reinterpret_cast<InsertStatementData*>(data);
                            AlgebraOperation* op = createInsertStatementExpression(ctx, insert_data);
                            if(!op){
                                ctx.error_status_ = Error::LOGICAL_PLAN_ERROR;
                                return;
                            }
                            ctx.operators_call_stack_.push_back(op);
                        } break;
                    default:
                        return;
                }
            }
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
        bool isValidInsertStatementData (InsertStatementData* data){
            // TODO: provide validation.
            return true;
        }


        AlgebraOperation* createInsertStatementExpression(QueryCTX& ctx, InsertStatementData* data){
            if(!isValidInsertStatementData(data))
                return nullptr;

            AlgebraOperation* result = new InsertionOperation(ctx);
            result->query_idx_ = data->idx_;
            result->query_parent_idx_ = data->parent_idx_;
            return result;
        }

        AlgebraOperation* createSelectStatementExpression(QueryCTX& ctx, SelectStatementData* data){
            if(!isValidSelectStatementData(data))
                return nullptr;

            AlgebraOperation* result = nullptr;
            // only use the first table until we add support for the product operation.
            if(data->tables_.size() && data->table_names_.size())
                result = new ScanOperation(ctx, data->tables_[0], data->table_names_[0]);
            if(data->where_)
                result = new FilterOperation(ctx, result, data->where_, data->fields_, data->field_names_);
            if(data->aggregates_.size())
                result = new AggregationOperation(ctx, result, data->aggregates_, data->group_by_);
            if(data->fields_.size())
                result = new ProjectionOperation(ctx, result, data->fields_);
            if(data->order_by_list_.size())
                result = new SortOperation(ctx, result, data->order_by_list_);
            if(result) {
                result->query_idx_ = data->idx_;
                result->query_parent_idx_ = data->parent_idx_;
            }
            return result;
        }

        Catalog* catalog_ = nullptr;
};
