#pragma once
#include "parser.cpp"
#include "algebra_operation.cpp"
#include <vector>



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

struct UnionOperation: AlgebraOperation {
    public:
        UnionOperation(QueryCTX& ctx, AlgebraOperation* lhs, AlgebraOperation* rhs, bool all): 
            AlgebraOperation(AL_UNION, ctx), lhs_(lhs), rhs_(rhs), all_(all)
        {}
        ~UnionOperation()
        {
            delete lhs_;
            delete rhs_;
        }

        AlgebraOperation* lhs_ = nullptr;
        AlgebraOperation* rhs_ = nullptr;
        bool all_ = false;
};

struct ExceptOperation: AlgebraOperation {
    public:
        ExceptOperation(QueryCTX& ctx, AlgebraOperation* lhs, AlgebraOperation* rhs, bool all): 
            AlgebraOperation(AL_EXCEPT, ctx), lhs_(lhs), rhs_(rhs), all_(all)
        {}
        ~ExceptOperation()
        {
            delete lhs_;
            delete rhs_;
        }

        AlgebraOperation* lhs_ = nullptr;
        AlgebraOperation* rhs_ = nullptr;
        bool all_ = false;
};

struct IntersectOperation: AlgebraOperation {
    public:
        IntersectOperation(QueryCTX& ctx, AlgebraOperation* lhs, AlgebraOperation* rhs, bool all): 
            AlgebraOperation(AL_INTERSECT, ctx), lhs_(lhs), rhs_(rhs), all_(all)
        {}
        ~IntersectOperation()
        {
            delete lhs_;
            delete rhs_;
        }

        AlgebraOperation* lhs_ = nullptr;
        AlgebraOperation* rhs_ = nullptr;
        bool all_ = false;
};

struct ProductOperation: AlgebraOperation {
    public:
        ProductOperation(QueryCTX& ctx,AlgebraOperation* lhs, AlgebraOperation* rhs): 
            AlgebraOperation(PRODUCT, ctx), lhs_(lhs), rhs_(rhs)
        {}
        ~ProductOperation()
        {
            delete lhs_;
            delete rhs_;
        }

        AlgebraOperation* lhs_ = nullptr;
        AlgebraOperation* rhs_ = nullptr;
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
        {
            delete child_;
        }
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
        {
            delete child_;
        }
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
        {
            delete child_;
        }
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
        {
            delete child_;
        }
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

            for(auto data : ctx.set_operations_){
                AlgebraOperation* set_operation = createSetOperationExpression(ctx, data);
                if(!set_operation){
                    ctx.error_status_ = Error::LOGICAL_PLAN_ERROR;
                    return;
                }
                ctx.operators_call_stack_.push_back(set_operation);
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

        AlgebraOperation* createSetOperationExpression(QueryCTX& ctx, QueryData* data, bool once = false){
            switch(data->type_){
                    case UNION:
                    case EXCEPT:
                        {
                            auto ex_or_un = reinterpret_cast<UnionOrExcept*>(data);
                            AlgebraOperation* lhs = createSetOperationExpression(ctx, ex_or_un->cur_);
                            if(once) return lhs;
                            bool all = ex_or_un->all_;
                            auto op = ex_or_un->type_;
                            auto ptr = ex_or_un->next_;
                            while(ptr){
                                AlgebraOperation* rhs = createSetOperationExpression(
                                        ctx, ptr,
                                        (ptr->type_ == UNION || ptr->type_ == EXCEPT)
                                    );
                                if(op == EXCEPT){
                                    lhs = new ExceptOperation(ctx, lhs, rhs, all);
                                } else if(op == UNION){
                                    lhs = new UnionOperation(ctx, lhs, rhs, all);
                                }
                                if(ptr->type_ == EXCEPT || ptr->type_ == UNION){
                                    auto tmp = reinterpret_cast<UnionOrExcept*>(ptr);
                                    op = ptr->type_;
                                    all = tmp->all_;
                                    ptr = tmp->next_;
                                } else break;
                            }
                            return lhs;
                        }
                    case INTERSECT:
                        {
                            auto intersect = reinterpret_cast<Intersect*>(data);
                            AlgebraOperation* lhs = createSetOperationExpression(ctx, intersect->cur_);
                            //AlgebraOperation* lhs = ctx.operators_call_stack_[intersect->cur_];
                            if(once) return lhs;
                            bool all = intersect->all_;
                            auto ptr = intersect->next_;
                            while(ptr){
                                //auto nxt = reinterpret_cast<Intersect*>(ptr);
                                AlgebraOperation* rhs = createSetOperationExpression(
                                        ctx, ptr,
                                        (ptr->type_ == INTERSECT)
                                    );
                                lhs = new IntersectOperation(ctx, lhs, rhs, all);

                                if(ptr->type_ == INTERSECT){
                                    auto tmp = reinterpret_cast<Intersect*>(ptr);
                                    all = tmp->all_;
                                    ptr = tmp->next_;
                                } else break;
                            }
                            return lhs;
                        }
                    case SELECT_DATA:
                        return createSelectStatementExpression(ctx, reinterpret_cast<SelectStatementData*>(data));
                    default:
                        return nullptr;
                
            }
        }

        AlgebraOperation* createSelectStatementExpression(QueryCTX& ctx, SelectStatementData* data){
            if(!isValidSelectStatementData(data))
                return nullptr;
            std::vector<ExpressionNode*> splitted_where;
            if(data->where_){
              // split conjunctive predicates.
              splitted_where = split_by_and(data->where_);
            }
            std::vector<std::pair<std::vector<std::string>, ExpressionNode*>> tables_per_filter;

            for(int i = 0; i < splitted_where.size(); ++i){
              std::vector<std::string> table_access;
              std::unordered_map<std::string, bool> f;
              accessed_tables(splitted_where[i], table_access, catalog_);
              std::vector<std::string> ta;
              for(auto &s: table_access){
                if(!f.count(s)){
                  ta.push_back(s);
                  f[s] = 1;
                }
              }
              tables_per_filter.push_back({ta, splitted_where[i]});
            }
            sort(
              tables_per_filter.begin(), tables_per_filter.end(),
              [](std::pair<std::vector<std::string>, ExpressionNode*> lhs,
                 std::pair<std::vector<std::string>, ExpressionNode*> rhs){ 
                return lhs.first.size() < rhs.first.size();
              });
            for(auto x : tables_per_filter){
              std::cout << x.first.size() << std::endl;
            }
            std::unordered_map<std::string, AlgebraOperation*> table_scanner;
            for(int i = 0; i < data->tables_.size(); ++i){
              AlgebraOperation* scan = new ScanOperation(ctx, data->tables_[i], data->table_names_[i]);
              // TODO: fix the case of products of the same table for example: select * from t1, t1
              table_scanner[data->tables_[i]] = scan;
            }

            for(int i = 0; i < splitted_where.size(); ++i){
              if(tables_per_filter[i].first.size() == 0) continue;
              
              for(int j = 1; j < tables_per_filter[i].first.size(); ++j){
                auto left = table_scanner[tables_per_filter[i].first[j-1]];
                auto right = table_scanner[tables_per_filter[i].first[j]];
                auto nw =  new ProductOperation(ctx, table_scanner[tables_per_filter[i].first[j-1]], table_scanner[tables_per_filter[i].first[j]]); 
                for(auto &t:table_scanner){
                  if(t.second == left || t.second == right){
                    table_scanner[t.first] = nw;
                  }
                }
              }

              auto new_filter = new FilterOperation(ctx, 
                  table_scanner[tables_per_filter[i].first[0]], 
                  tables_per_filter[i].second, 
                  data->fields_, data->field_names_);

              for(int j = 0; j < tables_per_filter[i].first.size(); ++j){
                  table_scanner[tables_per_filter[i].first[j]] = new_filter;
              }
            }

            AlgebraOperation* result = nullptr;

            std::unordered_map<AlgebraOperation*, bool> freq;
            for(auto &t:table_scanner){
              if(result == nullptr){
                result = table_scanner[t.first];
                freq[result] = 1;
              } else if(!freq.count(t.second)){
                result = new ProductOperation(ctx, result, t.second); 
                freq[t.second] = 1;
              }
            }

            /*
            int idx = 0;
            while(idx < data->tables_.size()){
                AlgebraOperation* scan = new ScanOperation(ctx, data->tables_[idx], data->table_names_[idx]);
                std::map<int, bool> deleted;
                for(int i = 0; i < splitted_where.size(); ++i){
                  std::vector<std::string> table_access;
                  accessed_tables(splitted_where[i], table_access, catalog_);
                  if(table_access.size() == 1 && table_access[0] == data->tables_[idx]){
                    scan = new FilterOperation(ctx, scan, splitted_where[i], data->fields_, data->field_names_);
                    deleted[i] = true;
                  }
                }
                std::vector<ExpressionNode*> new_splitted_where;
                for(int i = 0; i < splitted_where.size(); ++i){
                  if(!deleted[i]) new_splitted_where.push_back(splitted_where[i]);
                }
                splitted_where = new_splitted_where;
                if(!result) result = scan;
                else result = new ProductOperation(ctx, result, scan); 
                idx++;
            }

            
            if(splitted_where.size() > 0){
              for(int i = 0; i < splitted_where.size(); ++i){
                result = new FilterOperation(ctx, result, splitted_where[i], data->fields_, data->field_names_);
              }
            }
            */


            if(data->aggregates_.size() || data->group_by_.size()){
                result = new AggregationOperation(ctx, result, data->aggregates_, data->group_by_);
                // TODO: make a specific having operator and executor.
                if(data->having_)
                  result = new FilterOperation(ctx, result, data->having_, data->fields_, data->field_names_);
            }
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
