#pragma once
#include "parser.cpp"
#include "algebra_operation.cpp"
#include <queue>
#include <vector>
#include <set>



struct ScanOperation: AlgebraOperation {
    public:
        ScanOperation(QueryCTX& ctx,std::string table_name, std::string table_rename): 
            AlgebraOperation(SCAN, ctx),
            table_name_(table_name),
            table_rename_(table_rename)
        {}
        ~ScanOperation()
        {}
        void print(int prefix_space_cnt) {
            for(int i = 0; i < prefix_space_cnt; ++i)
                std::cout << " ";
          std::cout << "Scan operation, name: " << table_name_ << " rename: " << table_rename_ << std::endl;
        }
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
        void print(int prefix_space_cnt) {
            for(int i = 0; i < prefix_space_cnt; ++i)
                std::cout << " ";
          std::cout << "union operation\n"; 
          std::cout << " lhs:\n "; 
          lhs_->print(prefix_space_cnt + 1);
          std::cout << " rhs:\n "; 
          rhs_->print(prefix_space_cnt + 1);
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
        void print(int prefix_space_cnt) {
            for(int i = 0; i < prefix_space_cnt; ++i)
                std::cout << " ";
          std::cout << "except operation\n"; 
          lhs_->print(prefix_space_cnt + 1);
          rhs_->print(prefix_space_cnt + 1);
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
        void print(int prefix_space_cnt) {
            for(int i = 0; i < prefix_space_cnt; ++i)
                std::cout << " ";
          std::cout << "intersect operation\n"; 
          lhs_->print(prefix_space_cnt + 1);
          rhs_->print(prefix_space_cnt + 1);
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
        void print(int prefix_space_cnt) {
            for(int i = 0; i < prefix_space_cnt; ++i)
                std::cout << " ";
          std::cout << "product operation\n"; 
          lhs_->print(prefix_space_cnt + 1);
          rhs_->print(prefix_space_cnt + 1);
        }

        AlgebraOperation* lhs_ = nullptr;
        AlgebraOperation* rhs_ = nullptr;
};

struct JoinOperation: AlgebraOperation {
    public:
        JoinOperation(QueryCTX& ctx, AlgebraOperation* lhs, AlgebraOperation* rhs, 
            ExpressionNode* filter): 
            AlgebraOperation(JOIN, ctx), lhs_(lhs), rhs_(rhs), filter_(filter)
        {}
        ~JoinOperation()
        {
            delete lhs_;
            delete rhs_;
        }
        void print(int prefix_space_cnt) {
            for(int i = 0; i < prefix_space_cnt; ++i)
                std::cout << " ";
          std::cout << "join operation\n"; 
          lhs_->print(prefix_space_cnt + 1);
          rhs_->print(prefix_space_cnt + 1);
        }

        AlgebraOperation* lhs_ = nullptr;
        AlgebraOperation* rhs_ = nullptr;
        ExpressionNode* filter_;
};

struct InsertionOperation: AlgebraOperation {
    public:
        InsertionOperation(QueryCTX& ctx): 
            AlgebraOperation(INSERTION, ctx)
        {}
        void print(int prefix_space_cnt) {
            for(int i = 0; i < prefix_space_cnt; ++i)
                std::cout << " ";
          std::cout << "insertion operation\n"; 
        }
        ~InsertionOperation()
        {}
};

struct FilterOperation: AlgebraOperation {
    public:
        Catalog* catalog_ = nullptr;
        FilterOperation(QueryCTX& ctx,AlgebraOperation* child, ExpressionNode* filter, 
                std::vector<ExpressionNode*>& fields, 
                std::vector<std::string>& field_names,
                Catalog* catalog
                ): 
            AlgebraOperation(FILTER, ctx),
            child_(child), 
            filter_(filter),
            fields_(fields),
            field_names_(field_names),
            catalog_(catalog)
        {}
        ~FilterOperation()
        {
            delete child_;
        }

        void print(int prefix_space_cnt) {
            std::vector<std::string> table_access;
            accessed_tables(filter_, table_access, catalog_);
            for(int i = 0; i < prefix_space_cnt; ++i)
                std::cout << " ";
            std::cout << "filter operation "; 
            for(int i = 0; i < table_access.size(); ++i) std::cout << table_access[i] << " ";
            std::cout << "\n";
            child_->print(prefix_space_cnt + 1);
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
        void print(int prefix_space_cnt) {
            for(int i = 0; i < prefix_space_cnt; ++i)
                std::cout << " ";
          std::cout << "agg operation\n"; 
          child_->print(prefix_space_cnt + 1);
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
        void print(int prefix_space_cnt) {
            for(int i = 0; i < prefix_space_cnt; ++i)
                std::cout << " ";
          std::cout << "projection operation\n"; 
          child_->print(prefix_space_cnt + 1);
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
        void print(int prefix_space_cnt) {
            for(int i = 0; i < prefix_space_cnt; ++i)
                std::cout << " ";
          std::cout << "sort operation\n"; 
          child_->print(prefix_space_cnt + 1);
        }
        AlgebraOperation* child_ = nullptr;
        std::vector<int> order_by_list_;

};

class AlgebraEngine {
    public:
        AlgebraEngine(Catalog* catalog): catalog_(catalog)
        {}
        ~AlgebraEngine(){}

        std::vector<int> find_filters_that_access_table(std::string table, 
                std::vector<std::pair<std::vector<std::string>, ExpressionNode*>> tables_per_filter) {
            std::vector<int> ans;
            for(int i = 0; i < tables_per_filter.size(); ++i){
                for(int j = 0; j < tables_per_filter[i].first.size(); ++j){
                    std::string cur = tables_per_filter[i].first[j];
                    if(cur == table) {
                        ans.push_back(i);
                        continue;
                    }
                }
            }
            return ans;
        }


        // BFS on filters to sort tables as close as possible.
        // soring tables as close as possible based on filters is importent in the predicate pushdown step,
        // this will insure that filters are as close to the leafs as pussible.
        // this implementation is cluncky but works.
        // TODO: clean up this implementation. 
        void group_close_tables(std::vector<std::pair<std::vector<std::string>, ExpressionNode*>>& tables_per_filter) {
            std::queue<std::string> q;
            std::set<int> visited_filter;
            std::set<std::string> visited_table;
            std::vector<int> sorted_order;
            for(int f = 0; f < tables_per_filter.size(); ++f){
                if(tables_per_filter[f].first.size() == 0) { // no accessed_tables comes first.
                    sorted_order.push_back(f);
                    continue;
                };
                q.push(tables_per_filter[f].first[0]);
                visited_table.insert(q.front());
                while(!q.empty()) {
                    int n = q.size();
                    std::string frnt = q.front(); q.pop();
                    for(int i = 0; i < n; ++i){
                        std::vector<int> ans = find_filters_that_access_table(frnt, tables_per_filter);
                        for(int k = 0; k < ans.size(); ++k){
                            if(visited_filter.count(ans[k])) continue;
                            visited_filter.insert(ans[k]);
                            sorted_order.push_back(ans[k]);
                            for(int j = 0; j < tables_per_filter[ans[k]].first.size(); ++j){
                                std::string cur = tables_per_filter[ans[k]].first[j];
                                if(visited_table.count(cur)) continue;
                                visited_table.insert(cur);
                                q.push(cur);
                            }
                        }

                    }
                }
            }
            std::vector<std::pair<std::vector<std::string>, ExpressionNode*>> tables_per_filter_sorted;
            for(int i = 0; i < sorted_order.size(); ++i){
                int idx = sorted_order[i];
                tables_per_filter_sorted.push_back(tables_per_filter[idx]);
            }
            assert(tables_per_filter.size() == tables_per_filter_sorted.size());
            for(int i = 0; i < sorted_order.size(); ++i){
                tables_per_filter[i] = tables_per_filter_sorted[i];
            }
        }

        
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
            std::unordered_map<std::string, int> mentioned_tables;
            for(int i = 0; i < data->table_names_.size(); ++i){
                if(mentioned_tables.count(data->table_names_[i])) {
                    std::cout << "[ERROR] table name \"" << data->table_names_[i] << "\" specified more than once." 
                        << std::endl;
                    return false;

                }
                mentioned_tables[data->table_names_[i]] = i;
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


        void replaceFilteredProductWithJoin(AlgebraOperation** root){
          if(!root || !(*root)) return;
          switch((*root)->type_) {
            case SORT: 
              {
                auto op = reinterpret_cast<SortOperation*>(*root);
                if(op->child_)
                  replaceFilteredProductWithJoin(&op->child_);
              } break;
            case PROJECTION: 
              {
                auto op = reinterpret_cast<ProjectionOperation*>(*root);
                if(op->child_)
                  replaceFilteredProductWithJoin(&op->child_);

              } break;
            case AGGREGATION: 
              {
                auto op = reinterpret_cast<AggregationOperation*>(*root);
                if(op->child_)
                  replaceFilteredProductWithJoin(&op->child_);
              } break;
            case FILTER: 
              {
                auto op = reinterpret_cast<FilterOperation*>(*root);
                if(op->child_ && op->child_->type_ == PRODUCT){
                  auto product = reinterpret_cast<ProductOperation*>(op->child_);
                  auto join_node = new JoinOperation(product->ctx_, product->lhs_, product->rhs_, op->filter_);
                  // TODO: fix memory leak of the filter and the product nodes.
                  (*root) = join_node;
                  replaceFilteredProductWithJoin(root);
                }  else if(op->child_){
                  replaceFilteredProductWithJoin(&op->child_);
                }
              } break;
            case JOIN: 
            case PRODUCT: 
              {
                auto op = reinterpret_cast<ProductOperation*>(*root);
                replaceFilteredProductWithJoin(&op->lhs_);
                replaceFilteredProductWithJoin(&op->rhs_);
              } break;
            case AL_UNION: 
            case AL_EXCEPT: 
            case AL_INTERSECT: 
              {
                if((*root)->type_ == AL_UNION){
                  auto op = reinterpret_cast<UnionOperation*>(*root);
                  replaceFilteredProductWithJoin(&op->lhs_);
                  replaceFilteredProductWithJoin(&op->rhs_);
                } else if((*root)->type_ == AL_EXCEPT) {
                  auto op = reinterpret_cast<ExceptOperation*>(*root);
                  replaceFilteredProductWithJoin(&op->lhs_);
                  replaceFilteredProductWithJoin(&op->rhs_);
                } else {
                  auto op = reinterpret_cast<IntersectOperation*>(*root);
                  replaceFilteredProductWithJoin(&op->lhs_);
                  replaceFilteredProductWithJoin(&op->rhs_);
                }
              } break;
            case SCAN: 
            case INSERTION: 
            default: 
              return;
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
            // collect data about which tables did we access for each splitted predicate from the previous step.
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

            // sort predicates by the least accessed number of tables.
            sort(tables_per_filter.begin(), tables_per_filter.end(),
                    [](std::pair<std::vector<std::string>, ExpressionNode*> lhs,
                        std::pair<std::vector<std::string>, ExpressionNode*> rhs) {
                        return lhs.first.size() < rhs.first.size();
                    });



            group_close_tables(tables_per_filter);
            /*
            for(int i = 0; i < order.size(); ++i)
                std::cout << order[i] << " ";
            std::cout << "\n";*/


            // this is the "predicate push down" step but we are building the tree from the ground up 
            // with predicates being as low as possible.
            //
            // initialize 1 scanner for each accessed table.
            std::unordered_map<std::string, AlgebraOperation*> table_scanner;
            for(int i = 0; i < data->tables_.size(); ++i){
              AlgebraOperation* scan = new ScanOperation(ctx, data->tables_[i], data->table_names_[i]);
              table_scanner[data->table_names_[i]] = scan;
            }

            AlgebraOperation* result = nullptr;
            for(int i = 0; i < splitted_where.size(); ++i){
              if(tables_per_filter[i].first.size() == 0) continue; // 0 tables skip
              if(tables_per_filter[i].first.size() == 1) { // 1 table access,  wrap it in its filter.
                table_scanner[tables_per_filter[i].first[0]] = new FilterOperation(ctx, 
                    table_scanner[tables_per_filter[i].first[0]], 
                    tables_per_filter[i].second, 
                    data->fields_, data->field_names_, catalog_);
                continue;
              } 
              
              // loop over all tables that was accessed with in filter number 'i'
              for(int j = 0; j < tables_per_filter[i].first.size(); ++j) {
                  std::string t = tables_per_filter[i].first[j];
                if(result == nullptr) {
                  result = table_scanner[t];
                } else if(table_scanner.count(t)){
                  result =  new ProductOperation(ctx, result, table_scanner[t]); 
                } else {
                  continue;
                }
                table_scanner.erase(t);
              }

              result = new FilterOperation(ctx, 
                  result, 
                  tables_per_filter[i].second, 
                  data->fields_, data->field_names_, catalog_);
            }
              replaceFilteredProductWithJoin(&result);


            // remaining table outside of filters.
            for(auto &t:table_scanner) {
              if(result == nullptr)
                result = table_scanner[t.first];
              else
                result =  new ProductOperation(ctx, result, table_scanner[t.first]); 
            }
            // handle filters with zero table access.
            for(int i = 0; i < splitted_where.size(); ++i){
              if(tables_per_filter[i].first.size() == 0){
                result = new FilterOperation(ctx, 
                    result, 
                    tables_per_filter[i].second, 
                    data->fields_, data->field_names_, catalog_);
              }
            }



            //result->print(0);

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
                  result = new FilterOperation(ctx, result, data->having_, data->fields_, data->field_names_, catalog_);
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
