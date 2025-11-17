#pragma once
#include "parser.cpp"
#include "algebra_operation.cpp"
#include <queue>
#include <vector>
#include <set>

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
        // this will insure that filters are as close to the leafs as possible.
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

            //AlgebraOperation* result = new InsertionOperation();
            AlgebraOperation* result = nullptr; 
            ALLOCATE_INIT(ctx.arena_, result, InsertionOperation);
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
                                    //lhs = new ExceptOperation(lhs, rhs, all);
                                    ExceptOperation* tmp = nullptr;
                                    ALLOCATE_INIT(ctx.arena_, tmp, ExceptOperation, lhs, rhs, all);
                                    lhs = tmp;
                                } else if(op == UNION){
                                    //lhs = new UnionOperation(lhs, rhs, all);
                                    UnionOperation* tmp = nullptr;
                                    ALLOCATE_INIT(ctx.arena_, tmp, UnionOperation, lhs, rhs, all);
                                    lhs = tmp;
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
                                //lhs = new IntersectOperation(lhs, rhs, all);
                                IntersectOperation* tmp = nullptr; 
                                ALLOCATE_INIT(ctx.arena_, tmp, IntersectOperation, lhs, rhs, all);
                                lhs = tmp;

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


        void replaceFilteredProductWithJoin(QueryCTX& ctx, AlgebraOperation** root){
          if(!root || !(*root)) return;
          switch((*root)->type_) {
            case SORT: 
              {
                auto op = reinterpret_cast<SortOperation*>(*root);
                if(op->child_)
                  replaceFilteredProductWithJoin(ctx, &op->child_);
              } break;
            case PROJECTION: 
              {
                auto op = reinterpret_cast<ProjectionOperation*>(*root);
                if(op->child_)
                  replaceFilteredProductWithJoin(ctx, &op->child_);

              } break;
            case AGGREGATION: 
              {
                auto op = reinterpret_cast<AggregationOperation*>(*root);
                if(op->child_)
                  replaceFilteredProductWithJoin(ctx, &op->child_);
              } break;
            case FILTER: 
              {
                auto op = reinterpret_cast<FilterOperation*>(*root);
                if(op->child_ && op->child_->type_ == PRODUCT){
                  auto product = reinterpret_cast<ProductOperation*>(op->child_);

                  JoinAlgorithm join_algorithm = NESTED_LOOP_JOIN;
                  if(is_hashable_condition(op->filter_)) 
                      join_algorithm = HASH_JOIN;

                  JoinOperation* join_node = nullptr;
                  ALLOCATE_INIT(ctx.arena_, join_node, JoinOperation, 
                          product->lhs_, 
                          product->rhs_, 
                          op->filter_,
                          INNER_JOIN, join_algorithm);
                  /*
                  auto join_node = new JoinOperation(
                          product->lhs_,
                          product->rhs_,
                          op->filter_,
                          INNER_JOIN, join_algorithm);*/

                  // TODO: fix memory leak of the filter and the product nodes.
                  //op->child_ = join_node;
                  *root = join_node;
                  replaceFilteredProductWithJoin(ctx, root);
                }  else if(op->child_){
                  replaceFilteredProductWithJoin(ctx, &op->child_);
                }
              } break;
            case JOIN: 
            case PRODUCT: 
              {
                auto op = reinterpret_cast<ProductOperation*>(*root);
                replaceFilteredProductWithJoin(ctx, &op->lhs_);
                replaceFilteredProductWithJoin(ctx, &op->rhs_);
              } break;
            case AL_UNION: 
            case AL_EXCEPT: 
            case AL_INTERSECT: 
              {
                if((*root)->type_ == AL_UNION){
                  auto op = reinterpret_cast<UnionOperation*>(*root);
                  replaceFilteredProductWithJoin(ctx, &op->lhs_);
                  replaceFilteredProductWithJoin(ctx, &op->rhs_);
                } else if((*root)->type_ == AL_EXCEPT) {
                  auto op = reinterpret_cast<ExceptOperation*>(*root);
                  replaceFilteredProductWithJoin(ctx, &op->lhs_);
                  replaceFilteredProductWithJoin(ctx, &op->rhs_);
                } else {
                  auto op = reinterpret_cast<IntersectOperation*>(*root);
                  replaceFilteredProductWithJoin(ctx, &op->lhs_);
                  replaceFilteredProductWithJoin(ctx, &op->rhs_);
                }
              } break;
            case SCAN: 
            case INSERTION: 
            default: 
              return;
          }
        }

        // hashable only if(for now) elements from each table are on one side 
        bool is_hashable_condition(ASTNode* ex) {
            while(ex){
                CategoryType cat = ex->category_;
                switch(cat) {
                    case EXPRESSION:{
                                        ex = ((ExpressionNode*)ex)->cur_;
                                    }
                                    continue;
                    case AND:{
                                auto ptr = ((AndNode*)ex);
                                if(ptr->next_ == nullptr) {
                                    ex = ptr->cur_;
                                    continue;
                                }
                                return false;
                             }
                    case EQUALITY:{
                                      ASTNode* left  = ((EqualityNode*)ex)->cur_;
                                      ASTNode* right = ((EqualityNode*)ex)->next_;
                                      std::vector<std::string> left_fields, right_fields;
                                      accessed_fields(left , left_fields);
                                      accessed_fields(right, right_fields);
                                      if(left_fields.size() == 1 && right_fields.size() == 1) return true;
                                  }
                    default:
                        return false;
                }
            }
            return false;
        }

         bool match_index(ScanOperation* cur_scan, ASTNode* ex, std::string tname) {
            while(ex){
                CategoryType cat = ex->category_;
                switch(cat) {
                    case EXPRESSION:{
                                        ex = ((ExpressionNode*)ex)->cur_;
                                    }
                                    continue;
                    case AND:{
                                auto ptr = ((AndNode*)ex);
                                if(ptr->next_ == nullptr) {
                                    ex = ptr->cur_;
                                    continue;
                                }
                                return false;
                             }
                    case EQUALITY:
                    case COMPARISON:{
                                      ASTNode* left  = nullptr; 
                                      ASTNode* right = nullptr; 
                                      if(cat == EQUALITY){
                                          left  = ((EqualityNode*)ex)->cur_;
                                          right = ((EqualityNode*)ex)->next_;
                                      } else if(COMPARISON){
                                          left  = ((ComparisonNode*)ex)->cur_;
                                          right = ((ComparisonNode*)ex)->next_;
                                      }
                                      if(left->category_ != FIELD && left->category_ != SCOPED_FIELD
                                              && right->category_ != FIELD && right->category_ != SCOPED_FIELD)
                                          return false;
                                      std::vector<std::string> key;
                                      accessed_fields(left , key);
                                      accessed_fields(right, key);
                                      if(key.size() != 1) return false;
                                      std::vector<IndexHeader> indexes = catalog_->getIndexesOfTable(tname);
                                      int key_idx = catalog_->getTableSchema(tname)->colExist(key[0]);
                                      assert(key_idx != -1);
                                      for(int i = 0; i < indexes.size(); ++i) {
                                          // TODO: support multi-field keys.
                                          if(indexes[i].fields_numbers_[0].idx_ == key_idx) {
                                              cur_scan->scan_type_ = INDEX_SCAN;
                                              std::cout << "------ " << indexes[i].index_name_ << "\n";
                                              cur_scan->index_name_ = indexes[i].index_name_;
                                              cur_scan->filter_ = ex;
                                              return true;
                                          }
                                      }
                                      return false;
                                  }
                    default:
                        return false;
                }
            }
            return false;
        }


        AlgebraOperation* createSelectStatementExpression(QueryCTX& ctx, SelectStatementData* data) {
            if(!isValidSelectStatementData(data))
                return nullptr;

            std::vector<ExpressionNode*> splitted_where;
            if(data->where_){
                // split conjunctive predicates.
                splitted_where = split_by_and(ctx, data->where_);
            }
            // collect data about which tables did we access for each splitted predicate from the previous step.
            std::vector<std::pair<std::vector<std::string>, ExpressionNode*>> tables_per_filter;
            for(int i = 0; i < splitted_where.size(); ++i){
                std::vector<std::string> table_access;
                std::unordered_map<std::string, bool> f;
                accessed_tables(splitted_where[i], table_access, catalog_);
                std::vector<std::string> ta;
                for(auto &s: table_access){
                    bool used_in_query = false;
                    for(int j = 0; j < data->table_names_.size(); ++j){
                        if(data->table_names_[j] == s) {
                            used_in_query = true;
                            break;
                        }
                    }
                    if(!f.count(s) && used_in_query){
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


            // this is the "predicate push down" step but we are building the tree from the ground up 
            // with predicates being as low as possible.
            //
            // initialize 1 scanner for each accessed table.
            std::unordered_map<std::string, AlgebraOperation*> table_scanner;
            for(int i = 0; i < data->tables_.size(); ++i){
                //AlgebraOperation* scan = new ScanOperation(data->tables_[i], data->table_names_[i]);
                AlgebraOperation* scan = nullptr;
                ALLOCATE_INIT(ctx.arena_, scan, ScanOperation, data->tables_[i], data->table_names_[i]);
                table_scanner[data->table_names_[i]] = scan;
            }

            // handle filters with 1 table access.
            for(int i = 0; i < splitted_where.size(); ++i){ 
                if(tables_per_filter[i].first.size() != 1) continue;
                std::string cur_table = tables_per_filter[i].first[0];
                ExpressionNode* cur_filter = tables_per_filter[i].second;
                AlgebraOperation* scan = table_scanner[cur_table];
                // if this filter matched an index we don't need to create a filter operator.
                if(scan->type_ == SCAN && ((ScanOperation*)scan)->scan_type_ == SEQ_SCAN &&
                        match_index((ScanOperation*)scan, cur_filter, cur_table)) continue;
                /*table_scanner[cur_table] = new FilterOperation(
                        table_scanner[cur_table], 
                        cur_filter, 
                        data->fields_, data->field_names_, catalog_);*/
                FilterOperation* tmp = nullptr;
                ALLOCATE_INIT(ctx.arena_, tmp, FilterOperation,
                        table_scanner[cur_table],
                        cur_filter,
                        data->fields_, data->field_names_);
                table_scanner[cur_table] = tmp;
            }

            AlgebraOperation* result = nullptr;
            // join tables that where explicitly joined by a 'join ... on' operator.
            for(int i = 0; i < data->joined_tables_.size(); ++i) {
                // TODO: check that fields used inside the ON clause are scoped only to the two tables being joined.
                JoinedTablesData join_data = data->joined_tables_[i];
                assert(
                    data->table_names_.size() > join_data.lhs_idx_ && 
                    data->table_names_.size() > join_data.rhs_idx_
                );
                std::string lhs_name = data->table_names_[join_data.lhs_idx_];
                std::string rhs_name = data->table_names_[join_data.rhs_idx_];
                assert(table_scanner.count(lhs_name) && table_scanner.count(rhs_name));
                // parse the condition to decide the join algorithm.
                JoinAlgorithm join_algorithm = NESTED_LOOP_JOIN;
                if(is_hashable_condition(join_data.condition_)) join_algorithm = HASH_JOIN;
                /*auto join_op = new JoinOperation(
                                        table_scanner[lhs_name], 
                                        table_scanner[rhs_name],
                                        join_data.condition_,
                                        join_data.type_,
                                        join_algorithm
                                     );*/
                JoinOperation* join_op = nullptr;
                ALLOCATE_INIT(ctx.arena_, join_op, JoinOperation,
                        table_scanner[lhs_name], 
                        table_scanner[rhs_name],
                        join_data.condition_,
                        join_data.type_,
                        join_algorithm
                        );
                // lhs scanner eats rhs scanner and becomes a join node for latter use.
                // TODO: maybe there is a better way.
                table_scanner.erase(rhs_name);
                table_scanner[lhs_name] = join_op;
            }

            // handle filters with 2 or more table access.
            for(int i = 0; i < splitted_where.size(); ++i){
                if(tables_per_filter[i].first.size() < 2) continue;
                // loop over all tables that was accessed with in filter number 'i'
                for(int j = 0; j < tables_per_filter[i].first.size(); ++j) {
                    std::string t = tables_per_filter[i].first[j];
                    if(result == nullptr) {
                        result = table_scanner[t];
                    } else if(table_scanner.count(t)){
                        //result =  new ProductOperation(table_scanner[t], result); 
                        ProductOperation* tmp = nullptr;
                        ALLOCATE_INIT(ctx.arena_, tmp, ProductOperation, table_scanner[t], result);
                        result = tmp;
                    } else {
                        continue;
                    }
                    table_scanner.erase(t);
                }
                FilterOperation* tmp = nullptr;
                ALLOCATE_INIT(ctx.arena_, tmp, FilterOperation, 
                        result,
                        tables_per_filter[i].second,
                        data->fields_, data->field_names_);
                result = tmp;
                /*
                result = new FilterOperation(
                        result, 
                        tables_per_filter[i].second, 
                        data->fields_, data->field_names_, catalog_);*/
            }
            replaceFilteredProductWithJoin(ctx, &result);


            // remaining table outside of filters.
            for(std::string t : data->table_names_) {
                if(!table_scanner.count(t)) continue;
                if(result == nullptr){
                    result = table_scanner[t];
                } else{
                    ProductOperation* tmp = nullptr;
                    ALLOCATE_INIT(ctx.arena_, tmp, ProductOperation, table_scanner[t], result);
                    result = tmp;
                    //result =  new ProductOperation(table_scanner[t], result);
                }
            }

            // handle filters with 0 table access. (can't be pushed down).
            for(int i = 0; i < splitted_where.size(); ++i){
                if(tables_per_filter[i].first.size() == 0){
                    FilterOperation* tmp = nullptr;
                    ALLOCATE_INIT(ctx.arena_, tmp, FilterOperation, 
                            result, 
                            tables_per_filter[i].second,
                            data->fields_, data->field_names_);
                    result = tmp;
                    /*
                    result = new FilterOperation(
                            result, 
                            tables_per_filter[i].second, 
                            data->fields_, data->field_names_, catalog_);*/
                }
            }



            if(data->aggregates_.size() || data->group_by_.size()){
                AggregationOperation* tmp = nullptr;
                ALLOCATE_INIT(ctx.arena_, tmp, AggregationOperation, result, data->aggregates_, data->group_by_);
                result = tmp;
                //result = new AggregationOperation(result, data->aggregates_, data->group_by_);
                if(data->having_){
                    FilterOperation* tmp = nullptr;
                    ALLOCATE_INIT(ctx.arena_, tmp, FilterOperation, 
                            result,
                            data->having_,
                            data->fields_, data->field_names_);
                    result = tmp;
                    //result = new FilterOperation(result, data->having_, data->fields_, data->field_names_, catalog_);
                }
            }
            if(data->fields_.size()){
                ProjectionOperation* tmp = nullptr;
                ALLOCATE_INIT(ctx.arena_, tmp, ProjectionOperation, result, data->fields_);
                result = tmp;
                //result = new ProjectionOperation(result, data->fields_);
            }
            if(data->order_by_list_.size()){
                SortOperation* tmp = nullptr;
                ALLOCATE_INIT(ctx.arena_, tmp, SortOperation, result, data->order_by_list_);
                result = tmp;
                //result = new SortOperation(result, data->order_by_list_);
            }
            if(result) {
                result->query_idx_ = data->idx_;
                result->query_parent_idx_ = data->parent_idx_;
            }
            result->print(0);
            return result;
        }

        Catalog* catalog_ = nullptr;
};
