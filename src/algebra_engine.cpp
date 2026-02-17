#pragma once
#include "parser.cpp"
#include "algebra_operation.cpp"
#include <queue>
#include <vector>
#include <set>

Vector<ExpressionNode*> split_by_and(QueryCTX* ctx, ExpressionNode* expression);
void accessed_tables(ASTNode* expression ,Vector<String8>& tables, Catalog* catalog, bool only_one = true);
void get_fields_of_query(QueryData* data, Vector<FieldNode*>& fields);

class AlgebraEngine {
    public:
        AlgebraEngine(Catalog* catalog): catalog_(catalog)
        {}
        ~AlgebraEngine(){}


        Vector<int> find_filters_that_access_table(String8 table, 
                Vector<std::pair<Vector<String8>, ExpressionNode*>> tables_per_filter) {
            Vector<int> ans;
            for(int i = 0; i < tables_per_filter.size(); ++i){
                for(int j = 0; j < tables_per_filter[i].first.size(); ++j){
                    String8 cur = tables_per_filter[i].first[j];
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
        void group_close_tables(Vector<std::pair<Vector<String8>, ExpressionNode*>>& tables_per_filter) {
            std::queue<String8> q;
            std::set<int> visited_filter;
            std::set<String8> visited_table;
            Vector<int> sorted_order;
            for(int f = 0; f < tables_per_filter.size(); ++f){
                if(tables_per_filter[f].first.size() == 0) { // no accessed_tables comes first.
                    sorted_order.push_back(f);
                    continue;
                };
                q.push(tables_per_filter[f].first[0]);
                visited_table.insert(q.front());
                while(!q.empty()) {
                    int n = q.size();
                    String8 frnt = q.front(); q.pop();
                    for(int i = 0; i < n; ++i){
                        Vector<int> ans = find_filters_that_access_table(frnt, tables_per_filter);
                        for(int k = 0; k < ans.size(); ++k){
                            if(visited_filter.count(ans[k])) continue;
                            visited_filter.insert(ans[k]);
                            sorted_order.push_back(ans[k]);
                            for(int j = 0; j < tables_per_filter[ans[k]].first.size(); ++j){
                                String8 cur = tables_per_filter[ans[k]].first[j];
                                if(visited_table.count(cur)) continue;
                                visited_table.insert(cur);
                                q.push(cur);
                            }
                        }

                    }
                }
            }
            Vector<std::pair<Vector<String8>, ExpressionNode*>> tables_per_filter_sorted;
            for(int i = 0; i < sorted_order.size(); ++i){
                int idx = sorted_order[i];
                tables_per_filter_sorted.push_back(tables_per_filter[idx]);
            }
            assert(tables_per_filter.size() == tables_per_filter_sorted.size());
            for(int i = 0; i < sorted_order.size(); ++i){
                tables_per_filter[i] = tables_per_filter_sorted[i];
            }
        }



        // return false on error.
        bool assign_tables_to_fields(QueryCTX& ctx, QueryData* data) {
            if(!data) return false;
            Vector<FieldNode*> fields;
            get_fields_of_query(data, fields);
            i32 query_idx = data->idx_;
            data->accessed_fields_.resize(fields.size(), nullptr);
            while(query_idx > -1) {
                QueryData* cur_data = ctx.queries_call_stack_[query_idx];
                for(u32 i = 0; i < fields.size(); ++i) {
                    // if this field is already covered by subqueries skip it.
                    if(!fields[i]) continue; 
                    bool is_scoped_field = (fields[i]->table_name_ != nullptr);
                    bool already_matched = false;

                    Vector<String8> tables = catalog_->get_tables_by_field(fields[i]->token_.val_);
                    for(u32 j = 0; j < tables.size(); ++j){
                        for(u32 k = 0; k < cur_data->tables_.size(); ++k){
                            if(tables[j] == cur_data->tables_[k]) {
                                if(is_scoped_field) {
                                    if(fields[i]->table_name_->token_.val_ != cur_data->table_names_[k]) continue;

                                    // found the same table twice for the same field!
                                    if(already_matched){
                                        assert(0 && "Table is ambiguas!");
                                    }

                                    already_matched = true;
                                } else {

                                    // found the same table twice for the same field!
                                    if(already_matched){
                                        assert(0 && "Table is ambiguas!");
                                    }
                                    already_matched = true;

                                    ASTNode* tab = nullptr;
                                    Token tab_tok = Token(TokenType::TABLE, cur_data->table_names_[k]);
                                    ALLOCATE_INIT(ctx.arena_, tab, ASTNode, TABLE, tab_tok);
                                    fields[i]->table_name_ = tab;
                                }
                            }
                        }
                    }

                    if(already_matched) {
                        if(!data->corelated_ && query_idx != data->idx_) {
                            printf("Query %d is corelated duo to field %.*s\n", 
                                    data->idx_, (int)fields[i]->token_.val_.size_, fields[i]->token_.val_.str_);
                            data->corelated_ = true;
                        }
                        fields[i]->query_idx_ = query_idx;
                        data->accessed_fields_[i] = fields[i];
                        fields[i] = nullptr;
                    }
                }
                // check parent queries.
                query_idx = cur_data->parent_idx_;
            }
            // check if some fields did not find their table.
            for(u32 i = 0; i < fields.size(); ++i) {
                if(fields[i]) assert(0 && "Couldn't find table for this field");
            }
            return true;
        }
        
        void createAlgebraExpression(QueryCTX& ctx){
            for(auto data : ctx.queries_call_stack_){
                if(!assign_tables_to_fields(ctx, data)) assert(0);
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
                    case DELETE_DATA:
                        {
                            auto delete_data = reinterpret_cast<DeleteStatementData*>(data);
                            AlgebraOperation* op = createDeleteStatementExpression(ctx, delete_data);
                            if(!op){
                                ctx.error_status_ = Error::LOGICAL_PLAN_ERROR;
                                return;
                            }
                            ctx.operators_call_stack_.push_back(op);
                        } break;
                    case UPDATE_DATA:
                        {
                            auto update_data = reinterpret_cast<UpdateStatementData*>(data);
                            AlgebraOperation* op = createUpdateStatementExpression(ctx, update_data);
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
            for(String8 table_name : data->tables_){
                TableSchema* schema = catalog_->get_table_schema(table_name);
                if(!schema) {
                    std::cout << "[ERROR] Invalid table name" << std::endl;
                    return false;
                }
            }
            if(data->has_star_ &&  data->tables_.size() == 0) {
                std::cout << "[ERROR] no table spicified for SELECT *\n";
                return false;
            }
            for(int order_by : data->order_by_list_){
                if(order_by >= data->fields_.size()) {
                        std::cout << "[ERROR] order by list should be between 1 and " <<  data->fields_.size() << std::endl;
                        return false;
                }
            }
            std::unordered_map<String8, bool, String_hash, String_eq> mentioned_tables;
            for(int i = 0; i < data->table_names_.size(); ++i){
                if(mentioned_tables.count(data->table_names_[i])) {
                    /*
                    std::cout << "[ERROR] table name \"" << data->table_names_[i] << "\" specified more than once." 
                        << std::endl;*/
                    return false;

                }
                mentioned_tables.insert({data->table_names_[i], true});
            }
            // TODO: provide validation for fields and filters.
            return  true;
        }
        bool isValidInsertStatementData (InsertStatementData* data){
            // TODO: provide validation.
            return true;
        }

        bool isValidDeleteStatementData (DeleteStatementData* data){
            // TODO: provide validation.
            return true;
        }

        bool isValidUpdateStatementData (UpdateStatementData* data){
            // TODO: provide validation.
            return true;
        }


        AlgebraOperation* createInsertStatementExpression(QueryCTX& ctx, InsertStatementData* data){
            if(!isValidInsertStatementData(data))
                return nullptr;

            AlgebraOperation* result = New(InsertionOperation, ctx.arena_, data->idx_);
            return result;
        }

        AlgebraOperation* createDeleteStatementExpression(QueryCTX& ctx, DeleteStatementData* data){
            if(!isValidDeleteStatementData(data))
                return nullptr;
            AlgebraOperation* child = optimize(ctx, data); 
            AlgebraOperation* del = New(DeletionOperation, ctx.arena_, child, data->idx_);
            del->print(0);
            return del;
        }

        AlgebraOperation* createUpdateStatementExpression(QueryCTX& ctx, UpdateStatementData* data){
            if(!isValidUpdateStatementData(data))
                return nullptr;
            AlgebraOperation* child = optimize(ctx, data); 
            AlgebraOperation* update = New(UpdateOperation, ctx.arena_, child, data->idx_);
            update->print(0);
            return update;
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
                                    ExceptOperation* tmp = New(ExceptOperation, ctx.arena_, data->idx_, lhs, rhs, all);
                                    lhs = tmp;
                                } else if(op == UNION){
                                    UnionOperation* tmp = New(UnionOperation, ctx.arena_, data->idx_,lhs, rhs, all);
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
                            if(once) return lhs;
                            bool all = intersect->all_;
                            auto ptr = intersect->next_;
                            while(ptr){
                                AlgebraOperation* rhs = createSetOperationExpression(
                                        ctx, ptr,
                                        (ptr->type_ == INTERSECT)
                                    );
                                IntersectOperation* tmp = New(IntersectOperation, ctx.arena_, data->idx_, lhs, rhs, all);
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

                  JoinOperation* join_node = New(JoinOperation, ctx.arena_, product->query_idx_,
                          product->lhs_, 
                          product->rhs_, 
                          op->filter_,
                          INNER_JOIN, join_algorithm);
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
                                      Vector<FieldNode*> left_fields, right_fields;
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
        // return: if ret < 0  => didn't find a match.
        //         if ret >= 0 => the offset of the field within the IndexKey.
        int match_index_to_filter(IndexHeader& index, TableSchema* table, ASTNode* ex) {
            while(ex){
                CategoryType cat = ex->category_;
                switch(cat) {
                    case EXPRESSION:{
                                        ex = ((ExpressionNode*)ex)->cur_;
                                    }
                                    continue;
                    case AND:{
                                 auto ptr = ((AndNode*)ex);
                                 if(ptr->next_ == nullptr || ptr->mark_split_) {
                                     ex = ptr->cur_;
                                     continue;
                                 }
                                 return -1;
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
                                        if(left->category_ != FIELD_EXPR 
                                                && right->category_ != FIELD_EXPR)
                                            return -1;
                                        Vector<FieldNode*> key;
                                        accessed_fields(left , key);
                                        accessed_fields(right, key);
                                        if(key.size() != 1) return -1;
                                        int key_idx = 
                                            table->col_exist(key[0]->token_.val_, key[0]->table_name_->token_.val_);
                                        assert(key_idx != -1);
                                        if(key[0]->table_name_) 
                                            assert(table->getTableName() == key[0]->table_name_->token_.val_);

                                        for(int i = 0; i < index.fields_numbers_.size(); ++i) {
                                            if(index.fields_numbers_[i].idx_ == key_idx) {
                                                return i;
                                            }
                                        }
                                        return -1;
                                    }
                    default:
                                    return -1;
                }
            }
            return -1;
        }

        bool match_index(ScanOperation* cur_scan) {
            if(cur_scan->filters_.size() == 0) return false;
            String8 tname = cur_scan->table_name_;
            TableSchema* tschema = catalog_->get_table_schema(tname);
            assert(tschema);
            Vector<IndexHeader> table_indexes = catalog_->get_indexes_of_table(tname);
            if(table_indexes.size() == 0) return false;
            std::pair<int, std::vector<int>> best_index = {-1, {}};
            for(int i = 0 ; i < table_indexes.size(); ++i) {
                std::vector<int> matched_filters(table_indexes[i].fields_numbers_.size(), -1);
                int max_offset = -1;
                for(int j = 0; j < cur_scan->filters_.size(); ++j) {
                    int filter_offset = match_index_to_filter(table_indexes[i], tschema, cur_scan->filters_[j]);
                    if(filter_offset >= 0)
                        matched_filters[filter_offset] = j;
                }

                for(int j = 0; j < matched_filters.size(); ++j){
                    if(matched_filters[j] == -1) break;
                     //TODO: fix this condition.
                    // this condition makes sure that:
                    // all matched_filters are stored with the same order(desc or asc) within the index.
                    // for example:
                    // index with fields (col0 desc, col1 asc), 
                    // a query with predicates: col0 > 5000 AND col1 > 1000
                    // will only use the first predicate as a search key.
                    //if(j > 0 && 
                     //       table_indexes[i].fields_numbers_[j].desc_ != table_indexes[i].fields_numbers_[j-1].desc_) break;
                    if(j > 0) break;
                    max_offset = j;
                }
                if(max_offset >= 0 && max_offset >= best_index.second.size()) {
                    best_index = {i, {}};
                    for(int i = 0; i <= max_offset; ++i)
                        best_index.second.push_back(matched_filters[i]);
                }
            }
            // didn't match any indexes.
            if(best_index.first == -1) return false;
            assert(best_index.second.size() > 0);
            cur_scan->scan_type_ = INDEX_SCAN;
            cur_scan->index_name_ = table_indexes[best_index.first].index_name_;
            for(int i = 0; i < best_index.second.size(); ++i){
                int cur_filter_idx = best_index.second[i];
                cur_scan->index_filters_.push_back(cur_scan->filters_[cur_filter_idx]);
                cur_scan->filters_[cur_filter_idx] = nullptr;
            }
            // clear the normal filters vector.
            int starting_size = cur_scan->filters_.size();
            for(int i = 0; i < cur_scan->filters_.size(); ++i){
                if(cur_scan->filters_[i] == nullptr){
                    cur_scan->filters_.erase(cur_scan->filters_.begin() + i);
                    i--;
                }
            }
            assert(starting_size == cur_scan->filters_.size() + cur_scan->index_filters_.size());
            return true;
        }

        // should only be used with 'select', 'delete' and 'update' statements.
        AlgebraOperation* optimize(QueryCTX& ctx, QueryData* data) {
            int query_idx = data->idx_;
            Vector<ExpressionNode*> splitted_where;
            if(data->where_){
                // split conjunctive predicates.
                splitted_where = split_by_and(&ctx, data->where_);
            }
            // collect data about which tables did we access for each splitted predicate from the previous step.
            Vector<std::pair<Vector<String8>, ExpressionNode*>> tables_per_filter;
            for(int i = 0; i < splitted_where.size(); ++i){
                Vector<String8> table_access;
                std::unordered_map<String8, bool, String_hash, String_eq> f;
                accessed_tables(splitted_where[i], table_access, catalog_);
                Vector<String8> ta;
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
                    [](std::pair<Vector<String8>, ExpressionNode*> lhs,
                        std::pair<Vector<String8>, ExpressionNode*> rhs) {
                    return lhs.first.size() < rhs.first.size();
                    });



            group_close_tables(tables_per_filter);


            // this is the "predicate push down" step but we are building the tree from the ground up 
            // with predicates being as low as possible.
            //
            // initialize 1 scanner for each accessed table.
            std::unordered_map<String8, AlgebraOperation*, String_hash, String_eq> table_scanner;
            for(int i = 0; i < data->tables_.size(); ++i){
                String8 t = data->tables_[i];
                String8 tn = data->table_names_[i];
                AlgebraOperation* scan = New(ScanOperation, ctx.arena_, query_idx, t, tn);
                table_scanner[tn] = scan;
            }

            // handle filters with 1 table access.
            for(int i = 0; i < splitted_where.size(); ++i) { 
                if(tables_per_filter[i].first.size() != 1) continue;
                String8 cur_table = tables_per_filter[i].first[0];
                ExpressionNode* cur_filter = tables_per_filter[i].second;
                assert(table_scanner[cur_table]->type_ == SCAN);
                ((ScanOperation*)table_scanner[cur_table])->filters_.push_back(cur_filter);
            }
            // check if sequential scans can be switched into index scans based on their predicates.
            for(auto& scanner: table_scanner) {
                ScanOperation* scan = (ScanOperation*)(scanner.second);
                // no filters => no index scan.
                if(scan->filters_.size() == 0) continue;
                // if this is a delete/update operation 
                // => no index scan for the table to be deleted/updated from.
                if(data->type_ != SELECT_DATA && scan->table_name_ == data->table_names_[0]) continue;
                // check for a suitable index.
                match_index(scan);
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
                String8 lhs_name = data->table_names_[join_data.lhs_idx_];
                String8 rhs_name = data->table_names_[join_data.rhs_idx_];
                assert(table_scanner.count(lhs_name) && table_scanner.count(rhs_name));
                // parse the condition to decide the join algorithm.
                JoinAlgorithm join_algorithm = NESTED_LOOP_JOIN;
                if(is_hashable_condition(join_data.condition_)) join_algorithm = HASH_JOIN;
                JoinOperation* join_op = New(JoinOperation, ctx.arena_, query_idx, 
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
                    String8 t = tables_per_filter[i].first[j];
                    if(result == nullptr) {
                        result = table_scanner[t];
                    } else if(table_scanner.count(t)){
                        ProductOperation* tmp = New(ProductOperation, ctx.arena_, query_idx,table_scanner[t], result);
                        result = tmp;
                    } else {
                        continue;
                    }
                    table_scanner.erase(t);
                }
                FilterOperation* tmp = New(FilterOperation, ctx.arena_, query_idx,
                            result,
                            tables_per_filter[i].second);
                     //       data->fields_, data->field_names_);
                result = tmp;
            }
            replaceFilteredProductWithJoin(ctx, &result);


            // remaining table outside of filters.
            for(String8 t : data->table_names_) {
                if(!table_scanner.count(t)) continue;
                if(result == nullptr){
                    result = table_scanner[t];
                } else{
                    ProductOperation* tmp = New(ProductOperation, ctx.arena_, query_idx, table_scanner[t], result);
                    result = tmp;
                }
            }

            // handle filters with 0 table access. (can't be pushed down).
            for(int i = 0; i < splitted_where.size(); ++i){
                if(tables_per_filter[i].first.size() == 0){
                    FilterOperation* tmp = New(FilterOperation, ctx.arena_, query_idx,
                            result, 
                            tables_per_filter[i].second);
                            //data->fields_, data->field_names_);
                    result = tmp;
                }
            }
            return result;
        };

        AlgebraOperation* createSelectStatementExpression(QueryCTX& ctx, SelectStatementData* data) {
            if(!isValidSelectStatementData(data))
                return nullptr;

            int query_idx = data->idx_;

            auto result = optimize(ctx, data);
            if(data->aggregates_.size() || data->group_by_.size()){
                AggregationOperation* tmp = New(AggregationOperation, ctx.arena_, query_idx, result, data->aggregates_, data->group_by_);
                result = tmp;
                if(data->having_){
                    FilterOperation* tmp = New(FilterOperation, ctx.arena_, query_idx,
                            result,
                            data->having_);
                     //       data->fields_, data->field_names_);
                    result = tmp;
                }
            }
            if(data->fields_.size()){
                ProjectionOperation* tmp = New(ProjectionOperation, ctx.arena_, query_idx, result, data->fields_);
                result = tmp;
            }
            if(data->order_by_list_.size()){
                SortOperation* tmp = New(SortOperation, ctx.arena_, query_idx, result, data->order_by_list_);
                result = tmp;
            }
            result->print(0);
            return result;
        }

        Catalog* catalog_ = nullptr;
};
