#pragma once
#include "parser.cpp"
#include "expression.h"
#include "algebra_engine.cpp"
#include "catalog.cpp"
#include "executor.cpp"
#include "utils.h"
#include <deque>


typedef std::vector<std::vector<Value>> QueryResult;
struct IndexHeader;


/* The execution engine that holds all execution operators that could be 
 * (sequential scan, index scan) which are access operators,
 * (filter, join, projection) that are relational algebra operators, 
 * other operator such as (sorting, aggrecations) 
 * or modification queries (delete, insert, update).
 * Each one of these operators implements a next() method that will produce the next record of the current table that
 * is being scanned filtered joined etc... This method has so many names ( volcano, pipleline, iterator ) as apposed to
 * building the entire output table and returning it all at once.
 * In order to initialize an operator we need some sorts of meta data that should be passed into it via the constructor,
 * The larger the project gets the more data we are going to need, Which might require using a wrapper class 
 * around the that data, But for now we are just going to pass everything to the constructor.
 */


class ExecutionEngine {
    public:
        ExecutionEngine(Catalog* catalog): catalog_(catalog)
    {}
        ~ExecutionEngine() {}

        // DDL handlers.
        bool create_table_handler(QueryCTX& ctx) {
            CreateTableStatementData* create_table = reinterpret_cast<CreateTableStatementData*>(ctx.queries_call_stack_[0]);
            std::string table_name = create_table->table_name_;
            std::vector<FieldDef> fields = create_table->field_defs_;
            std::deque<std::string> col_names;
            std::deque<Type> col_types;
            std::deque<std::vector<Constraint>> col_constraints;
            for(int i = 0; i < fields.size(); ++i){
                std::string name = fields[i].field_name_;
                Type type = catalog_->tokenTypeToColType(fields[i].type_);
                if(type == INVALID) {
                    std::cout << "[ERROR] Invalid type\n";
                    return false;
                }
                col_names.push_back(name);
                col_types.push_back(type);
                col_constraints.push_back(fields[i].constraints_);
            }
            std::vector<Column> columns;
            std::vector<IndexField> primary_key_cols;
            uint8_t offset_ptr = 0;
            for(size_t i = 0; i < col_names.size(); ++i){
                columns.push_back(Column(col_names[i], col_types[i], offset_ptr, col_constraints[i]));
                bool is_primary_key = false;
                for(int j = 0; j < col_constraints[i].size(); ++j){
                  if(col_constraints[i][j] == Constraint::PRIMARY_KEY){
                    is_primary_key = true;
                    break;
                  }
                }
                if(is_primary_key) 
                    primary_key_cols.push_back({col_names[i], false}); // primary key can only be asc.
                offset_ptr += Column::getSizeFromType(col_types[i]);
            }
            TableSchema* sch = catalog_->createTable(table_name, columns);
            if(sch == nullptr) return false;
            if(!primary_key_cols.empty()) {
                std::cout << "create idx from create pkey\n";
              int err = catalog_->createIndex(table_name, table_name+"_pkey", primary_key_cols);
              // TODO: use CTX error status instead of this.
              if(err) return false;
            }
            return true;
        }

        bool create_index_handler(QueryCTX& ctx) {
            CreateIndexStatementData* create_index = reinterpret_cast<CreateIndexStatementData*>(ctx.queries_call_stack_[0]);
            std::string index_name = create_index->index_name_;
            std::string table_name = create_index->table_name_;
            std::vector<IndexField> fields = create_index->fields_;
            bool err = catalog_->createIndex(table_name, index_name, fields);
            if(err) return false;
            return true;
        }

        /*
        bool delete_handler(ASTNode* statement_root){
            DeleteStatementNode* delete_statement = reinterpret_cast<DeleteStatementNode*>(statement_root);

            auto table_ptr = delete_statement->table_; 
            // did not find any tables.
            if(table_ptr == nullptr) return false;
            std::string table_name = table_ptr->token_.val_;
            TableSchema* schema = catalog_->getTableSchema(table_name);

            TableIterator* it = schema->getTable()->begin();
            while(it->advance()){
                RecordID rid = it->getCurRecordID();
                schema->getTable()->deleteRecord(rid);
            }
            // handle filters later.
            
            return true;
        }

        bool update_handler(ASTNode* statement_root){
            UpdateStatementNode* update_statement = reinterpret_cast<UpdateStatementNode*>(statement_root);

            std::string table_name = update_statement->table_->token_.val_;
            TableSchema* schema = catalog_->getTableSchema(table_name);
            // did not find any tables with that name.
            if(schema == nullptr) return false;
            auto field_ptr = update_statement->field_;
            std::string field_name = field_ptr->token_.val_;
            auto val_ptr = update_statement->expression_;
            std::string val_str = val_ptr->token_.val_;
            // check valid column.
            if(!schema->isValidCol(field_name)) 
                return false;
            // we consider int and string types for now.
            Type val_type = INVALID;
            if(val_ptr->category_ == STRING_CONSTANT) val_type = VARCHAR;
            else if(val_ptr->category_ == INTEGER_CONSTANT) val_type = INT;
            // invalid or not supported type;
            if( val_type == INVALID ) return false;
            Value val;
            if(val_type == INT) val = Value(stoi(val_str));
            else if(val_type == VARCHAR) val = Value(val);

            if(!schema->checkValidValue(field_name, val)) return false;


            TableIterator* it = schema->getTable()->begin();
            while(it->advance()){
                RecordID rid = it->getCurRecordID();
                // rid is not used for now.
                Record cpy = it->getCurRecordCpy();
                std::vector<Value> values;
                int err = schema->translateToValues(cpy, values);
                int idx = schema->getColIdx(field_name, val);
                if(idx < 0) return false;
                values[idx] = val;
                Record* new_rec = schema->translateToRecord(values);

                err = schema->getTable()->updateRecord(&rid, *new_rec);
                if(err) return false;
            }
            return true;
            // handle filters later.
        }
        */

        // DDL execution.
        bool directExecute(QueryCTX& ctx){
            // should always be 1.
            if(ctx.queries_call_stack_.size() != 1) return false;
            std::cout << "[INFO] executing DDL command" << std::endl;
            QueryType type = ctx.queries_call_stack_[0]->type_;
            switch (type) {
                case CREATE_TABLE_DATA:
                    return create_table_handler(ctx);
                case CREATE_INDEX_DATA:
                    return create_index_handler(ctx);
                default:
                    return false;
            }
        }

        // DML execution.
        bool executePlan(QueryCTX& ctx, QueryResult* result){
            if(ctx.queries_call_stack_.size() < 1 || !result) return false;
            std::cout << "[INFO] Creating physical plan" << std::endl;

            for(auto cur_plan : ctx.operators_call_stack_){
                Executor* created_physical_plan = buildExecutionPlan(ctx, cur_plan, cur_plan->query_idx_, cur_plan->query_parent_idx_);
                if(!created_physical_plan){
                    std::cout << "[ERROR] Could not build physical operation\n";
                    return false;
                }
                if(cur_plan->distinct_){
                    created_physical_plan = new DistinctExecutor(created_physical_plan, created_physical_plan->output_schema_, ctx, cur_plan->query_idx_, cur_plan->query_parent_idx_);
                }

                if(created_physical_plan->query_idx_ > 0 &&
                        created_physical_plan->query_idx_ < ctx.queries_call_stack_.size() &&
                        !ctx.queries_call_stack_[created_physical_plan->query_idx_]->is_corelated_
                  ){
                    std::cout << "not corelated query: " << created_physical_plan->query_idx_ << "\n";
                    created_physical_plan = new SubQueryExecutor(created_physical_plan, created_physical_plan->output_schema_, ctx, cur_plan->query_idx_, cur_plan->query_parent_idx_);
                }
                ctx.executors_call_stack_.push_back(created_physical_plan);
            }

            std::cout << "[INFO] executing physical plan" << std::endl;
            return runExecutor(ctx, result);
        }

    private:

        Executor* buildExecutionPlan(QueryCTX& ctx, AlgebraOperation* logical_plan, int query_idx, int parent_query_idx) {
            if(!logical_plan) return nullptr;
            switch(logical_plan->type_) {
                case SORT: 
                    {
                        SortOperation* op = reinterpret_cast<SortOperation*>(logical_plan);
                        Executor* child = buildExecutionPlan(ctx, op->child_, query_idx, parent_query_idx);
                        SortExecutor* sort = new SortExecutor(child, child->output_schema_, op->order_by_list_, ctx, query_idx, parent_query_idx);
                        return sort;
                    } break;
                case PROJECTION: 
                    {
                        ProjectionOperation* op = reinterpret_cast<ProjectionOperation*>(logical_plan);
                        Executor* child = buildExecutionPlan(ctx, op->child_, query_idx, parent_query_idx);

                        ProjectionExecutor* project = nullptr; 
                        if(!child)
                            project = new ProjectionExecutor(child, nullptr, op->fields_, ctx, query_idx, parent_query_idx);
                        else
                            project = new ProjectionExecutor(child, child->output_schema_, op->fields_, ctx, query_idx, parent_query_idx);
                        return project;
                    } break;
                case AGGREGATION: 
                    {
                        AggregationOperation* op = reinterpret_cast<AggregationOperation*>(logical_plan);
                        Executor* child = buildExecutionPlan(ctx, op->child_, query_idx, parent_query_idx);

                        std::vector<Column> new_cols;
                        int offset_ptr = 0; 
                        if(child && child->output_schema_){
                            new_cols = child->output_schema_->getColumns();
                            offset_ptr = Column::getSizeFromType(new_cols[new_cols.size() - 1].getType());
                        } 
                        for(int i = 0; i < op->aggregates_.size(); i++){
                            std::string col_name = "agg_tmp_schema.";
                            col_name += AGG_FUNC_IDENTIFIER_PREFIX;
                            //col_name += intToStr(op->aggregates_[i]->parent_id_);
                            col_name += intToStr(i+1);
                            new_cols.push_back(Column(col_name, INT, offset_ptr));
                            offset_ptr += Column::getSizeFromType(INT);
                        }
                        TableSchema* new_output_schema = new TableSchema("agg_tmp_schema", nullptr, new_cols, true);

                        return new AggregationExecutor(child, new_output_schema, op->aggregates_, op->group_by_, ctx, query_idx, parent_query_idx);
                    } break;
                case FILTER: 
                    {
                        FilterOperation* op = reinterpret_cast<FilterOperation*>(logical_plan);
                        Executor* child = buildExecutionPlan(ctx, op->child_, query_idx, parent_query_idx);
                        TableSchema* schema = nullptr;
                        if(child == nullptr) schema = nullptr;
                        else                 schema = child->output_schema_;

                        FilterExecutor* filter = new FilterExecutor(child, schema, op->filter_, op->fields_, op->field_names_, ctx, query_idx, parent_query_idx);
                        return filter;
                    } break;
                case SCAN: 
                    {
                        ScanOperation* op = reinterpret_cast<ScanOperation*>(logical_plan);
                        TableSchema* schema = catalog_->getTableSchema(op->table_name_);
                        std::string tname =  op->table_name_;
                        if(op->table_rename_.size() != 0) tname = op->table_rename_;
                        std::vector<Column> columns = schema->getColumns();
                        // create a new schema and rename columns to table.col_name
                        for(int i = 0; i < columns.size(); i++){
                            std::string col_name = tname; 
                            col_name += ".";
                            col_name += columns[i].getName();
                            columns[i].setName(col_name);
                        }

                        TableSchema* new_output_schema = new TableSchema(tname, schema->getTable(), columns, true);
                        Executor* scan = nullptr;
                        if(op->scan_type_ == SEQ_SCAN){
                            scan = new SeqScanExecutor(new_output_schema, ctx, query_idx, parent_query_idx);
                        } else {
                            scan = new IndexScanExecutor(catalog_->getIndexHeader(op->index_name_),
                                    op->filter_,
                                    new_output_schema,
                                    ctx, query_idx,
                                    parent_query_idx);
                        }
                        return scan;
                    } break;
                case PRODUCT: 
                    {
                        ProductOperation* op = reinterpret_cast<ProductOperation*>(logical_plan);
                        Executor* lhs = buildExecutionPlan(ctx, op->lhs_, query_idx, parent_query_idx);
                        Executor* rhs = buildExecutionPlan(ctx, op->rhs_, query_idx, parent_query_idx);
                        std::vector<Column> lhs_columns = lhs->output_schema_->getColumns();
                        std::vector<Column> rhs_columns = rhs->output_schema_->getColumns();
                        for(int i = 0; i < lhs_columns.size(); i++)
                            rhs_columns.push_back(lhs_columns[i]);

                        TableSchema* product_output_schema = new TableSchema("TMP_PRODUCT_TABLE", nullptr, rhs_columns, true);
                        ProductExecutor* product = new ProductExecutor(product_output_schema, ctx, query_idx, parent_query_idx, rhs, lhs);
                        return product;
                    } break;
                case JOIN: 
                    {
                        auto op = reinterpret_cast<JoinOperation*>(logical_plan);
                        JoinAlgorithm join_algo = op->join_algo_;
                        Executor* lhs = buildExecutionPlan(ctx, op->lhs_, query_idx, parent_query_idx);
                        Executor* rhs = buildExecutionPlan(ctx, op->rhs_, query_idx, parent_query_idx);
                        std::vector<Column> lhs_columns = lhs->output_schema_->getColumns();
                        std::vector<Column> rhs_columns = rhs->output_schema_->getColumns();
                        for(int i = 0; i < rhs_columns.size(); i++)
                            lhs_columns.push_back(rhs_columns[i]);

                        TableSchema* join_output_schema = new TableSchema("TMP_JOIN_TABLE", nullptr, lhs_columns, true);
                        if(join_algo == NESTED_LOOP_JOIN){
                            return new NestedLoopJoinExecutor(join_output_schema,
                                    ctx, query_idx, parent_query_idx,
                                    lhs, rhs, 
                                    op->filter_, op->join_type_);

                        } else {
                            return new HashJoinExecutor(join_output_schema,
                                        ctx, query_idx, parent_query_idx,
                                        lhs, rhs, 
                                        op->filter_, op->join_type_);

                        }
                    } break;
                case AL_UNION: 
                case AL_EXCEPT: 
                case AL_INTERSECT: 
                    {
                        // TODO: condense all set operators into one operator.
                        if(logical_plan->type_ == AL_UNION){
                            UnionOperation* op = reinterpret_cast<UnionOperation*>(logical_plan);
                            if(!op->lhs_ || !op->rhs_) return nullptr;
                            // TODO: don't rebuild queries.
                            // Executor* lhs = buildExecutionPlan(ctx, op->lhs_, op->lhs_->query_idx_, op->lhs_->query_parent_idx_);
                            // Executor* rhs = buildExecutionPlan(ctx, op->rhs_, op->rhs_->query_idx_, op->rhs_->query_parent_idx_);
                            Executor* lhs = nullptr; 
                            Executor* rhs = nullptr; 

                            if(op->lhs_->query_idx_ == -1)
                                lhs = buildExecutionPlan(ctx, op->lhs_, op->lhs_->query_idx_, op->lhs_->query_parent_idx_);
                            else
                                lhs = ctx.executors_call_stack_[op->lhs_->query_idx_];

                            if(op->rhs_->query_idx_ == -1)
                                rhs = buildExecutionPlan(ctx, op->rhs_, op->rhs_->query_idx_, op->rhs_->query_parent_idx_);
                            else
                                rhs = ctx.executors_call_stack_[op->rhs_->query_idx_];
                            // TODO: check that lhs and rhs have the same schema.
                            UnionExecutor* un = new UnionExecutor(lhs->output_schema_, ctx, query_idx, parent_query_idx, lhs, rhs);

                            // TODO: don't use an extra distinct executor for the all = false case, 
                            // Use a built-in hashtable inside of the set-operation executor instead.
                            if(op->all_ == false) 
                                return new DistinctExecutor(un , un->output_schema_, ctx, query_idx, parent_query_idx);
                            return un;
                        } else if(logical_plan->type_ == AL_EXCEPT) {
                            ExceptOperation* op = reinterpret_cast<ExceptOperation*>(logical_plan);
                            if(!op->lhs_ || !op->rhs_) return nullptr;
                            //Executor* lhs = buildExecutionPlan(ctx, op->lhs_, op->lhs_->query_idx_, op->lhs_->query_parent_idx_);
                            //Executor* rhs = buildExecutionPlan(ctx, op->rhs_, op->rhs_->query_idx_, op->rhs_->query_parent_idx_);
                            Executor* lhs = nullptr; 
                            Executor* rhs = nullptr; 

                            if(op->lhs_->query_idx_ == -1)
                                lhs = buildExecutionPlan(ctx, op->lhs_, op->lhs_->query_idx_, op->lhs_->query_parent_idx_);
                            else
                                lhs = ctx.executors_call_stack_[op->lhs_->query_idx_];

                            if(op->rhs_->query_idx_ == -1)
                                rhs = buildExecutionPlan(ctx, op->rhs_, op->rhs_->query_idx_, op->rhs_->query_parent_idx_);
                            else
                                rhs = ctx.executors_call_stack_[op->rhs_->query_idx_];
                            // TODO: check that lhs and rhs have the same schema.
                            ExceptExecutor* ex = new ExceptExecutor(lhs->output_schema_, ctx, query_idx, parent_query_idx, lhs, rhs);

                            // TODO: don't use an extra distinct executor for the all = false case, 
                            // Use a built-in hashtable inside of the set-operation executor instead.
                            if(op->all_ == false) 
                                return new DistinctExecutor(ex , ex->output_schema_, ctx, query_idx, parent_query_idx);
                            return ex;
                        } else {
                            IntersectOperation* op = reinterpret_cast<IntersectOperation*>(logical_plan);
                            if(!op->lhs_ || !op->rhs_) return nullptr;
                            //Executor* lhs = buildExecutionPlan(ctx, op->lhs_, op->lhs_->query_idx_, op->lhs_->query_parent_idx_);
                            //Executor* rhs = buildExecutionPlan(ctx, op->rhs_, op->rhs_->query_idx_, op->rhs_->query_parent_idx_);
                            Executor* lhs = nullptr; 
                            Executor* rhs = nullptr; 

                            if(op->lhs_->query_idx_ == -1)
                                lhs = buildExecutionPlan(ctx, op->lhs_, op->lhs_->query_idx_, op->lhs_->query_parent_idx_);
                            else
                                lhs = ctx.executors_call_stack_[op->lhs_->query_idx_];

                            if(op->rhs_->query_idx_ == -1)
                                rhs = buildExecutionPlan(ctx, op->rhs_, op->rhs_->query_idx_, op->rhs_->query_parent_idx_);
                            else
                                rhs = ctx.executors_call_stack_[op->rhs_->query_idx_];
                            // TODO: check that lhs and rhs have the same schema.
                            IntersectExecutor* intersect = new IntersectExecutor(lhs->output_schema_, ctx, query_idx, parent_query_idx, lhs, rhs);

                            // TODO: don't use an extra distinct executor for the all = false case, 
                            // Use a built-in hashtable inside of the set-operation executor instead.
                            if(op->all_ == false) 
                                return new DistinctExecutor(intersect , intersect->output_schema_, ctx, query_idx, parent_query_idx);
                            return intersect;
                        }
                    } break;
                case INSERTION: 
                    {
                        auto statement = reinterpret_cast<InsertStatementData*>(ctx.queries_call_stack_[query_idx]);
                        TableSchema* table = catalog_->getTableSchema(statement->table_name_);
                        int select_idx = statement->select_idx_;

                        InsertionExecutor* insert =  new InsertionExecutor(
                                                         table,
                                                         catalog_->getIndexesOfTable(statement->table_name_),            
                                                         ctx, 
                                                         query_idx, 
                                                         parent_query_idx, 
                                                         select_idx
                                                       );
                        return insert;
                    } break;
                default: 
                    std::cout << "[ERROR] unsupported Algebra Operaion\n";
                    return nullptr;
            }
        }
        bool runExecutor(QueryCTX& ctx, QueryResult* result){
            if(ctx.executors_call_stack_.size() < 1 || (bool) ctx.error_status_ ) return false;
            Executor* physical_plan = nullptr;

            // this case means we have set operations: union, except or intersect
            if(ctx.executors_call_stack_.size() > ctx.queries_call_stack_.size()) {
                // first set operation.
                physical_plan = ctx.executors_call_stack_[ctx.queries_call_stack_.size()]; 
                std::cout << "Set operation\n";
            }
            else{
                physical_plan = ctx.executors_call_stack_[0];
            }

            physical_plan->init();
            while(!physical_plan->error_status_ && !physical_plan->finished_){
                std::vector<Value> tmp = physical_plan->next();
                if(tmp.size() == 0 || physical_plan->error_status_) break;
                    
                result->push_back(tmp);
            }
            return (physical_plan->error_status_ == 0);
        }
        Catalog* catalog_;
};
