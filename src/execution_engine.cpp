#pragma once
#include "parser.cpp"
#include "expression.h"
#include "algebra_engine.cpp"
#include "catalog.cpp"
#include "executor.cpp"
#include "utils.h"
#include <deque>


typedef Vector<Vector<Value>> QueryResult;
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
            String table_name = create_table->table_name_;
            Vector<FieldDef> fields = create_table->field_defs_;
            std::deque<String> col_names;
            std::deque<Type> col_types;
            std::deque<ConstraintType> col_constraints;
            for(int i = 0; i < fields.size(); ++i){
                String name = fields[i].field_name_;
                Type type = tokenTypeToColType(fields[i].type_);
                if(type == INVALID) {
                    std::cout << "[ERROR] Invalid type\n";
                    return false;
                }
                col_names.push_back(name);
                col_types.push_back(type);
                col_constraints.push_back(fields[i].constraints_);
            }
            Vector<Column> columns;
            Vector<IndexField> primary_key_cols;
            uint8_t offset_ptr = 0;
            for(size_t i = 0; i < col_names.size(); ++i){
                columns.push_back(Column(col_names[i], col_types[i], offset_ptr, col_constraints[i]));
                bool is_primary_key = col_constraints[i]&CONSTRAINT_PRIMARY_KEY;
                if(is_primary_key) 
                    primary_key_cols.push_back({col_names[i], false}); // primary key can only be asc.
                offset_ptr += Column::getSizeFromType(col_types[i]);
            }
            TableSchema* sch = catalog_->createTable(&ctx, table_name, columns);
            if(sch == nullptr) return false;
            if(!primary_key_cols.empty()) {
                std::cout << "create idx from create pkey\n";
              int err = catalog_->createIndex(&ctx, table_name, table_name+"_pkey", primary_key_cols);
              // TODO: use CTX error status instead of this.
              if(err) return false;
            }
            return true;
        }

        bool create_index_handler(QueryCTX& ctx) {
            CreateIndexStatementData* create_index = reinterpret_cast<CreateIndexStatementData*>(ctx.queries_call_stack_[0]);
            String index_name = create_index->index_name_;
            String table_name = create_index->table_name_;
            Vector<IndexField> fields = create_index->fields_;
            bool err = catalog_->createIndex(&ctx, table_name, index_name, fields);
            if(err) return false;
            return true;
        }

        bool drop_table_handler(QueryCTX& ctx) {
            auto drop_table = reinterpret_cast<DropTableStatementData*>(ctx.queries_call_stack_[0]);
            String table_name = drop_table->table_name_;
            bool err = catalog_->deleteTable(&ctx, table_name);
            if(err) return false;
            return true;
        }

        bool drop_index_handler(QueryCTX& ctx) {
            DropIndexStatementData* drop_index = reinterpret_cast<DropIndexStatementData*>(ctx.queries_call_stack_[0]);
            String index_name = drop_index->index_name_;
            bool err = catalog_->deleteIndex(&ctx, index_name);
            if(err) return false;
            return true;
        }

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
                case DROP_TABLE_DATA:
                    return drop_table_handler(ctx);
                case DROP_INDEX_DATA:
                    return drop_index_handler(ctx);
                default:
                    return false;
            }
        }

        // DML execution.
        bool executePlan(QueryCTX& ctx, Executor** execution_root){
            if(ctx.queries_call_stack_.size() < 1) return false;
            std::cout << "[INFO] Creating physical plan" << std::endl;

            for(auto cur_plan : ctx.operators_call_stack_){
                Executor* created_physical_plan = buildExecutionPlan(ctx, cur_plan);
                if(!created_physical_plan){
                    std::cout << "[ERROR] Could not build physical operation\n";
                    return false;
                }
                if(cur_plan->distinct_){
                    DistinctExecutor* dis = nullptr;
                    ALLOCATE_CONSTRUCT(ctx.arena_, dis, DistinctExecutor, &ctx, created_physical_plan);
                    created_physical_plan = dis;
                }

                if(created_physical_plan->query_idx_ > 0 &&
                        created_physical_plan->query_idx_ < ctx.queries_call_stack_.size() &&
                        !ctx.queries_call_stack_[created_physical_plan->query_idx_]->is_corelated_
                  ){
                    SubQueryExecutor* sub_q = nullptr;
                    ALLOCATE_CONSTRUCT(ctx.arena_, sub_q, SubQueryExecutor, &ctx, created_physical_plan);
                    created_physical_plan = sub_q;
                }
                ctx.executors_call_stack_.push_back(created_physical_plan);
            }

            std::cout << "[INFO] executing physical plan" << std::endl;
            return runExecutor(ctx, execution_root);
        }

    private:

        Executor* buildExecutionPlan(QueryCTX& ctx, AlgebraOperation* logical_plan) {
            if(!logical_plan) return nullptr;
            switch(logical_plan->type_) {
                case SORT: 
                    {
                        SortOperation* op = reinterpret_cast<SortOperation*>(logical_plan);
                        Executor* child = buildExecutionPlan(ctx, op->child_);
                        SortExecutor* sort = nullptr;
                        ALLOCATE_CONSTRUCT(ctx.arena_, sort, SortExecutor, &ctx, op, child);
                        return sort;
                    } break;
                case PROJECTION: 
                    {
                        ProjectionOperation* op = reinterpret_cast<ProjectionOperation*>(logical_plan);
                        Executor* child = buildExecutionPlan(ctx, op->child_);

                        ProjectionExecutor* project = nullptr; 
                        ALLOCATE_CONSTRUCT(ctx.arena_, project, ProjectionExecutor, &ctx, op, child);
                        return project;
                    } break;
                case AGGREGATION: 
                    {
                        AggregationOperation* op = reinterpret_cast<AggregationOperation*>(logical_plan);
                        Executor* child = buildExecutionPlan(ctx, op->child_);
                        AggregationExecutor* agg = nullptr;
                        ALLOCATE_CONSTRUCT(ctx.arena_, agg, AggregationExecutor, &ctx, op, child);
                        return agg;
                    } break;
                case FILTER: 
                    {
                        FilterOperation* op = reinterpret_cast<FilterOperation*>(logical_plan);
                        Executor* child = buildExecutionPlan(ctx, op->child_);
                        FilterExecutor* filter = nullptr;
                        ALLOCATE_CONSTRUCT(ctx.arena_, filter, FilterExecutor, &ctx, op, child);
                        return filter;
                    } break;
                case SCAN: 
                    {
                        ScanOperation* op = reinterpret_cast<ScanOperation*>(logical_plan);
                        TableSchema* schema = catalog_->getTableSchema(op->table_name_);
                        String tname =  op->table_name_;
                        if(op->table_rename_.size() != 0) tname = op->table_rename_;
                        Vector<Column> columns = schema->getColumns();
                        // create a new schema and rename columns to table.col_name
                        for(int i = 0; i < columns.size(); i++){
                            String col_name = tname; 
                            col_name += ".";
                            col_name += columns[i].getName();
                            columns[i].setName(col_name);
                        }

                        TableSchema* new_output_schema = New(TableSchema, ctx.arena_,
                                tname, schema->getTable(), columns, true);
                        Executor* scan = nullptr;
                        if(op->scan_type_ == SEQ_SCAN){
                            ALLOCATE_CONSTRUCT(ctx.arena_, scan, SeqScanExecutor, &ctx, op, new_output_schema);
                        } else {
                            ALLOCATE_CONSTRUCT(ctx.arena_, scan, IndexScanExecutor, &ctx, op,
                                    new_output_schema, catalog_->getIndexHeader(op->index_name_));
                        }
                        return scan;
                    } break;
                case PRODUCT: 
                    {
                        ProductOperation* op = reinterpret_cast<ProductOperation*>(logical_plan);
                        Executor* lhs = buildExecutionPlan(ctx, op->lhs_);
                        Executor* rhs = buildExecutionPlan(ctx, op->rhs_);
                        ProductExecutor* product = nullptr;
                        ALLOCATE_CONSTRUCT(ctx.arena_, product, ProductExecutor, &ctx, op, rhs, lhs);
                        return product;
                    } break;
                case JOIN: 
                    {
                        auto op = reinterpret_cast<JoinOperation*>(logical_plan);
                        JoinAlgorithm join_algo = op->join_algo_;
                        Executor* lhs = buildExecutionPlan(ctx, op->lhs_);
                        Executor* rhs = buildExecutionPlan(ctx, op->rhs_);
                        if(join_algo == NESTED_LOOP_JOIN){
                                                        NestedLoopJoinExecutor* join = nullptr;
                            ALLOCATE_CONSTRUCT(ctx.arena_, join, NestedLoopJoinExecutor, &ctx, op, lhs, rhs);
                            return join;

                        } else {
                            HashJoinExecutor* join = nullptr;
                            ALLOCATE_CONSTRUCT(ctx.arena_, join, HashJoinExecutor, &ctx, op, lhs, rhs);
                            return join;
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

                            Executor* lhs = nullptr; 
                            Executor* rhs = nullptr; 

                            if(op->lhs_->query_idx_ == -1)
                                lhs = buildExecutionPlan(ctx, op->lhs_);
                            else
                                lhs = ctx.executors_call_stack_[op->lhs_->query_idx_];

                            if(op->rhs_->query_idx_ == -1)
                                rhs = buildExecutionPlan(ctx, op->rhs_);
                            else
                                rhs = ctx.executors_call_stack_[op->rhs_->query_idx_];
                            // TODO: check that lhs and rhs have the same schema.
                            
                            UnionExecutor* un = nullptr;
                            ALLOCATE_CONSTRUCT(ctx.arena_, un, UnionExecutor, &ctx, op, lhs, rhs);

                            // TODO: don't use an extra distinct executor for the all = false case, 
                            // Use a built-in hashtable inside of the set-operation executor instead.
                            if(op->all_ == false) {
                                DistinctExecutor* dis = nullptr;
                                ALLOCATE_CONSTRUCT(ctx.arena_, dis, DistinctExecutor, &ctx, un);
                                return dis;
                            }
                            return un;
                        } else if(logical_plan->type_ == AL_EXCEPT) {
                            ExceptOperation* op = reinterpret_cast<ExceptOperation*>(logical_plan);
                            if(!op->lhs_ || !op->rhs_) return nullptr;
                            Executor* lhs = nullptr; 
                            Executor* rhs = nullptr; 

                            if(op->lhs_->query_idx_ == -1)
                                lhs = buildExecutionPlan(ctx, op->lhs_);
                            else
                                lhs = ctx.executors_call_stack_[op->lhs_->query_idx_];

                            if(op->rhs_->query_idx_ == -1)
                                rhs = buildExecutionPlan(ctx, op->rhs_);
                            else
                                rhs = ctx.executors_call_stack_[op->rhs_->query_idx_];
                            // TODO: check that lhs and rhs have the same schema.
                            ExceptExecutor* ex = nullptr;
                            ALLOCATE_CONSTRUCT(ctx.arena_, ex, ExceptExecutor, &ctx, op, lhs, rhs);

                            // TODO: don't use an extra distinct executor for the all = false case, 
                            // Use a built-in hashtable inside of the set-operation executor instead.
                            if(op->all_ == false) {
                                DistinctExecutor* dis = nullptr;
                                ALLOCATE_CONSTRUCT(ctx.arena_, dis, DistinctExecutor, &ctx, ex);
                                return dis;
                            }
                            return ex;
                        } else {
                            IntersectOperation* op = reinterpret_cast<IntersectOperation*>(logical_plan);
                            if(!op->lhs_ || !op->rhs_) return nullptr;

                            Executor* lhs = nullptr; 
                            Executor* rhs = nullptr; 

                            if(op->lhs_->query_idx_ == -1)
                                lhs = buildExecutionPlan(ctx, op->lhs_);
                            else
                                lhs = ctx.executors_call_stack_[op->lhs_->query_idx_];

                            if(op->rhs_->query_idx_ == -1)
                                rhs = buildExecutionPlan(ctx, op->rhs_);
                            else
                                rhs = ctx.executors_call_stack_[op->rhs_->query_idx_];
                            // TODO: check that lhs and rhs have the same schema.
                            IntersectExecutor* intersect = nullptr;
                            ALLOCATE_CONSTRUCT(ctx.arena_, intersect, IntersectExecutor, &ctx, op, lhs, rhs);

                            // TODO: don't use an extra distinct executor for the all = false case, 
                            // Use a built-in hashtable inside of the set-operation executor instead.
                            if(op->all_ == false) {
                                DistinctExecutor* dis = nullptr;
                                ALLOCATE_CONSTRUCT(ctx.arena_, dis, DistinctExecutor, &ctx, intersect);
                                return dis;
                            }
                            return intersect;
                        }
                    } break;
                case INSERTION: 
                    {
                        auto statement = 
                            (InsertStatementData*)(ctx.queries_call_stack_[logical_plan->query_idx_]);
                        TableSchema* table = catalog_->getTableSchema(statement->table_name_);
                        int select_idx = statement->select_idx_;

                        InsertionExecutor* insert = nullptr;
                        ALLOCATE_CONSTRUCT(ctx.arena_, insert, InsertionExecutor,
                                &ctx,
                                logical_plan, 
                                table,
                                catalog_->getIndexesOfTable(statement->table_name_),
                                select_idx);
                        return insert;
                    } break;
                case DELETION: 
                    {
                        DeletionOperation* op = reinterpret_cast<DeletionOperation*>(logical_plan);
                        Executor* child = buildExecutionPlan(ctx, op->child_); 
                        auto statement = 
                            (DeleteStatementData*)(ctx.queries_call_stack_[logical_plan->query_idx_]);
                        assert(statement->tables_.size() != 0);
                        TableSchema* table = catalog_->getTableSchema(statement->tables_[0]);

                        DeletionExecutor* deletion = nullptr;
                        ALLOCATE_CONSTRUCT(ctx.arena_, deletion, DeletionExecutor,
                                &ctx,
                                logical_plan, 
                                child,
                                table,
                                catalog_->getIndexesOfTable(statement->tables_[0]));
                        return deletion;
                    } break;
                case UPDATE: 
                    {
                        UpdateOperation* op = reinterpret_cast<UpdateOperation*>(logical_plan);
                        Executor* child = buildExecutionPlan(ctx, op->child_); 
                        auto statement = 
                            (UpdateStatementData*)(ctx.queries_call_stack_[logical_plan->query_idx_]);
                        assert(statement->tables_.size() != 0);
                        TableSchema* table = catalog_->getTableSchema(statement->tables_[0]);

                        UpdateExecutor* update = nullptr;
                        ALLOCATE_CONSTRUCT(ctx.arena_, update, UpdateExecutor,
                                &ctx,
                                logical_plan, 
                                child,
                                table,
                                catalog_->getIndexesOfTable(statement->tables_[0]));
                        return update;
                    } break;
                default: 
                    std::cout << "[ERROR] unsupported Algebra Operaion\n";
                    return nullptr;
            }
        }
        bool runExecutor(QueryCTX& ctx, Executor** execution_root){
            if(ctx.executors_call_stack_.size() < 1 || (bool) ctx.error_status_ ) return false;
            Executor* physical_plan = nullptr;

            // this case means we have set operations: union, except or intersect
            if(ctx.executors_call_stack_.size() > ctx.queries_call_stack_.size()) {
                // first set operation.
                physical_plan = ctx.executors_call_stack_[ctx.queries_call_stack_.size()]; 
            } else {
                physical_plan = ctx.executors_call_stack_[0];
            }

            physical_plan->init();
            *execution_root = physical_plan;
            if(physical_plan->error_status_ || physical_plan->finished_)
                return (physical_plan->error_status_ == 0);

            return (physical_plan->error_status_ == 0);
        }
        Catalog* catalog_;
};
