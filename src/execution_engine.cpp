#pragma once
#include "catalog.cpp"
#include "parser.cpp"
#include "expression.cpp"
#include "algebra_engine.cpp"
#include "utils.cpp"
#include <deque>


typedef std::vector<std::vector<Value>> QueryResult;


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



enum ExecutorType {
    SEQUENTIAL_SCAN_EXECUTOR,
    FILTER_EXECUTOR,
    AGGREGATION_EXECUTOR,
    PROJECTION_EXECUTOR,
    SORT_EXECUTOR,
};

struct Executor {
    public:
        Executor(ExecutorType type, TableSchema* output_schema): 
            type_(type), output_schema_(output_schema)
        {}
        ~Executor() {}
        virtual void init() = 0;
        virtual std::vector<Value> next() = 0;

        ExecutorType type_;
        TableSchema* output_schema_ = nullptr;
        bool error_status_ = 0;
        bool finished_ = 0;
};

class SeqScanExecutor : public Executor {
    public:
        SeqScanExecutor(TableSchema* table)
            : Executor(SEQUENTIAL_SCAN_EXECUTOR, table), table_(table)
        {}
        ~SeqScanExecutor()
        {}

        void init() {
            it_ = table_->getTable()->begin();
        }

        std::vector<Value> next() {
            // no more records.
            if(!it_->advance()) {
                finished_ = 1;
                return {};
            };
            Record r = it_->getCurRecordCpy();
            std::vector<Value> output;
            int err = table_->translateToValues(r, output);
            if(err) {
                error_status_ = 1;
                return {};
            }
            return output;
        }
    private:
        TableSchema* table_ = nullptr;
        TableIterator* it_ = nullptr;
};


class FilterExecutor : public Executor {
    public:
        FilterExecutor(Executor* child, TableSchema* output_schema, ExpressionNode* filter, 
                std::vector<ExpressionNode*>& fields, 
                std::vector<std::string>& field_names)
            : Executor(FILTER_EXECUTOR, output_schema), child_executor_(child), filter_(filter), 
              fields_(fields), field_names_(field_names)
        {}
        ~FilterExecutor()
        {}

        Value evaluate(ASTNode* item){
            if(item->category_ == FIELD) {
                std::string field = item->token_.val_;
                int idx = -1; 
                if(output_schema_) {
                    idx = output_schema_->colExist(field);
                }
                if(idx < 0){
                    for(int i = 0; i < field_names_.size(); i++)
                        if(field == field_names_[i]) 
                            return evaluate_expression(fields_[i], eval);
                    
                }
                if(idx < 0 || idx >= output_.size()) {
                    // TODO: check that in the Algebra Engine not here.
                    std::cout << "[ERROR] Invalid field name " << field << std::endl;
                    error_status_ = 1;
                    return Value();
                }
                return output_[idx];
            }
            std::cout << "[ERROR] Item type is not supported!" << std::endl;
            error_status_ = 1;
            return Value();
        } 

        void init() {
            if(child_executor_) {
                child_executor_->init();
            }
        }


        std::vector<Value> next() {
            while(true){
                if(error_status_ || finished_)  return {};
                if(child_executor_) {
                    output_ = child_executor_->next();
                    error_status_ = child_executor_->error_status_;
                    finished_ = child_executor_->finished_;
                } else {
                    finished_ = true;
                }

                Value exp = evaluate_expression(filter_, eval).getBoolVal();
                if(exp != false && !exp.isNull()){
                    if(child_executor_)
                        return output_;
                    return {exp};
                }
            }
        }
    private:
        Executor* child_executor_ = nullptr;
        ExpressionNode* filter_ = nullptr;
        std::vector<ExpressionNode*> fields_ = {};
        std::vector<std::string> field_names_ = {};
        std::vector<Value> output_; 
        std::function<Value(ASTNode*)>
            eval = std::bind(&FilterExecutor::evaluate, this, std::placeholders::_1);
};

class AggregationExecutor : public Executor {
    public:
        AggregationExecutor(Executor* child_executor, TableSchema* output_schema, std::vector<AggregateFuncNode*> aggregates): 
            Executor(AGGREGATION_EXECUTOR, output_schema), child_executor_(child_executor), aggregates_(aggregates)
        {}
        ~AggregationExecutor()
        {}

        Value evaluate(ASTNode* item){
            if(item->category_ == FIELD) {
                std::string field = item->token_.val_;
                if(!output_schema_) {
                    std::cout << "[ERROR] Invalid field name " << field << std::endl;
                    return Value();
                }
                int idx = output_schema_->colExist(field);
                if(idx < 0 || idx >= output_.size()) {
                    // TODO: check that in the Algebra Engine not here.
                    std::cout << "[ERROR] Invalid field name " << field << std::endl;
                    return Value();
                }
                return output_[idx];
            }
            std::cout << "[ERROR] Item type is not supported!" << std::endl;
            return Value();
        } 

        void init() {
            child_executor_->init();
            std::vector<int> aggregate_vals(aggregates_.size(), 0);
            bool first_iteration = true;
            int num_of_rows = 0;

            while(true){
                std::vector<Value> child_output; 
                child_output = child_executor_->next();
                error_status_ = child_executor_->error_status_;
                if(error_status_)  return;
                if(child_executor_->finished_) break;

                num_of_rows++;
                if(first_iteration){
                    for(auto& val : child_output){
                        output_.push_back(val);
                    }
                }

                for(int i = 0; i < aggregates_.size(); i++){
                    ExpressionNode* exp = aggregates_[i]->exp_;
                    switch(aggregates_[i]->type_){
                        case COUNT:
                                    {
                                        if(exp == nullptr){
                                            aggregate_vals[i]++;
                                            break;
                                        }
                                        Value val = evaluate_expression(exp, eval);
                                        if(!val.isNull()) aggregate_vals[i]++;
                                    }
                                    break;
                        case AVG:
                        case SUM:
                                   {
                                       Value val = evaluate_expression(exp, eval);
                                       if(val.type_ == INT) aggregate_vals[i] += val.getIntVal();
                                   }
                                   break;
                        case MIN:
                                   {
                                       if(first_iteration) aggregate_vals[i] = std::numeric_limits<int>::max();
                                       Value val = evaluate_expression(exp, eval);
                                       if(val.type_ == INT) aggregate_vals[i] = 
                                           std::min<int>(aggregate_vals[i], val.getIntVal());
                                   }
                                   break;
                        case MAX:
                                   {
                                       if(first_iteration) aggregate_vals[i] = std::numeric_limits<int>::min();
                                       Value val = evaluate_expression(exp, eval);
                                       if(val.type_ == INT) aggregate_vals[i] = 
                                           std::max<int>(aggregate_vals[i], val.getIntVal());
                                   }
                                   break;
                        default :
                            break;
                    }
                }
                first_iteration = false;

            }

            for(int i = 0; i < aggregates_.size(); i++){
                output_.push_back(Value((aggregates_[i]->type_ == AVG  && num_of_rows != 0 ? (aggregate_vals[i] / num_of_rows) : aggregate_vals[i])));
            }
        }

        std::vector<Value> next() {
            if(error_status_ || finished_)  return {};
            finished_ = true;
            return output_;
        }
    private:
        Executor* child_executor_ = nullptr;
        std::vector<AggregateFuncNode*> aggregates_;
        std::vector<Value> output_;
        std::function<Value(ASTNode*)>
            eval = std::bind(&AggregationExecutor::evaluate, this, std::placeholders::_1);
};

class ProjectionExecutor : public Executor {
    public:
        ProjectionExecutor(Executor* child_executor, TableSchema* output_schema, std::vector<ExpressionNode*> fields): 
            Executor(PROJECTION_EXECUTOR, output_schema), child_executor_(child_executor), fields_(fields)
        {}
        ~ProjectionExecutor()
        {}

        Value evaluate(ASTNode* item){
            if(item->category_ == FIELD) {
                std::string field = item->token_.val_;
                if(!output_schema_) {
                    std::cout << "[ERROR] Invalid field name " << field << std::endl;
                    return Value();
                }
                int idx = output_schema_->colExist(field);
                if(idx < 0 || idx >= output_.size()) {
                    // TODO: check that in the Algebra Engine not here.
                    std::cout << "[ERROR] Invalid field name " << field << std::endl;
                    return Value();
                }
                return output_[idx];
            }
            std::cout << "[ERROR] Item type is not supported!" << std::endl;
            return Value();
        } 

        void init() {
            if(child_executor_) {
                child_executor_->init();
            }
        }

        std::vector<Value> next() {
            if(error_status_ || finished_)  return {};


            if(child_executor_){
                output_ = child_executor_->next();
                error_status_ = child_executor_->error_status_;
                finished_ = child_executor_->finished_;
                std::cout << "has a child ";
            } else {
                std::cout << "has no child ";
                finished_ = true;
            }

            std::vector<Value> tmp_output;
            if(child_executor_ && ((finished_ || error_status_) && output_.size() == 0)) return {};

            for(int i = 0; i < fields_.size(); i++){
                if(fields_[i] == nullptr){
                    for(auto& val : output_){
                        tmp_output.push_back(val);
                    }
                } else {
                    tmp_output.push_back(evaluate_expression(fields_[i], eval));
                }
            }
            return tmp_output;
        }
    private:
        // child_executor_ is optional in case of projection for example : select 1 + 1 should work without a from clause.
        Executor* child_executor_ = nullptr;
        std::vector<ExpressionNode*> fields_;
        std::vector<Value> output_;
        std::function<Value(ASTNode*)>
            eval = std::bind(&ProjectionExecutor::evaluate, this, std::placeholders::_1);
};

class SortExecutor : public Executor {
    public:
        SortExecutor(Executor* child_executor , TableSchema* output_schema, std::vector<int> order_by_list): 
            Executor(SORT_EXECUTOR, output_schema), child_executor_(child_executor), order_by_list_(order_by_list)
        {}
        ~SortExecutor()
        {}

        void init() {
            if(idx_ != 0) {
                idx_ = 0;
                return;
            }
            child_executor_->init();

            while(true){
                std::vector<Value> tuple; 
                tuple = child_executor_->next();
                error_status_ = child_executor_->error_status_;
                if(error_status_)  return;
                if(tuple.size())
                    tuples_.push_back(tuple);
                if(child_executor_->finished_) break;
            }

            std::vector<int> order_by = order_by_list_;

            std::sort(tuples_.begin(), tuples_.end(), 
                    [&order_by](std::vector<Value>& lhs, std::vector<Value>& rhs){
                        for(int i = 0; i < order_by.size(); i++){
                            if(lhs[order_by[i]] < rhs[order_by[i]]) return true;
                            if(lhs[order_by[i]] == rhs[order_by[i]]) continue;
                        }
                        return false;
                    });
        }

        std::vector<Value> next() {
            finished_ = (idx_ >= tuples_.size());
            if(error_status_ || finished_)  return {};
            return tuples_[idx_++];
        }
    private:
        Executor* child_executor_ = nullptr;
        std::vector<int> order_by_list_;
        std::vector<std::vector<Value>> tuples_;
        int idx_ = 0;
};



class ExecutionEngine {
    public:
        ExecutionEngine(Catalog* catalog): catalog_(catalog)
    {}
        ~ExecutionEngine() {}

        bool create_table_handler(ASTNode* statement_root) {
            CreateTableStatementNode* create_table = reinterpret_cast<CreateTableStatementNode*>(statement_root);
            std::string table_name = create_table->table_->token_.val_;
            auto fields = create_table->field_defs_;
            std::deque<std::string> col_names;
            std::deque<Type> col_types;
            while(fields != nullptr){
                // a little too much nesting (fix that later).
                std::string name = fields->field_def_->field_->token_.val_;
                Type type = catalog_->stringToType(fields->field_def_->type_->token_.val_);
                if(type == INVALID) return false;
                // variable columns first;
                if(type == VARCHAR) {
                    col_names.push_front(name);
                    col_types.push_front(type);
                } else {
                    col_names.push_back(name);
                    col_types.push_back(type);
                }
                fields = fields->next_;
            }
            std::vector<Column> columns;
            uint8_t offset_ptr = 0;
            for(size_t i = 0; i < col_names.size(); ++i){
                // assume no constraints for now.
                columns.push_back(Column(col_names[i], col_types[i], offset_ptr));
                offset_ptr += Column::getSizeFromType(col_types[i]);
            }
            TableSchema* sch = catalog_->createTable(table_name, columns);
            if(sch != nullptr) return true;
            return false;
        }

        bool insert_handler(ASTNode* statement_root){
            InsertStatementNode* insert = reinterpret_cast<InsertStatementNode*>(statement_root);
            // We are going to assume that there are no default values for now.
            // We also assume that the order of fields matches the schema.

            std::string table_name = insert->table_->token_.val_;
            TableSchema* schema = catalog_->getTableSchema(table_name);
            auto field_ptr = insert->fields_;
            auto value_ptr = insert->values_;
            std::vector<std::string> fields(schema->numOfCols());
            std::vector<Value> vals(schema->numOfCols());
            while(field_ptr != nullptr && value_ptr != nullptr){
                std::string field_name = field_ptr->field_->token_.val_;
                // check valid column.
                if(!schema->isValidCol(field_name)) 
                    return false;
                int idx = schema->colExist(field_name);

                std::string val = value_ptr->token_.val_;
                // we consider int and string types for now.
                Type val_type = INVALID;
                if(value_ptr->category_ == STRING_CONSTANT) val_type = VARCHAR;
                else if(value_ptr->category_ == INTEGER_CONSTANT) val_type = INT;
                // invalid or not supported type;
                if( val_type == INVALID ) return false;

                if(idx > fields.size() || idx  < 0) return false;
                fields[idx] = field_name;
                if(val_type == INT) vals[idx] = (Value(stoi(val)));
                else if(val_type == VARCHAR) vals[idx] = (Value(val));

                field_ptr = field_ptr->next_;
                value_ptr = value_ptr->next_;
            }
            // size of values do not match the size of the fields.
            if(value_ptr || field_ptr) {
                std::cout << "[ERROR] Size of values does not match size of fields" << std::endl;
                return false;
            }
            // invalid field list or val list.
            if(!schema->checkValidValues(fields, vals)) {
                std::cout << "[ERROR] Invalid field list or val list" << std::endl;
                return false;
            }

            Record* record = schema->translateToRecord(vals);
            // rid is not used for now.
            RecordID* rid = new RecordID();
            int err = schema->getTable()->insertRecord(rid, *record);
            if(!err) return true;
            return false;
        }


        /*
        bool select_handler(ASTNode* statement_root, QueryResult* result){
            if(!result) return false;
            SelectStatementNode* select = reinterpret_cast<SelectStatementNode*>(statement_root);
            // nothing to be selected.
            if(select->fields_ == nullptr) return false;
            SelectExecutor* executor = new SelectExecutor(select, catalog_);

            bool valid_order_by = true;
            std::vector<int> order_by;

            auto order_by_ptr = select->order_by_list_;
            while(order_by_ptr){
                std::string val = order_by_ptr->token_.val_;
                int cur = str_to_int(val);
                if(cur == 0) {
                    valid_order_by = false;
                    break;
                }
                order_by.push_back(cur-1); 
                order_by_ptr = order_by_ptr->next_;
            }
            while(!executor->errorStatus() && !executor->finished()){
                std::vector<std::string> tmp = executor->next();
                if(tmp.size() == 0) break;
                if(order_by.size() > tmp.size()) {
                    std::cout << "[ERROR] order by list should be between 1 and " <<  tmp.size() << std::endl;
                    return false;
                }
                    
                for(int i = 0; i < tmp.size(); i++){
                    std::cout << tmp[i] << std::endl;
                }
                result->push_back(tmp);
            }
            if(valid_order_by && executor->errorStatus() == 0) {
                std::sort(result->begin(), result->end(), 
                    [&order_by](std::vector<std::string>& lhs, std::vector<std::string>& rhs){
                        for(int i = 0; i < order_by.size(); i++){
                            if(lhs[order_by[i]] < rhs[order_by[i]]) return true;
                            if(lhs[order_by[i]] == rhs[order_by[i]]) continue;
                        }
                        return false;
                    });
            }

            return (executor->errorStatus() == 0);
        }*/

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

        bool execute(ASTNode* statement_root, QueryResult* result){
            if(!statement_root) return false;

            switch (statement_root->category_) {
                case CREATE_TABLE_STATEMENT:
                    return create_table_handler(statement_root);
                case INSERT_STATEMENT:
                    return insert_handler(statement_root);
                //case SELECT_STATEMENT:
                 //   return select_handler(statement_root, result);
                case DELETE_STATEMENT:
                    return delete_handler(statement_root);
                case UPDATE_STATEMENT:
                    return update_handler(statement_root);
                default:
                    return false;
            }
        }
        Executor* buildExecutionPlan(AlgebraOperation* logical_plan) {
            if(!logical_plan) return nullptr;
            switch(logical_plan->type_) {
                case SORT: {
                               SortOperation* op = reinterpret_cast<SortOperation*>(logical_plan);
                               Executor* child = buildExecutionPlan(op->child_);
                               SortExecutor* sort = new SortExecutor(child, child->output_schema_, op->order_by_list_);
                               return sort;
                           }
                case PROJECTION: {
                                    ProjectionOperation* op = reinterpret_cast<ProjectionOperation*>(logical_plan);
                                    Executor* child = buildExecutionPlan(op->child_);
                            
                                    ProjectionExecutor* project = nullptr; 
                                    if(!child)
                                        project = new ProjectionExecutor(child, nullptr, op->fields_);
                                    else
                                        project = new ProjectionExecutor(child, child->output_schema_, op->fields_);
                                    return project;
                                 }
                case AGGREGATION: {
                                    AggregationOperation* op = reinterpret_cast<AggregationOperation*>(logical_plan);
                                    Executor* child = buildExecutionPlan(op->child_);
                            
                                    std::vector<Column> child_cols = child->output_schema_->getColumns();

                                    int offset_ptr = Column::getSizeFromType(child_cols[child_cols.size() - 1].getType());
                                    for(int i = 0; i < op->aggregates_.size(); i++){
                                        std::string col_name = AGG_FUNC_IDENTIFIER_PREFIX;
                                        col_name += intToStr(op->aggregates_[i]->parent_id_);
                                        child_cols.push_back(Column(col_name, INT, offset_ptr));
                                        offset_ptr += Column::getSizeFromType(INT);
                                    }

                                    TableSchema* new_output_schema = new TableSchema("agg_tmp_schema", nullptr, child_cols);
                                    //new_output_schema->printTableHeader();
                                    return new AggregationExecutor(child, new_output_schema, op->aggregates_);
                                 }
                case FILTER: {
                                 FilterOperation* op = reinterpret_cast<FilterOperation*>(logical_plan);
                                 Executor* child = buildExecutionPlan(op->child_);
                                 TableSchema* schema = nullptr;
                                 if(child == nullptr) schema = nullptr;
                                 else                 schema = child->output_schema_;
                                 
                                 FilterExecutor* filter = new FilterExecutor(child, schema, op->filter_, op->fields_, op->field_names_);
                                 return filter;
                             }

                case SCAN: {
                               ScanOperation* op = reinterpret_cast<ScanOperation*>(logical_plan);
                               TableSchema* schema = catalog_->getTableSchema(op->table_name_);
                               SeqScanExecutor* scan = new SeqScanExecutor(schema);
                               return scan;
                           }
                default: 
                           std::cout << "[ERROR] unsupported Algebra Operaion\n";
                           return nullptr;
            }
        }

        bool executePlan(AlgebraOperation* logical_plan, QueryResult* result){
            if(!logical_plan) return false;
            if(!result) return false;
            std::cout << "[INFO] Creating physical plan" << std::endl;
            Executor* physical_plan = buildExecutionPlan(logical_plan);
            if(!physical_plan){
                std::cout << "[ERROR] Could not build physical operation\n";
            }

            std::cout << "[INFO] executing physical plan" << std::endl;
            physical_plan->init();

            while(!physical_plan->error_status_ && !physical_plan->finished_){
                std::vector<Value> tmp = physical_plan->next();
                if(tmp.size() == 0 || physical_plan->error_status_) break;
                    
                result->push_back(tmp);
            }
            return (physical_plan->error_status_ == 0);
        }
    private:
        Catalog* catalog_;
};
