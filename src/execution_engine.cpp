#pragma once
#include "catalog.cpp"
#include "parser.cpp"
#include <deque>

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
            std::vector<std::string> fields;
            std::vector<Value> vals;
            while(field_ptr != nullptr && value_ptr != nullptr){
                std::string field_name = field_ptr->field_->token_.val_;
                // check valid column.
                if(!schema->isValidCol(field_name)) 
                    return false;

                std::string val = value_ptr->token_.val_;
                // we consider int and string types for now.
                Type val_type = INVALID;
                if(value_ptr->category_ == STRING_CONSTANT) val_type = VARCHAR;
                else if(value_ptr->category_ == INTEGER_CONSTANT) val_type = INT;
                // invalid or not supported type;
                if( val_type == INVALID ) return false;

                fields.push_back(field_name);
                if(val_type == INT) vals.push_back(Value(stoi(val)));
                else if(val_type == VARCHAR) vals.push_back(Value(val));

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

        int str_to_int(std::string& s){
            if(!s.size()) return 0;
            for(int i = 0; i < s.size(); i++) 
                if (s[i] > '9' || s[i] < '0') 
                    return 0;
            return stoi(s);
        }

        int evaluate_term(ASTNode* term){
            TermNode* t = reinterpret_cast<TermNode*>(term);
            int res = 0;
            if(t->cur_->category_ == EXPRESSION) res = evaluate_expression(t->cur_);
            else                                 res = str_to_int(t->cur_->token_.val_);
            std::string op = t->token_.val_;
            t = t->next_;
            while(t){
                int cur = str_to_int(t->cur_->token_.val_);
                if(op == "*") res *= cur; 
                if(op == "/") {
                    if(cur != 0)
                        res /= cur;
                }
                op = t->token_.val_;
                t = t->next_;
            }
            return res;
        }
        int evaluate_expression(ASTNode* expression) {
            ExpressionNode* ex = reinterpret_cast<ExpressionNode*>(expression);
            int res = evaluate_term(ex->cur_);
            std::string op = ex->token_.val_;
            ex = ex->next_;
            while(ex){
                int cur = evaluate_term(ex->cur_);
                if(op == "+") res += cur; 
                if(op == "-") res -= cur;
                op = ex->token_.val_;
                ex = ex->next_;
            }
            return res;
        }

        bool select_handler(ASTNode* statement_root){
            SelectStatementNode* select = reinterpret_cast<SelectStatementNode*>(statement_root);
            // nothing to be selected.
            if(select->fields_ == nullptr) return false;
            bool fields_need_table = false;
            auto field_ptr = select->fields_;
            std::vector<std::string> fields;
            std::vector<SelectListNode*> field_ptrs;
            while(field_ptr != nullptr){
                std::string field_name = field_ptr->field_->token_.val_;
                TokenType field_type = field_ptr->field_->token_.type_;
                //std::cout << field_name << " " << field_type << std::endl;
                if(field_type == IDENTIFIER) fields_need_table = true;

                fields.push_back(field_name);
                field_ptrs.push_back(field_ptr);
                field_ptr = field_ptr->next_;
            }

            // if they don't need a table evaluate them and send them back.
            // TODO: it is better to create an in memory table or a schema-less global table that goes with the flow.
            if(!fields_need_table){
                for(int i = 0; i < field_ptrs.size(); i++) {
                    auto field_ptr = field_ptrs[i];
                    std::string field_name = field_ptr->field_->cur_->token_.val_;
                    TokenType field_type = field_ptr->field_->token_.type_;
                    std::cout << evaluate_expression(field_ptr->field_) << std::endl;
                }
                return true;
            }
            
            // handle filters later.
            // handle joins later ( only select values from the first table on the join list ).
            auto table_ptr = select->tables_; 
            // did not find any tables.
            if(table_ptr == nullptr) return false;
            std::string table_name = table_ptr->token_.val_;
            TableSchema* schema = catalog_->getTableSchema(table_name);


            // handle duplicate fields later.
            for(int i = 0; i < fields.size(); i++){
                std::string field_name = fields[i]; 
                // check valid column.
                if(!schema->isValidCol(field_name)) 
                    return false;
            }
            
            TableIterator* it = schema->getTable()->begin();
            // print the schema at the top of the table
            schema->printTableHeader();
            while(it->advance()){
                Record r = it->getCurRecordCpy();
                std::vector<Value> values;
                int err = schema->translateToValues(r, values);
                if(err) return false;
                // filter the output based on the fields vector later.
                // our output is just printing for now.
                for(size_t i = 0; i < values.size(); ++i){
                    if(values[i].type_ == INT) 
                        std::cout << values[i].getIntVal();
                    else 
                        std::cout << values[i].getStringVal();

                    if(i < values.size() - 1 ) std::cout << " | ";
                }
                std::cout << std::endl;
            }
            return true;
        }

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


        bool execute(ASTNode* statement_root){
            if(!statement_root) return false;

            switch (statement_root->category_) {
                case CREATE_TABLE_STATEMENT:
                    return create_table_handler(statement_root);
                case INSERT_STATEMENT:
                    return insert_handler(statement_root);
                case SELECT_STATEMENT:
                    return select_handler(statement_root);
                case DELETE_STATEMENT:
                    return delete_handler(statement_root);
                case UPDATE_STATEMENT:
                    return update_handler(statement_root);
                default:
                    return false;
            }
        }
    private:
        Catalog* catalog_;
};
