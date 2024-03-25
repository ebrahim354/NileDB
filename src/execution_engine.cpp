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

        bool execute(ASTNode* statement_root){
            if(!statement_root) return false;
            // just hard coded handlers for now.
            if(statement_root->category_ == CREATE_TABLE_STATEMENT){
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
            } else if( statement_root->category_ == INSERT_STATEMENT ){
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
                if(value_ptr || field_ptr) return false;
                // invalid field list or val list.
                if(!schema->checkValidValues(fields, vals)) return false;

                Record* record = schema->translateToRecord(vals);
                // rid is not used for now.
                RecordID* rid = new RecordID();
                int err = schema->getTable()->insertRecord(rid, *record);
                if(!err) return true;
            } else if( statement_root->category_ == SELECT_STATEMENT ){
                SelectStatementNode* select = reinterpret_cast<SelectStatementNode*>(statement_root);
                // handle filters later.
                // handle joins later ( only select values from the first table on the list ).

                auto table_ptr = select->tables_; 
                // did not find any tables.
                if(table_ptr == nullptr) return false;
                std::string table_name = table_ptr->token_.val_;
                TableSchema* schema = catalog_->getTableSchema(table_name);

                auto field_ptr = select->fields_;
                std::vector<std::string> fields;
                while(field_ptr != nullptr){
                    std::string field_name = field_ptr->field_->token_.val_;
                    // check valid column.
                    if(!schema->isValidCol(field_name)) 
                        return false;

                    fields.push_back(field_name);
                    field_ptr = field_ptr->next_;
                }
                // handle duplicate fields later.

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
            return false;
        }
    private:
        Catalog* catalog_;
};
