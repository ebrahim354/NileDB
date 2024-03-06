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
        
        bool execut(ASTNode* statement_root){
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
                std::cout << " out of the loop " << std::endl;
                std::vector<Column> columns;
                uint8_t offset_ptr = 0;
                for(size_t i = 0; i < col_names.size(); ++i){
                    // assume no constraints for now.
                    columns.push_back(Column(col_names[i], col_types[i], offset_ptr));
                    offset_ptr += Column::getSizeFromType(col_types[i]);
                }
                TableSchema* sch = catalog_->createTable(table_name, columns);
                if(sch != nullptr) return true;
            }
            return false;
        }
    private:
        Catalog* catalog_;
};

