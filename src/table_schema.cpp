#pragma once
#include "column.cpp"
#include "table.cpp"



class TableSchema {
    public:
        TableSchema(std::string& name, Table* table,const std::vector<Column>& columns)
            : table_name_(name),
              table_(table),
              columns_(columns)
        {}

        // Get a pointer to a spicific value inside of a record based on the schema.
        // Type conversion is done by the user of the function.
        // Return nullptr in case of an error or the value is equal to null (handle cases separately later).
        // Maybe return a valid pointer to an invalid value (create an invalid constant for each type and use it),
        // and add an isNull method to the Record class.
        char* getValue(std::string& col_name ,Record& r, uint16_t* size){
            Column* col = nullptr;
            // Not very effecient.
            for(size_t i = 0; i < columns_.size(); ++i){
                if(columns_[i].getName() == col_name) {
                    col = &columns_[i];
                    break;
                }
            }
            // invalid column name.
            if(!col) return nullptr;

            char* val = nullptr;
            if(col->isVarLength()){
                val = r.getVariablePtr(col->getOffset(), size);
            } else {
                val = r.getFixedPtr(col->getOffset());
                *size = col->getSize();
            }
            // value is null or the column offset is invalid.
            // (the difference need to be handled). 
            return val;
        }

    private:
        std::vector<Column> columns_;
        std::string table_name_;
        Table* table_;
};

