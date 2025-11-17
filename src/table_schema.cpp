#pragma once
#include "column.cpp"
#include "table.cpp"



class TableSchema {
    public:
        TableSchema(std::string name, Table* table,const std::vector<Column> columns,bool tmp_schema = false)
            : table_name_(name),
              table_(table),
              columns_(columns),
              tmp_schema_(tmp_schema) 
        {
            size_ = 0;
            for(auto c : columns){
                size_ += c.getSize();
            }
        }
        ~TableSchema() {
            if(!tmp_schema_)
                delete table_; 
        }

        std::string getTableName(){
            return table_name_;
        }

        int numOfCols() {
            return columns_.size();
        }
        // TODO: change columns to be a set instead of doing this.
        int colExist(std::string& col_name) {
            std::string field = col_name;
            if(!tmp_schema_)
                field = split_scoped_field(col_name).second;

            for(size_t i = 0; i < columns_.size(); ++i){
                if(columns_[i].getName() == field)
                    return i;
            }
            return -1;
        }

        bool checkValidValues(std::vector<std::string>& fields, std::vector<Value>& vals) {
            if(fields.size() != columns_.size() || fields.size() != vals.size()) return false;
            for(size_t i = 0; i < fields.size(); ++i){
                int col_idx = colExist(fields[i]);
                if(col_idx == -1) return false;
                if(columns_[col_idx].getName() != fields[i] || columns_[col_idx].getType() != vals[i].type_)
                    return false;
            }
            return true;
        }

        bool checkValidValue(std::string& field, Value& val) {
            for(size_t i = 0; i < columns_.size(); ++i){
                if(columns_[i].getName() == field && columns_[i].getType() != val.type_)
                    return true;
            }
            return false;
        }
        
        int getColIdx(std::string& field, Value& val){
            for(size_t i = 0; i < columns_.size(); ++i){
                if(columns_[i].getName() == field && columns_[i].getType() != val.type_){
                    return i;
                }
            }
            return -1;

        }

        bool isValidCol(std::string& col_name){
            for(auto c : columns_)
                if(c.getName() == col_name) return true;
            return false;
        }

        void addColumn(Column c){
            size_ += c.getSize();
            columns_.push_back(c);
            std::sort(columns_.begin(), columns_.end(), [](Column& a, Column& b){
                return a.getOffset() < b.getOffset();
            });
        }

        std::string typeToString(Type t){
            if(t == BOOLEAN)        return "BOOLEAN";
            else if(t == INT)       return "INT";
            else if(t == BIGINT)    return "BIGINT";
            else if(t == FLOAT)     return "FLOAT";
            else if(t == DOUBLE)    return "DOUBLE";
            else if(t == TIMESTAMP) return "TIMESTAMP";
            else if(t == VARCHAR)   return "VARCHAR";
            return "INVALID";
        }

        void printSchema(std::stringstream& ss){
            ss << " number of columns : " <<  columns_.size() << std::endl;
            for(int i = 0; i < columns_.size(); i++){
                ss << "col num : " << i << std::endl;
                ss << "name: " << columns_[i].getName() << " offset: " << columns_[i].getOffset() << std::endl;
                ss << "type: " << typeToString(columns_[i].getType()) << std::endl;
                ss << "constraints\n";
                ss << "primary_key: " << columns_[i].isPrimaryKey() 
                          << " foreign_key: " << columns_[i].isForeignKey();
                ss << " nullable: " << columns_[i].isNullable() << " unique: " 
                          << columns_[i].isUnique() << std::endl;
                ss << "-----------------------------------------------------------------" << std::endl;
            }
        }

        void printSchema(){
            std::cout << " number of columns : " <<  columns_.size() << std::endl;
            for(int i = 0; i < columns_.size(); i++){
                std::cout << "col num : " << i << std::endl;
                std::cout << "name: " << columns_[i].getName() << " offset: " << columns_[i].getOffset() << std::endl;
                std::cout << "type: " << typeToString(columns_[i].getType()) << std::endl;
                std::cout << "constraints\n";
                std::cout << "primary_key: " << columns_[i].isPrimaryKey() 
                          << " foreign_key: " << columns_[i].isForeignKey();
                std::cout << " nullable: " << columns_[i].isNullable() << " unique: " 
                          << columns_[i].isUnique() << std::endl;
                std::cout << "-----------------------------------------------------------------" << std::endl;
            }
        }
        std::vector<std::string> getCols(){
            std::vector<std::string> cols;
            for(size_t i = 0; i < columns_.size(); ++i){
                 cols.push_back(columns_[i].getName());
            }
            return cols;
        }

        std::vector<Column> getColumns(){
            return columns_;
        }

        void printTableHeader(){
            for(size_t i = 0; i < columns_.size(); ++i){
                std::cout << columns_[i].getName();
                if(i != columns_.size() - 1) std::cout << " | ";
            }
            std::cout << "\n-----------------------------------------------------------------" << std::endl;
        }
        // get a pointer to a spicific value inside of a record using the schema. 
        // Type conversion is done by the user of the function.
        // return nullptr in case of an error or the value is equal to null (handle cases separately later).
        char* getValue(std::string col_name ,Record& r, uint16_t* size){
            Column* col = nullptr;
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
            // (the difference needs to be handled). 
            return val;
        }

        // translate a given record using the schema to a vector of Value type.
        // return 1 in case of an error.
        // values is the output.
        int translateToValues(Record& r, std::vector<Value>& values){
            for(int i = 0; i < columns_.size(); ++i){
                // check the bitmap if this value is null.
                char * bitmap_ptr = r.getFixedPtr(size_)+(i/8);  
                int is_null = *bitmap_ptr & (1 << (i%8));
                if(is_null) {
                  values.emplace_back(Value(NULL_TYPE));
                  continue;
                }
                Value val{};
                char* content = getValue(columns_[i].getName(), r, &val.size_);
                if(!content)
                  return 1;
                val.value_from_size(val.size_);
                memcpy(val.content_, content, val.size_);
                val.type_ = columns_[i].getType();
                values.emplace_back(val);
            }
            return 0;
        }

        int translateToValuesOffset(Record& r, std::vector<Value>& values, int offset){
            if( offset < 0 || offset + columns_.size() > values.size()) return 1;
            for(int i = 0; i < columns_.size(); ++i){
                // check the bitmap if this value is null.
                char * bitmap_ptr = r.getFixedPtr(size_)+(i/8);  
                int is_null = *bitmap_ptr & (1 << (i%8));
                if(is_null) {
                  values[offset+i] = Value(NULL_TYPE);
                  continue;
                }
                Value* val = &values[offset+i];
                char* content = getValue(columns_[i].getName(), r, &val->size_);
                if(!content)
                  return 1;
                val->value_from_size(val->size_);
                memcpy(val->content_, content, val->size_);
                val->type_ = columns_[i].getType();
            }
            return 0;
        }

        // translate a vector of values using the schema to a Record. 
        // return null in case of an error.
        // the user of the class should handle deleting the record after using it.
        // we assume that the variable length columns are represented first.
        Record* translateToRecord(std::vector<Value>& values){
            if(values.size() != columns_.size()) return nullptr;
            uint32_t fixed_part_size = size_;
            uint32_t var_part_size = 0;
            for(size_t i = 0; i < columns_.size(); ++i){
                if(columns_[i].isVarLength()) var_part_size += values[i].size_;
            }
            // The bitmap bytes.
            fixed_part_size += (columns_.size() / 8) + (columns_.size() % 8);
            char* data = new char[fixed_part_size + var_part_size];
            std::memset(data, 0, fixed_part_size + var_part_size);
            
            uint16_t cur_var_offset = fixed_part_size; 
            for(size_t i = 0; i < values.size(); i++){
                if(columns_[i].isVarLength()){
                    // add 2 bytes for offset and 2 bytes for length
                    memcpy(data + columns_[i].getOffset(), &cur_var_offset, sizeof(cur_var_offset));
                    memcpy(data + columns_[i].getOffset() + 2, &values[i].size_, sizeof(cur_var_offset));
                    // cpy the actual data and update the var offset pointer.
                    memcpy(data + cur_var_offset, values[i].content_, values[i].size_);
                    cur_var_offset+= values[i].size_;
                } else {
                    // just copy the data into its fixed offset.
                    memcpy(data + columns_[i].getOffset(), values[i].content_, values[i].size_);
                }
                // initialize the bitmap, 1 means null and 0 means not null.
                if(values[i].isNull()){
                  char * bitmap_ptr = data+size_+(i/8);  
                  *bitmap_ptr = *bitmap_ptr | (1 << (i%8));
                }
            }

            auto r = new Record(data, fixed_part_size + var_part_size, false); // false => record owns the data ptr now.
            return r;
        }
        Table* getTable(){
            return table_;
        }
    private:
        bool tmp_schema_ = false;
        std::string table_name_;
        Table* table_;
        std::vector<Column> columns_;
        uint32_t size_;
};

