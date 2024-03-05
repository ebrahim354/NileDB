#pragma once
#include "cache_manager.cpp"
#include "table.cpp"
#include "column.cpp"
#include "seq_scan.cpp"
#include <algorithm>
#include <cstdint>
#include <cstring>
#include <vector>


/* The system catalog is just a special table with hard coded schema and table name,
 * It is loaded from disk once at booting process of the system.
 * The special table name is :  NILEDB_META_DATA.
 * The schema of the table is as follows: 
 * table_name (var char), col_name (var char), col_type (integer), col_offset (integer), 4 boolean type columns.
 * col_position is indicating the index of the column within the columns_ vector of the schema.
 * The 4 columns are some sort of a bitmap to store weather this column has the constraints of this index or not, 
 * For example: 1 0 1 0 means this column has constraints:  NULLABLE, not a PRIMARY KEY, FOREIGHT KEY, not UNIQUE.
 * this approach is not very performant or memory effecient, 
 * But it doesn't matter tables are only loaded once and rarely updated or deleted.
 * The name of the file that stores a specific table is not needed because the catalog assumes that the name of the
 * file is just : the name of the table .ndb.
 * This meta data system should be suffecient for now.
 */

#define META_DATA_FILE "NILEDB_META_DATA.ndb"
#define META_DATA_TABLE "NILEDB_META_DATA"


class Value {
    public:
        char* content_; 
        uint16_t size_;
        Type type_;
        bool my_allocation = true;
        Value(){
            my_allocation = false;
        } 
        ~Value(){
            if(my_allocation) delete content_;
        }
        // constuctors for different value types.
        Value(std::string str){
            size_ = str.size(); 
            content_ = new char[size_];
            memcpy(content_, str.c_str(), size_);
            type_ = VARCHAR;
        }
        Value(bool val){
            size_ = 1; 
            content_ = new char[size_];
            memcpy(content_, &val, size_);
            type_ = BOOLEAN;
        }
        Value(int val){
            size_ = 4; 
            content_ = new char[size_];
            memcpy(content_, &val, size_);
            type_ = INT;
        }
        Value(long long val){
            size_ = 8; 
            content_ = new char[size_];
            memcpy(content_, &val, size_);
            type_ = BIGINT;
        }
        Value(float val){
            size_ = 4; 
            content_ = new char[size_];
            memcpy(content_, &val, size_);
            type_ = FLOAT;
        }
        Value(double val){
            size_ = 8; 
            content_ = new char[size_];
            memcpy(content_, &val, size_);
            type_ = DOUBLE;
        }
        std::string getStringVal(){
            return std::string(content_, size_);  
        }
        bool getBoolVal(){
            return *reinterpret_cast<bool*>(content_);
        }
        int getIntVal(){
            return *reinterpret_cast<int*>(content_);
        }
        long long getBigIntVal(){
            return *reinterpret_cast<long long*>(content_);
        }
        float getFloatVal(){
            return *reinterpret_cast<float*>(content_);
        }
        double getDoubleVal(){
            return *reinterpret_cast<double*>(content_);
        }
};


class TableSchema {
    public:
        TableSchema(std::string name, Table* table,const std::vector<Column> columns)
            : table_name_(name),
              table_(table),
              columns_(columns)
        {
            size_ = 0;
            for(auto c : columns){
                size_ += c.getSize();
            }
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
            for(auto c : columns_){
                Value val{};
                char* content = getValue(c.getName(), r, &val.size_);
                if(!content) return 1;
                val.content_ = content;
                val.type_ = c.getType();
                values.push_back(val);
            }
            return 0;
        }

        // translate a vector of values using the schema to a Record. 
        // return null in case of an error.
        // the user of the class should handle deleting the record after using it.
        Record* translateToRecord(std::vector<Value>& values){
            if(values.size() != columns_.size()) return nullptr;
            uint32_t fixed_part_size = size_;
            uint32_t var_part_size = 0;
            // we assume that the variable length columns are represented first.
            for(int i = 0; i < columns_.size(); ++i){
                if(columns_[i].isVarLength()) var_part_size += values[i].size_;
                else break;
            }
            // The bitmap bytes.
            fixed_part_size = (columns_.size() / 8) + (columns_.size() % 8);
            char* data = new char[fixed_part_size + var_part_size];
            std::memset(data, 0, fixed_part_size + var_part_size);
            
            uint16_t cur_var_offset = fixed_part_size; 
            for(int i = 0; i < values.size(); i++){
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
                // should initialize the bitmap byte (TODO).
            }
            return new Record(data, fixed_part_size + var_part_size);
        }
    private:
        std::vector<Column> columns_;
        std::string table_name_;
        uint32_t size_;
        Table* table_;
};


class Catalog {
    public:
        Catalog(CacheManager *cm)
            : cache_manager_ (cm)
        {
            
            // change the size after adding free space map support.
            FreeSpaceMap* meta_free_space = new FreeSpaceMap(0);

            // loading the hard coded meta data table schema.
            PageID meta_pid = {.file_name_ = META_DATA_FILE, .page_num_ = 0};
            meta_data_table_ = new Table(cm, meta_pid, meta_free_space);

            std::vector<Column> meta_data_columns_;
            meta_data_columns_.emplace_back(Column("table_name", VARCHAR, 0));
            meta_data_columns_.emplace_back(Column("col_name"  , VARCHAR, 4));
            meta_data_columns_.emplace_back(Column("col_type"  , INT    , 8));
            meta_data_columns_.emplace_back(Column("col_offset", INT    , 12));
            meta_data_columns_.emplace_back(Column("nullable"  , BOOLEAN, 16));
            meta_data_columns_.emplace_back(Column("primary"   , BOOLEAN, 17));
            meta_data_columns_.emplace_back(Column("foreign"   , BOOLEAN, 18));
            meta_data_columns_.emplace_back(Column("unique"    , BOOLEAN, 19));
            TableSchema* meta_table_schema = new TableSchema(META_DATA_TABLE, meta_data_table_, meta_data_columns_);

            // loading TableSchema of each table into memory.
            SequentialScan scanner(meta_data_table_);
            Record *r = nullptr;
            while(scanner.next(r)){
                std::vector<Value> values;
                int err = meta_table_schema->translateToValues(*r, values);
                if(err) break;
                // extract the data of this row.
                std::string table_name = values[0].getStringVal();
                std::string col_name = values[1].getStringVal();
                Type col_type = static_cast<Type>(values[2].getIntVal());
                int col_offset = values[3].getIntVal();
                int nullable = values[4].getBoolVal();
                int primary_key = values[5].getBoolVal();
                int foreign_key = values[6].getBoolVal();
                int unique = values[7].getBoolVal();

                // first time seeing this table? if yes then we need to initialize it.
                if(!tables_.count(table_name)){
                    PageID first_page = {.file_name_ = table_name+".ndb", .page_num_ = 0};
                    FreeSpaceMap* free_space = new FreeSpaceMap(0);
                    Table* table = new Table(cm, first_page, free_space);
                    TableSchema* schema = new TableSchema(table_name, table, {});
                    tables_.insert({table_name, schema});
                }

                // add the column to the table's meta data.
                std::vector<Column> cols;
                std::vector<Constraint> cons;
                if(nullable) cons.push_back(NULLABLE); 
                if(primary_key) cons.push_back(PRIMARY_KEY); 
                if(foreign_key) cons.push_back(FOREIGN_KEY); 
                if(unique) cons.push_back(UNIQUE); 
                
                tables_[table_name]->addColumn(Column(col_name, col_type, col_offset, cons));
            }
        }

        TableSchema* createTable(const std::string &table_name, std::vector<Column> &columns) {
                if (tables_.count(table_name))
                    return nullptr;
                // initialize the table
                PageID first_page = {.file_name_ = table_name+".ndb", .page_num_ = 0};
                FreeSpaceMap* free_space = new FreeSpaceMap(0);
                Table* table = new Table(cache_manager_, first_page, free_space);
                TableSchema* schema = new TableSchema(table_name, table, {});
                tables_.insert({table_name, schema});
                // persist the table schema in the meta data table.
                // create a vector of Values per column,
                // then insert it to the meta table.
                for(auto c : columns){
                    std::vector<Value> vals;
                    vals.emplace_back(Value(table_name));
                    vals.emplace_back(Value(c.getName()));
                    vals.emplace_back(Value(c.getType()));
                    vals.emplace_back(Value(c.getOffset()));
                    std::vector<Constraint> cons = c.getConstraints();
                    vals.emplace_back(Value(cons[0]));
                    vals.emplace_back(Value(cons[1]));
                    vals.emplace_back(Value(cons[2]));
                    vals.emplace_back(Value(cons[3]));
                    // translate the vals to a record and persist them.
                    Record* record = schema->translateToRecord(vals);
                    // rid is not used for now.
                    RecordID* rid = nullptr;
                    int err = table->insertRecord(rid, *record);
                    if(err) return nullptr;
                }
                return schema;
        }

        TableSchema* getTableSchema(const std::string &table_name) {
            if (!tables_.count(table_name))
                return nullptr;
            return tables_[table_name];
        }

        bool isValidTable(const std::string& table_name) {
            if (!tables_.count(table_name)) return false;
            return true;
        }

        Type stringToType(std::string t){
            if(t == "BOOLEAN")        return BOOLEAN;
            else if(t == "INT")       return INT;
            else if(t == "BIGINT")    return BIGINT;
            else if(t == "FLOAT")     return FLOAT;
            else if(t == "DOUBLE")    return DOUBLE;
            else if(t == "TIMESTAMP") return TIMESTAMP;
            else if(t == "VARCHAR")   return VARCHAR;

            return INVALID;
        }

        // provide delete and alter table later.
    private:
        CacheManager* cache_manager_;
        std::unordered_map<std::string, TableSchema*> tables_;
        FreeSpaceMap* free_space_map_;

        // hard coded data:
        Table* meta_data_table_;
        std::vector<Column> meta_data_columns_;

};
