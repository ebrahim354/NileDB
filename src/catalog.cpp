#pragma once
#include "cache_manager.cpp"
#include "table.cpp"
#include "column.cpp"
#include "value.cpp"
#include <sstream>
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
#define META_DATA_FSM "NILEDB_META_DATA_fsm.ndb"
#define META_DATA_TABLE "NILEDB_META_DATA"





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
        ~TableSchema() {
            delete table_; 
        }

        int numOfCols() {
            return columns_.size();
        }
        // TODO: change columns to be a set instead of doing this.
        int colExist(std::string& col_name) {
            for(size_t i = 0; i < columns_.size(); ++i){
                if(columns_[i].getName() == col_name)
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
            for(size_t i = 0; i < columns_.size(); ++i){
                if(columns_[i].isVarLength()) var_part_size += values[i].size_;
                else break;
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
                // should initialize the bitmap byte (TODO).
            }
            return new Record(data, fixed_part_size + var_part_size);
        }
        Table* getTable(){
            return table_;
        }
    private:
        std::string table_name_;
        Table* table_;
        std::vector<Column> columns_;
        uint32_t size_;
};


class Catalog {
    public:
        Catalog(CacheManager *cm)
            : cache_manager_ (cm)
        {
            
            // change the size after adding free space map support.
            PageID meta_fsm_pid = {.file_name_ = META_DATA_FSM, .page_num_ = 1};
            free_space_map_ = new FreeSpaceMap(cm, meta_fsm_pid);


            // loading the hard coded meta data table schema.
            PageID meta_pid = {.file_name_ = META_DATA_FILE, .page_num_ = 1};
            auto meta_data_table = new Table(cm, meta_pid, free_space_map_);

            std::vector<Column> meta_data_columns;
            meta_data_columns.emplace_back(Column("table_name", VARCHAR, 0));
            meta_data_columns.emplace_back(Column("col_name"  , VARCHAR, 4));
            meta_data_columns.emplace_back(Column("col_type"  , INT    , 8));
            meta_data_columns.emplace_back(Column("col_offset", INT    , 12));
            meta_data_columns.emplace_back(Column("nullable"  , BOOLEAN, 16));
            meta_data_columns.emplace_back(Column("primary"   , BOOLEAN, 17));
            meta_data_columns.emplace_back(Column("foreign"   , BOOLEAN, 18));
            meta_data_columns.emplace_back(Column("unique"    , BOOLEAN, 19));
            meta_table_schema_ = new TableSchema(META_DATA_TABLE, meta_data_table, meta_data_columns);

            // loading TableSchema of each table into memory.
            TableIterator* it = meta_data_table->begin();
            while(it->advance()){
                Record r = it->getCurRecordCpy();
                std::vector<Value> values;
                int err = meta_table_schema_->translateToValues(r, values);
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
                    PageID first_page = {.file_name_ = table_name+".ndb", .page_num_ = 1};
                    PageID first_fsm_page = {.file_name_ = table_name+"_fsm.ndb", .page_num_ = 1};
                    FreeSpaceMap* free_space = new FreeSpaceMap(cm, first_fsm_page);
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
            delete it;
        }
        ~Catalog() {
            delete meta_table_schema_;
            delete free_space_map_;
            for(auto table : tables_){
                delete tables_[table.first];
            }
        }

        TableSchema* createTable(const std::string &table_name, std::vector<Column> &columns) {
                if (tables_.count(table_name))
                    return nullptr;
                // initialize the table
                PageID first_page = {.file_name_ = table_name+".ndb", .page_num_ = 1};
                PageID first_fsm_page = {.file_name_ = table_name+"_fsm.ndb", .page_num_ = 1};
                FreeSpaceMap* free_space = new FreeSpaceMap(cache_manager_, first_fsm_page);
                Table* table = new Table(cache_manager_, first_page, free_space);
                TableSchema* schema = new TableSchema(table_name, table, columns);
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
                    vals.emplace_back(Value(c.isNullable()));
                    vals.emplace_back(Value(c.isPrimaryKey()));
                    vals.emplace_back(Value(c.isForeignKey()));
                    vals.emplace_back(Value(c.isUnique()));
                    // translate the vals to a record and persist them.
                    Record* record = meta_table_schema_->translateToRecord(vals);
                    /*
                    if(record == nullptr) std::cout << " invalid record " << std::endl;
                    std::cout << " translated a schema to a record with size: " 
                        << record->getRecordSize() << std::endl;
                    for(int i = 0; i < record->getRecordSize(); i++){
                        std::cout << +(char)(*record->getFixedPtr(i)) << ",";
                    }
                    std::cout << std::endl;
                    */

                    // rid is not used for now.
                    RecordID* rid = new RecordID();
                    int err = meta_table_schema_->getTable()->insertRecord(rid, *record);
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
            else if(t == "INTEGER")   return INT;
            else if(t == "BIGINT")    return BIGINT;
            else if(t == "FLOAT")     return FLOAT;
            else if(t == "DOUBLE")    return DOUBLE;
            else if(t == "TIMESTAMP") return TIMESTAMP;
            else if(t == "VARCHAR")   return VARCHAR;

            return INVALID;
        }
        std::vector<std::string> getTableNames(){
            std::vector<std::string> output;
            for(auto& t : tables_){
                output.push_back(t.first);
            }
            return output;
        }

        // provide delete and alter table later.
    private:
        CacheManager* cache_manager_;
        std::unordered_map<std::string, TableSchema*> tables_;
        FreeSpaceMap* free_space_map_;

        // hard coded data:

        TableSchema* meta_table_schema_;

};
