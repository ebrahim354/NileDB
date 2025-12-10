#pragma once
#include "column.cpp"
#include "table.cpp"
#include "table_schema.h"



TableSchema::TableSchema(Arena* arena, String name, Table* table, const Vector<Column>& columns, bool tmp_schema):
    table_name_(name, arena), table_(table), columns_(columns, arena), tmp_schema_(tmp_schema)
{
    columns_.reserve(20);
    size_ = 0;
    for(auto& c : columns){
        size_ += c.getSize();
    }
}
void TableSchema::destroy() {}

String TableSchema::getTableName(){
    return table_name_;
}

int TableSchema::numOfCols() {
    return columns_.size();
}
// TODO: change columns to be a set instead of doing this.
int TableSchema::colExist(String& col_name) {
    String field = col_name;
    if(!tmp_schema_)
        field = split_scoped_field(col_name).second;

    for(size_t i = 0; i < columns_.size(); ++i){
        if(columns_[i].getName() == field)
            return i;
    }
    return -1;
}

bool TableSchema::checkValidValues(Vector<String>& fields, Vector<Value>& vals) {
    if(fields.size() != columns_.size() || fields.size() != vals.size()) return false;
    for(size_t i = 0; i < fields.size(); ++i){
        int col_idx = colExist(fields[i]);
        if(col_idx == -1) return false;
        if(columns_[col_idx].getName() != fields[i] || columns_[col_idx].getType() != vals[i].type_)
            return false;
    }
    return true;
}

bool TableSchema::checkValidValue(String& field, Value& val) {
    for(size_t i = 0; i < columns_.size(); ++i){
        if(columns_[i].getName() == field && columns_[i].getType() != val.type_)
            return true;
    }
    return false;
}

int TableSchema::getColIdx(String& field, Value& val){
    for(size_t i = 0; i < columns_.size(); ++i){
        if(columns_[i].getName() == field && columns_[i].getType() != val.type_){
            return i;
        }
    }
    return -1;

}

bool TableSchema::isValidCol(String& col_name){
    for(auto c : columns_)
        if(c.getName() == col_name) return true;
    return false;
}

void TableSchema::addColumn(Arena* arena, const String& name, Type type,
        u8 col_offset, ConstraintType constraints){
    size_ += getSizeFromType(type);
    columns_.emplace_back(arena, name, type, col_offset, constraints);
    std::sort(columns_.begin(), columns_.end(), [](Column& a, Column& b){
            return a.getOffset() < b.getOffset();
            });
}

String TableSchema::typeToString(Type t){
    if(t == BOOLEAN)        return "BOOLEAN";
    else if(t == INT)       return "INT";
    else if(t == BIGINT)    return "BIGINT";
    else if(t == FLOAT)     return "FLOAT";
    else if(t == DOUBLE)    return "DOUBLE";
    else if(t == TIMESTAMP) return "TIMESTAMP";
    else if(t == VARCHAR)   return "VARCHAR";
    return "INVALID";
}

void TableSchema::printSchema(std::stringstream& ss){
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

void TableSchema::printSchema(){
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
Vector<String> TableSchema::getCols(){
    Vector<String> cols;
    for(size_t i = 0; i < columns_.size(); ++i){
        cols.push_back(columns_[i].getName());
    }
    return cols;
}

Vector<Column> TableSchema::getColumns(){
    return columns_;
}

void TableSchema::printTableHeader(){
    for(size_t i = 0; i < columns_.size(); ++i){
        std::cout << columns_[i].getName();
        if(i != columns_.size() - 1) std::cout << " | ";
    }
    std::cout << "\n-----------------------------------------------------------------" << std::endl;
}
// get a pointer to a spicific value inside of a record using the schema. 
// Type conversion is done by the user of the function.
// return nullptr in case of an error or the value is equal to null (handle cases separately later).
char* TableSchema::getValue(String col_name ,Record& r, uint16_t* size){
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
int TableSchema::translateToValues(Record& r, Vector<Value>& values){
    for(int i = 0; i < columns_.size(); ++i){
        // check the bitmap if this value is null.
        char * bitmap_ptr = r.getFixedPtr(size_)+(i/8);  
        int is_null = *bitmap_ptr & (1 << (i%8));
        if(is_null) {
            values.emplace_back(Value(NULL_TYPE));
            continue;
        }
        uint16_t sz = 0;
        char* content = getValue(columns_[i].getName(), r, &sz);
        if(!content)
            return 1;
        Value val(content, columns_[i].getType(), sz);
        //val.type_ = columns_[i].getType();
        //val.value_from_size(val.size_);
        //memcpy(val.get_ptr(), content, val.size_);
        values.emplace_back(val);
    }
    return 0;
}

int TableSchema::translateToValuesOffset(Record& r, Vector<Value>& values, int offset){
    if( offset < 0 || offset + columns_.size() > values.size()) {
        assert(0 && "Can't translate this record");
        return 1;
    }
    for(int i = 0; i < columns_.size(); ++i){
        // check the bitmap if this value is null.
        char * bitmap_ptr = r.getFixedPtr(size_)+(i/8);  
        int is_null = *bitmap_ptr & (1 << (i%8));
        if(is_null) {
            values[offset+i] = Value(NULL_TYPE);
            continue;
        }
        Value* val = &values[offset+i];
        //val->type_ = columns_[i].getType();
        uint16_t sz = 0;
        char* content = getValue(columns_[i].getName(), r, &sz);
        if(!content)
            return 1;
        *val = Value(content, columns_[i].getType(), sz);
        //val->value_from_size(val->size_);
        //memcpy(val->get_ptr(), content, val->size_);
    }
    return 0;
}

int TableSchema::translateToTuple(Record& r, Tuple& tuple, RecordID& rid){
    if(columns_.size() > tuple.size()) {
        assert(0 && "Can't translate this record");
        return 1;
    }
    for(int i = 0; i < columns_.size(); ++i){
        // check the bitmap if this value is null.
        char * bitmap_ptr = r.getFixedPtr(size_)+(i/8);  
        int is_null = *bitmap_ptr & (1 << (i%8));
        if(is_null) {
            tuple.put_val_at(i, Value(NULL_TYPE));
            continue;
        }
        //val->type_ = columns_[i].getType();
        uint16_t sz = 0;
        char* content = getValue(columns_[i].getName(), r, &sz);
        if(!content)
            return 1;
        tuple.put_val_at(i, Value(content, columns_[i].getType(), sz));
    }
    tuple.left_most_rid_ = rid;
    return 0;
}

// translate a vector of values using the schema to a Record. 
// return invalid record in case of an error.
// the user of the class should handle deleting the record after using it.
// we assume that the variable length columns are represented first.
Record TableSchema::translateToRecord(Arena* arena, Vector<Value>& values) {
    if(values.size() != columns_.size()) return Record(nullptr, 0);
    uint32_t fixed_part_size = size_;
    uint32_t var_part_size = 0;
    for(size_t i = 0; i < columns_.size(); ++i){
        if(columns_[i].isVarLength()) var_part_size += values[i].size_;
    }
    // The bitmap bytes.
    fixed_part_size += (columns_.size() / 8) + (columns_.size() % 8);
    char* data = (char*)arena->alloc(fixed_part_size + var_part_size); 
    std::memset(data, 0, fixed_part_size + var_part_size);

    uint16_t cur_var_offset = fixed_part_size; 
    for(size_t i = 0; i < values.size(); i++){
        if(columns_[i].isVarLength()){
            // add 2 bytes for offset and 2 bytes for length
            memcpy(data + columns_[i].getOffset(), &cur_var_offset, sizeof(cur_var_offset));
            memcpy(data + columns_[i].getOffset() + 2, &values[i].size_, sizeof(cur_var_offset));
            // cpy the actual data and update the var offset pointer.
            memcpy(data + cur_var_offset, values[i].get_ptr(), values[i].size_);
            cur_var_offset+= values[i].size_;
        } else {
            // just copy the data into its fixed offset.
            memcpy(data + columns_[i].getOffset(), values[i].get_ptr(), values[i].size_);
        }
        // initialize the bitmap, 1 means null and 0 means not null.
        if(values[i].isNull()){
            char * bitmap_ptr = data+size_+(i/8);  
            *bitmap_ptr = *bitmap_ptr | (1 << (i%8));
        }
    }

    auto r = Record(data, fixed_part_size + var_part_size, false); // false => record owns the data ptr now.
    return r;
}

Record TableSchema::translateToRecord(Arena* arena, Tuple tuple){
    if(tuple.size() != columns_.size()) return Record(nullptr, 0);
    uint32_t fixed_part_size = size_;
    uint32_t var_part_size = 0;
    for(size_t i = 0; i < columns_.size(); ++i){
        if(columns_[i].isVarLength()) var_part_size += tuple.get_val_at(i).size_;
    }
    // The bitmap bytes.
    fixed_part_size += (columns_.size() / 8) + (columns_.size() % 8);
    char* data = (char*)arena->alloc(fixed_part_size + var_part_size); 
    std::memset(data, 0, fixed_part_size + var_part_size);

    uint16_t cur_var_offset = fixed_part_size; 
    for(size_t i = 0; i < tuple.size(); i++){
        Value cur_val = tuple.get_val_at(i);
        if(columns_[i].isVarLength()){
            // add 2 bytes for offset and 2 bytes for length
            memcpy(data + columns_[i].getOffset(), &cur_var_offset, sizeof(cur_var_offset));
            memcpy(data + columns_[i].getOffset() + 2, &cur_val.size_, sizeof(cur_var_offset));
            // cpy the actual data and update the var offset pointer.
            memcpy(data + cur_var_offset, cur_val.get_ptr(), cur_val.size_);
            cur_var_offset+= cur_val.size_;
        } else {
            // just copy the data into its fixed offset.
            memcpy(data + columns_[i].getOffset(), cur_val.get_ptr(), cur_val.size_);
        }
        // initialize the bitmap, 1 means null and 0 means not null.
        if(cur_val.isNull()){
            char * bitmap_ptr = data+size_+(i/8);  
            *bitmap_ptr = *bitmap_ptr | (1 << (i%8));
        }
    }

    auto r = Record(data, fixed_part_size + var_part_size, false); // false => record owns the data ptr now.
    return r;
}
Table* TableSchema::getTable(){
    return table_;
}
