#pragma once
#include "column.cpp"
#include "table.cpp"
#include "table_schema.h"
#include <queue>


#define MAX_RECORD_SiZE (PAGE_SIZE / 2)

TableSchema::TableSchema(Arena* arena, String8 name, Table* table, const Vector<Column>& columns, bool tmp_schema):
    table_name_(name), table_(table), columns_(columns, arena), tmp_schema_(tmp_schema)
{
    columns_.reserve(20);
    size_ = 0;
    for(auto& c : columns){
        size_ += c.getSize();
    }
    // for non-temporary schemas the fixed part of a tuple can't exceed PAGE_SIZE / 2.
    // TODO: change the constructor to be a function that may or may not fail.
    if(!tmp_schema)
        assert(size_ < MAX_RECORD_SiZE);
}
void TableSchema::destroy() {}

String8 TableSchema::getTableName(){
    return table_name_;
}



int TableSchema::numOfCols() {
    return columns_.size();
}

int TableSchema::col_exist(String8 col_name) {
    for(size_t i = 0; i < columns_.size(); ++i){
        if(columns_[i].getName() == col_name)
            return i;
    }
    return -1;
}

bool TableSchema::is_valid_col(String8 col_name){
    for(auto c : columns_)
        if(c.getName() == col_name) return true;
    return false;
}

Vector<String8> TableSchema::getCols(){
    Vector<String8> cols;
    for(size_t i = 0; i < columns_.size(); ++i){
        cols.push_back(columns_[i].getName());
    }
    return cols;
}

Vector<Column> TableSchema::getColumns(){
    return columns_;
}

// get a pointer to a specific value inside of a record using the schema. 
// Type conversion is done by the user of the function.
// return nullptr in case of an error or the value is equal to null (handle cases separately later).
char* TableSchema::getValue(String8 col_name ,Record& r, uint16_t* size){
    Column* col = nullptr;
    for(size_t i = 0; i < columns_.size(); ++i) {
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
    u32 fixed_part_size = size_;
    u32 var_part_size = 0;
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
    u32 fixed_part_size = size_;
    u32 var_part_size = 0;
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

int TableSchema::remove(RecordID& rid) {
    TableDataPage* table_page = table_->get_data_page(rid.page_id_.page_num_); 
    if(table_page == nullptr) return 1;
    char* cur_data = nullptr;
    uint32_t rsize = 0;
    int err = table_page->getRecord(&cur_data, &rsize, rid.slot_number_);
    if(err || rsize <= 0 || !cur_data) return 1;

    // loop over the record and check for values that utilize overflow pages.
    Record cur_r = Record(cur_data, rsize);
    for(int i = 0; i < columns_.size(); ++i){
        if(columns_[i].isVarLength()){
            u16 sz = 0;
            char* content = getValue(columns_[i].getName(), cur_r, &sz);
            // it is in fact an overflow page keep fetching other overflow pages to delete them.
            if(sz == MAX_U16) {
                PageNum cur_pnum = *(PageNum*)content;
                while(cur_pnum != 0 && cur_pnum != INVALID_PAGE_NUM) {
                    auto page = table_->get_overflow_page(cur_pnum);
                    PageNum next_pnum = page->getNextPageNumber();
                    table_->delete_overflow_page(cur_pnum);
                    std::cout << "removed page: " << cur_pnum << "\n";
                    cur_pnum = next_pnum;
                }
            }
        }
    }

    err = table_->deleteRecord(rid);
    table_->release_data_page(rid.page_id_.page_num_);
    if(err) 
        return err;
    return 0;
}

int TableSchema::insert(Arena& arena, const Tuple& in_tuple, RecordID* rid) {
    if(tmp_schema_ || !table_) {
        assert(0);
        return 1;
    }

    if(in_tuple.size() != columns_.size()) {
        assert(0);
        return 1;
    }


    u32 fixed_part_size = size_;
    u32 var_part_size = 0;

    //                           size, col_idx
    std::priority_queue<std::pair<u32, u32>> max_size_queue;

    u32 tuple_size = 0;
    for(size_t i = 0; i < columns_.size(); ++i){
        if(columns_[i].isVarLength()) {
            tuple_size = in_tuple.get_val_at(i).size_;
            max_size_queue.push({tuple_size, i});
            var_part_size += tuple_size;
        }
    }

    // The bitmap bytes.
    fixed_part_size += (columns_.size() / 8) + (columns_.size() % 8);

    ArenaTemp memory_snapshot = arena.start_temp_arena(); 

    // take a copy in case of mutating the tuple.
    Tuple tuple = in_tuple.duplicate(&arena);

    // record is too big => start creating overflow pages that will fit the size.
    while(var_part_size + fixed_part_size > MAX_RECORD_SiZE && !max_size_queue.empty()) {
        u32 col_idx  = max_size_queue.top().second;
        u32 val_size = max_size_queue.top().first;
        // no point in overflowing if the size is already 4 or smaller.
        if(val_size < 5) break;
        char* val_ptr = tuple.get_val_at(col_idx).get_ptr();
        char* end_ptr = (val_ptr + val_size);
        u32 bytes_written = 0;
        PageNum last_overflow_page_num = 0;

        while(val_size) {
            OverflowPage* page = table_->new_overflow_page(); {
                assert(page != nullptr);

                page->setNextPageNumber(last_overflow_page_num);
                last_overflow_page_num = page->page_id_.page_num_;

                u32 max_copyiable_content = std::min(val_size, (u32) MAX_OVERFLOW_PAGE_CONTENT_SIZE);

                /*
                std::string str = std::string(end_ptr-(max_copyiable_content), max_copyiable_content);
                std::cout << "current copy: " << str << std::endl;
                */

                memcpy(page->getContentPtr(), (end_ptr-(max_copyiable_content)), max_copyiable_content);

                page->setContentSize((u16) max_copyiable_content);
                val_size -= max_copyiable_content;
                bytes_written += max_copyiable_content; 
                end_ptr -= bytes_written;

            } table_->release_overflow_page(last_overflow_page_num);
        }

        var_part_size -= max_size_queue.top().first;
        // re-assign the value to be the page number of the overflow page.
        tuple.put_val_at(col_idx, Value((i32) last_overflow_page_num));
        var_part_size += tuple.get_val_at(col_idx).size_;
        max_size_queue.pop();
    }

    int result = 0;
    char* data = (char*)arena.alloc(fixed_part_size + var_part_size); 
    std::memset(data, 0, fixed_part_size + var_part_size);

    uint16_t cur_var_offset = fixed_part_size; 
    for(size_t i = 0; i < tuple.size(); i++){
        Value cur_val = tuple.get_val_at(i);
        // first case => the value isVarLength and it's type is an INT instead of VARCHAR,
        // this indicates taht it got stored in an overflow page,
        // assign the size in the slot array to 0xFF and the actual data has size 4,
        // and holds the pageNum of the overflow page.
        if(columns_[i].isVarLength() && cur_val.type_ == INT){
            u16 max_sz = MAX_U16;
            // add 2 bytes for offset and 2 bytes for length
            memcpy(data + columns_[i].getOffset(), &cur_var_offset, sizeof(cur_var_offset));
            memcpy(data + columns_[i].getOffset() + 2, &max_sz, sizeof(cur_var_offset));
            // cpy the actual data and update the var offset pointer.
            memcpy(data + cur_var_offset, cur_val.get_ptr(), cur_val.size_);
            cur_var_offset+= cur_val.size_;

        } else if(columns_[i].isVarLength()) {
            // secocnd case => variable length column and no overflow happened,
            // we just store it in the record.
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

    Record r = Record(data, fixed_part_size + var_part_size);
    result = table_->insertRecord(rid, r);

    arena.clear_temp_arena(memory_snapshot);
    return result;
}

TableIterator TableSchema::begin(){
    return table_->begin(this); 
}

Table* TableSchema::getTable(){
    return table_;
}

Column TableSchema::getCol(int idx){
    assert(idx < columns_.size());
    return columns_[idx];
}

u32 TableSchema::getSize() {
    return size_;
}
