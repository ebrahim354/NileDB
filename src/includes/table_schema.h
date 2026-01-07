#ifndef TABLE_SCHEMA_H
#define TABLE_SCHEMA_H

#include "column.h"
#include "table.h"
#include "record.h"
#include "table_iterator.h"

class Table;
class TableIterator;
struct Record;
struct RecordID;
struct Tuple;



class TableSchema {
    public:
        TableSchema(Arena* arena, String8 name, Table* table, const Vector<Column>& columns, bool tmp_schema = false);
        void destroy();

        String8 getTableName();
        int numOfCols();
        int col_exist(String8 col_name);

        Column getCol(int idx);
        u32 getSize();

        bool is_valid_col(String8 col_name);
        Vector<String8> getCols();
        Vector<Column> getColumns();
        // get a pointer to a spicific value inside of a record using the schema. 
        // Type conversion is done by the user of the function.
        // return nullptr in case of an error or the value is equal to null (handle cases separately later).
        char* getValue(u64 col_idx, Record& r, uint16_t* size);
        // translate a given record using the schema to a vector of Value type.
        // return 1 in case of an error.
        int translateToTuple(Record& r, Tuple& tuple, RecordID& rid);
        // translate a vector of values using the schema to a Record. 
        // return null in case of an error.
        // the user of the class should handle deleting the record after using it.
        // we assume that the variable length columns are represented first.
        Record translateToRecord(Arena* arena, Vector<Value>& values);
        Record translateToRecord(Arena* arena, Tuple tuple);
        // rid is output.
        // return non 0 value in case of an error.
        int insert(Arena& arena, const Tuple& tuple, RecordID* rid);
        int remove(RecordID& rid);

        TableIterator begin(); 
        Table* getTable();
    private:
        bool tmp_schema_ = false;
        String8 table_name_;
        Table* table_;
        Vector<Column> columns_;
        u32 size_;
};

#endif // TABLE_SCHEMA_H
