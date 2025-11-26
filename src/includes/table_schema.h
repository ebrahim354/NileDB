#ifndef TABLE_SCHEMA_H
#define TABLE_SCHEMA_H

#include "column.h"
#include "table.h"
#include "record.h"

class Table;
struct Record;
struct Tuple;



class TableSchema {
    public:
        TableSchema(std::string name, Table* table,const std::vector<Column> columns,bool tmp_schema = false);
        ~TableSchema();

        std::string getTableName();
        int numOfCols();
        int colExist(std::string& col_name);
        bool checkValidValues(std::vector<std::string>& fields, std::vector<Value>& vals);
        bool checkValidValue(std::string& field, Value& val);
        int getColIdx(std::string& field, Value& val);
        bool isValidCol(std::string& col_name);
        void addColumn(Column c);
        std::string typeToString(Type t);
        void printSchema(std::stringstream& ss);
        void printSchema();
        std::vector<std::string> getCols();
        std::vector<Column> getColumns();
        void printTableHeader();
        // get a pointer to a spicific value inside of a record using the schema. 
        // Type conversion is done by the user of the function.
        // return nullptr in case of an error or the value is equal to null (handle cases separately later).
        char* getValue(std::string col_name ,Record& r, uint16_t* size);
        // translate a given record using the schema to a vector of Value type.
        // return 1 in case of an error.
        // values is the output.
        int translateToValues(Record& r, std::vector<Value>& values);
        int translateToValuesOffset(Record& r, std::vector<Value>& values, int offset);
        int translateToTuple(Record& r, Tuple& tuple, int offset);
        // translate a vector of values using the schema to a Record. 
        // return null in case of an error.
        // the user of the class should handle deleting the record after using it.
        // we assume that the variable length columns are represented first.
        Record* translateToRecord(std::vector<Value>& values);
        Record* translateToRecord(Tuple tuple);
        Table* getTable();
    private:
        bool tmp_schema_ = false;
        std::string table_name_;
        Table* table_;
        std::vector<Column> columns_;
        uint32_t size_;
};

#endif // TABLE_SCHEMA_H
