#ifndef RECORD_H
#define RECORD_H

#include "page.h"
#include "value.h"
#include "table_schema.h"
#include <cstdint>



struct RecordID {
    RecordID();
    RecordID (PageID pid, uint32_t slot_number);

    PageID page_id_ = INVALID_PAGE_ID; 
    uint32_t slot_number_ = 0;
};


 /* starting with variable length columns, they are represended as 2 byte pairs (first 2 -> offset, last 2 -> length).
 *  then fixed size columns ( the user class should understand how to read those based on the size of each type).
 *  then null bitmap, its size is in bytes.
 *  the number of bitmap bytes is decided by this formula: (no. columns / 8) + (no. columns % 8),
 *  meaning that if we have 8 columns we would have 1 byte bitmap, and in case of 9 columns that means 2 bytes etc.
 *  then comes the actual variable length columns, in case of delete or update we just delete them then update them
 *  then append them to the end of the record the update the offset array.
 *  the size of the record is already stored inside the containing page.
 */

 //  this class is just a container to help upper levels of the system to decypher the data easier.
 //  it's supposed to be dump, just provides pointers to certain elements by providing the offset. 
class Record {
    public:
        Record(char* data, uint32_t r_size, bool read_only = true);
        ~Record();

        uint32_t getRecordSize();
        bool isInvalidRecord();
        void print();

        // a pointer to a variable length colomn.
        // size (output) the size of the returned variable length column.
        // return null in case of an error.
        char* getVariablePtr(uint32_t offset, uint16_t *size);

        // a pointer to any offset (the size is known by the user of the function).
        char* getFixedPtr(uint32_t offset);
    private:
        char* data_;
        uint32_t record_size_;
        bool read_only_ = true; // is the data_ read only (can't deallocate it?) or not.
};

struct Tuple {
    TableSchema* schema_;
    std::vector<Value> values_;
    /*
    Record* rec_;
    */
    void append_tuple_copy_to_end(Tuple* new_tuple) {
        assert(new_tuple != nullptr);
        int n = new_tuple->values_.size();
        assert(schema_ && schema_->numOfCols() >= values_.size() + n);
        for(int i = 0; i < n; ++i)
            values_.push_back(new_tuple->values_[i]);
    }
    void append_value_to_end(Value v) {
        values_.push_back(v);
    }
    /*
       void init(TableSchema* schema){
       schema_ = schema;
       values_.resize(schema_.numOfCols());
       }
       void copy_at_start(const Tuple* t) {
       assert(t && t.values_ && t.values_.size() <= values_.size());
       for(int i = 0; i < t.values_.size(); ++i)
       values_[i] = t->values_[i];
       }*/
};


#endif // RECORD_H
