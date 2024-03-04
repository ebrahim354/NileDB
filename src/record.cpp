#pragma once
#include "page.cpp"
#include <cstdint>



struct RecordID {
    PageID page_id_; 
    uint32_t slot_number_;
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
        Record(char* data, uint32_t r_size): 
            data_(data),
            record_size_(r_size)
        {}
        ~Record(){}


        uint32_t getRecordSize(){
            return record_size_;
        }

        bool isInvalidRecord(){
            return data_ == nullptr;
        }



        // a pointer to a variable length colomn.
        // size (output) the size of the returned variable length column.
        // return null in case of an error.
        char* getVariablePtr(uint32_t offset, uint16_t *size){
            if( offset >= record_size_ ) return nullptr;
            uint16_t real_value_offset = *reinterpret_cast<uint16_t*>(data_+offset);
            *size = *reinterpret_cast<uint16_t*>(data_+offset+2);
            return data_+real_value_offset;
        }

        // a pointer to any offset (the size is known by the user of the function).
        char* getFixedPtr(uint32_t offset){
            if( offset >= record_size_ ) return nullptr;
            return data_+offset;
        }
    private:
        char* data_;
        uint32_t record_size_;
};
