#pragma once

#include <cstdint>
#include <vector>
#include <string>
// char_n  => fixed length string (less than page size).
// varchar => variable length string (less than page size).
enum Type { INVALID = -1, BOOLEAN, INT, BIGINT, FLOAT, DOUBLE, TIMESTAMP, VARCHAR}; 
enum Constraint { NULLABLE = 0, PRIMARY_KEY, FOREIGN_KEY, UNIQUE };


class Column {
    public:
        Column(std::string name, Type type, uint8_t col_offset, std::vector<Constraint> constraints= {}): 
            name_(name), 
            type_(type), 
            col_offset_(col_offset),
            constraints_(constraints),
            size_(getSizeFromType(type)){}
        ~Column(){}
        // the reson for this is that we don't want anyone to change the column meta data after initializing it.
        // if you want to modify the column you have to delete it and start a new one, this is better to avoid errors
        // in the future, for example: when we start adding ALTER TABLE command.
        std::string                    getName()        { return name_            ; }
        Type                           getType()        { return type_            ; }
        uint8_t                        getSize()        { return size_            ; }
        uint16_t                       getOffset()      { return col_offset_      ; }
        const std::vector<Constraint>& getConstraints() { return constraints_     ; }
        bool                           isVarLength()    { return type_ == VARCHAR ; }

    private:
        uint8_t getSizeFromType(Type t){
            switch(t){
                // size in bytes:
                case BOOLEAN: 
                    return 1; // could be 1 bit but the smallest unit in our system is 1 byte.
                case INT:
                case FLOAT: 
                case VARCHAR: // two bytes for the location pointer, two bytes for the size.
                    return 4; 
                case DOUBLE:
                case BIGINT:
                    return 8;
                default: 
                    return -1;
            }
        }
        std::string name_;
        Type type_ = INVALID;
        uint8_t size_ = 0; // in bytes.
        uint16_t col_offset_ = 0;
        std::vector<Constraint> constraints_;
};

