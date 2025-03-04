#pragma once

#include <cstdint>
#include <vector>
#include <string>
// char_n  => fixed length string (less than page size).
// varchar => variable length string (less than page size).


// if you need to add a new type append it to the end because those types are saved to disk as int values
// if you add the new type from the begining that will shift the int value of the rest.
// and changing those values will work for new .NDB files but will not work for older ones.
enum Type { INVALID = -1, BOOLEAN, INT, BIGINT, FLOAT, DOUBLE, TIMESTAMP, VARCHAR, NULL_TYPE}; 
bool checkSameType(Type lhs, Type rhs) {
    return lhs == rhs;
}

enum Constraint { NOT_NULL = 0, PRIMARY_KEY, FOREIGN_KEY, UNIQUE };


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
        void                           setName(std::string& name) { name_ = name  ; }
        std::string                    getName()        { return name_            ; }
        Type                           getType()        { return type_            ; }
        uint8_t                        getSize()        { return size_            ; }
        uint16_t                       getOffset()      { return col_offset_      ; }
        const std::vector<Constraint>& getConstraints() { return constraints_     ; }
        bool                           isVarLength()    { return type_ == VARCHAR ; }
        bool isNullable() { 
            for (auto con : constraints_) 
                if(con == NOT_NULL) return false; 
            return true;
        }
        bool isPrimaryKey() { 
            for (auto con : constraints_) 
                if(con == PRIMARY_KEY) return true; 
            return false;
        }
        bool isForeignKey() { 
            for (auto con : constraints_) 
                if(con == FOREIGN_KEY) return true; 
            return false;
        }
        bool isUnique() { 
            for (auto con : constraints_) 
                if(con == UNIQUE) return true; 
            return false;
        }

        static uint8_t getSizeFromType(Type t){
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

    private:
        std::string name_;
        Type type_ = INVALID;
        uint16_t col_offset_ = 0;
        std::vector<Constraint> constraints_ = {};
        uint8_t size_ = 0; // in bytes.
};

