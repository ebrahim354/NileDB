#pragma once

#include <cstdint>
#include <string>
#include "column.h"

Type tokenTypeToColType(TokenType t){
    switch(t){
        case TokenType::BOOLEAN:
            return Type::BOOLEAN;
        case TokenType::INTEGER:
            return Type::INT;
        case TokenType::BIGINT:
            return Type::BIGINT;
        case TokenType::FLOAT:
            return Type::FLOAT;
        case TokenType::REAL:
            return Type::DOUBLE;
        case TokenType::TIMESTAMP:
            return Type::TIMESTAMP;
        case TokenType::VARCHAR:
            return Type::VARCHAR;
        default:
            return INVALID;
    }
}

Column::Column(std::string name, Type type, u8 col_offset, ConstraintType constraints): 
    name_(name), 
    type_(type), 
    col_offset_(col_offset),
    constraints_(constraints),
    size_(getSizeFromType(type)){}
    Column::~Column(){}
    // the reson for this is that we don't want anyone to change the column meta data after initializing it.
    // if you want to modify the column you have to delete it and start a new one, this is better to avoid errors
    // in the future, for example: when we start adding ALTER TABLE command.
inline void                    Column::setName(std::string& name) { name_ = name  ; }
inline std::string             Column::getName()        { return name_            ; }
inline Type                    Column::getType()        { return type_            ; }
inline u8                      Column::getSize() const  { return size_            ; }
inline u16                     Column::getOffset()      { return col_offset_      ; }
inline ConstraintType          Column::getConstraints() { return constraints_     ; }
inline bool                    Column::isVarLength()    { return type_ == VARCHAR ; }
inline bool Column::isNullable() { 
    return !(constraints_&CONSTRAINT_NOT_NULL);
}
inline bool Column::isPrimaryKey() { 
    return (constraints_&CONSTRAINT_PRIMARY_KEY);
}
inline bool Column::isForeignKey() { 
    return (constraints_&CONSTRAINT_FOREIGN_KEY);
}
inline bool Column::isUnique() { 
    return (constraints_&CONSTRAINT_UNIQUE);
}

uint8_t Column::getSizeFromType(Type t){
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
