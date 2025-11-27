#pragma once

#include <cstdint>
#include <vector>
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

Column::Column(std::string name, Type type, uint8_t col_offset, std::vector<Constraint> constraints): 
    name_(name), 
    type_(type), 
    col_offset_(col_offset),
    constraints_(constraints),
    size_(getSizeFromType(type)){}
    Column::~Column(){}
    // the reson for this is that we don't want anyone to change the column meta data after initializing it.
    // if you want to modify the column you have to delete it and start a new one, this is better to avoid errors
    // in the future, for example: when we start adding ALTER TABLE command.
    void                           Column::setName(std::string& name) { name_ = name  ; }
    std::string                    Column::getName()        { return name_            ; }
    Type                           Column::getType()        { return type_            ; }
    uint8_t                        Column::getSize()        { return size_            ; }
    uint16_t                       Column::getOffset()      { return col_offset_      ; }
    const std::vector<Constraint>& Column::getConstraints() { return constraints_     ; }
    bool                           Column::isVarLength()    { return type_ == VARCHAR ; }
    bool Column::isNullable() { 
        for (auto con : constraints_) 
            if(con == NOT_NULL) return false; 
        return true;
    }
bool Column::isPrimaryKey() { 
    for (auto con : constraints_) 
        if(con == PRIMARY_KEY) return true; 
    return false;
}
bool Column::isForeignKey() { 
    for (auto con : constraints_) 
        if(con == FOREIGN_KEY) return true; 
    return false;
}
bool Column::isUnique() { 
    for (auto con : constraints_) 
        if(con == UNIQUE) return true; 
    return false;
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
