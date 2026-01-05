#ifndef COLUMN_H
#define COLUMN_H

#include "tokenizer.h"
#include <cstdint>
#include <string>
// char_n  => fixed length string (less than page size).
// varchar => variable length string (less than page size).

// if you need to add a new type append it to the end because those types are saved to disk as int values
// if you add the new type from the begining that will shift the int value of the rest.
// and changing those values will work for new .NDB files but will not work for older ones.
//
//
// EXECUTOR_ID is integer index that points to the executor inside of the QueryCTX object.
enum Type : i8 { 
    INVALID = -1,
    BOOLEAN,
    INT,
    BIGINT,
    FLOAT,
    DOUBLE,
    TIMESTAMP,
    VARCHAR,
    OVERFLOW_ITERATOR, // contains a pointer to an iterator of type OverflowIterator, used for large values.
    EXECUTOR_ID,
    NULL_TYPE
};

// constants for constraints masks.
#define ConstraintType           u8
#define CONSTRAINT_NOTHING       0
#define CONSTRAINT_NOT_NULL      1
#define CONSTRAINT_PRIMARY_KEY   2
#define CONSTRAINT_FOREIGN_KEY   4
#define CONSTRAINT_UNIQUE        8

Type tokenTypeToColType(TokenType t);
bool checkSameType(Type lhs, Type rhs);
u8 getSizeFromType(Type t);


class Column {
    public:
        Column(Arena* arena, String8 name, Type type, u8 col_offset, ConstraintType constraints = CONSTRAINT_NOTHING);
        Column(String8 name, Type type, u8 col_offset, ConstraintType constraints = CONSTRAINT_NOTHING);
        ~Column();

        // the reson for this is that we don't want anyone to change the column meta data after initializing it.
        // if you want to modify the column you have to delete it and start a new one, this is better to avoid errors
        // in the future, for example: when we start adding ALTER TABLE command.
        void                           setName(String8 name);
        String8                        getName();
        Type                           getType();
        u8                             getSize() const;
        u16                            getOffset();
        ConstraintType                 getConstraints();
        bool                           isVarLength();
        bool isNullable();
        bool isPrimaryKey();
        bool isForeignKey();
        bool isUnique();

    private:
        String8 name_;
        Type type_ = INVALID;
        uint16_t col_offset_ = 0;
        ConstraintType constraints_ = CONSTRAINT_NOTHING;
        uint8_t size_ = 0; // in bytes.
};

#endif // COLUMN_H
