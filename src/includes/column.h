#ifndef COLUMN_H
#define COLUMN_H

#include <cstdint>
#include <vector>
#include <string>
// char_n  => fixed length string (less than page size).
// varchar => variable length string (less than page size).

// if you need to add a new type append it to the end because those types are saved to disk as int values
// if you add the new type from the begining that will shift the int value of the rest.
// and changing those values will work for new .NDB files but will not work for older ones.
//
//
// EXECUTOR_ID is integer index that points to the executor inside of the QueryCTX object.
enum Type { INVALID = -1, BOOLEAN, INT, BIGINT, FLOAT, DOUBLE, TIMESTAMP, VARCHAR, NULL_TYPE, EXECUTOR_ID};
bool checkSameType(Type lhs, Type rhs);

enum Constraint { NOT_NULL = 0, PRIMARY_KEY, FOREIGN_KEY, UNIQUE };

class Column {
    public:
        Column(std::string name, Type type, uint8_t col_offset, std::vector<Constraint> constraints= {});
        ~Column();

        // the reson for this is that we don't want anyone to change the column meta data after initializing it.
        // if you want to modify the column you have to delete it and start a new one, this is better to avoid errors
        // in the future, for example: when we start adding ALTER TABLE command.
        void                           setName(std::string& name);
        std::string                    getName();
        Type                           getType();
        uint8_t                        getSize();
        uint16_t                       getOffset();
        const std::vector<Constraint>& getConstraints();
        bool                           isVarLength();
        bool isNullable();
        bool isPrimaryKey();
        bool isForeignKey();
        bool isUnique();
        static uint8_t getSizeFromType(Type t);

    private:
        std::string name_;
        Type type_ = INVALID;
        uint16_t col_offset_ = 0;
        std::vector<Constraint> constraints_ = {};
        uint8_t size_ = 0; // in bytes.
};

#endif // COLUMN_H
