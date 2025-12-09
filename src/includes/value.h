#pragma once
#include <cstdint>
#include <string>
#include <memory.h>
#include "column.h"
#include "utils.h"

#define INT64_MX (int64_t) 9223372036854775807
#define INT64_MN (int64_t) -9223372036854775807
#define INT32_MX 2147483647
#define INT32_MN -2147483648
#define EPS 1e-6
class Value;
int value_cmp(Value lhs, Value rhs);

enum MathOp {
    MATH_PLUS,
    MATH_MINUS,
    MATH_MULT,
    MATH_DIV
};
// check for overflows and underflows for 64 bit.
bool is_valid_operation64(int64_t a, int64_t b, MathOp op);

class Value {
    private:
        uintptr_t content_ = 0;
    public:
        uint16_t size_ = 0;
        Type type_ = INVALID;
        //Value(){} 
        ~Value();

        Value(Type type = NULL_TYPE);
        bool cast_up();
        char* get_ptr();


        Value& operator+=(const Value& rhs);

        Value& operator-=(const Value& rhs);

        Value& operator*=(const Value& rhs);

        Value& operator/=(const Value& rhs);

        Value operator-(Value rhs);

        void setValue(Type t, char* content);
        Value get_copy(Arena* arena);
        Value(char* content, Type t, uint16_t size);
        Value(char* str, uint16_t size);
        Value(Arena* arena, const String& str);
        Value(bool val);
        Value(int val);
        Value(long long val);
        Value(float val);
        Value(double val);


        String toString() const;

        inline bool isInvalid() const;
        inline bool isNull() const;
        String getStringVal() const;
        bool getBoolVal() const;
        int getIntVal() const;
        long long getBigIntVal() const;
        float getFloatVal() const;
        double getDoubleVal() const;

        bool operator==(const Value &rhs) const;
        bool operator!=(const Value &rhs) const;
        bool operator<(const Value &rhs) const;
        bool operator<=(const Value &rhs) const;
        bool operator>(const Value &rhs) const;
        bool operator>=(const Value &rhs) const;

        void setIntValue(int32_t val);
        void setBigintValue(int64_t val);
        void setFloatValue(float val);
        void setDoubleValue(double val);
        Value& operator++();
};

// -1 ==> lhs < rhs, 0 eq, 1 ==> lhs > rhs
int value_cmp(Value lhs, Value rhs);
