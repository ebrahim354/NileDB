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
    public:
        char* content_ = nullptr;  
        uint16_t size_ = 0;
        Type type_ = INVALID;
        //Value(){} 
        void value_from_size(int sz);
        ~Value();

        Value(Type type = NULL_TYPE);
        // handle memory leaks later.
        // constuctors for different value types.
        Value(const Value& rhs);

        Value& operator=(const Value& rhs);
        bool cast_up();


        Value& operator+=(const Value& rhs);

        Value& operator-=(const Value& rhs);

        Value& operator*=(const Value& rhs);

        Value& operator/=(const Value& rhs);

        Value operator-(Value rhs);

        Value(const std::string& str);
        Value(bool val);
        Value(int val);
        Value(long long val);
        Value(float val);
        Value(double val);


        std::string toString() const;

        inline bool isInvalid() const;
        inline bool isNull() const;
        std::string getStringVal() const;
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

        void setValue(int val);
        void setValue(long long val);
        void setValue(float val);
        void setValue(double val);
        Value& operator++();
};

// -1 ==> lhs < rhs, 0 eq, 1 ==> lhs > rhs
int value_cmp(Value lhs, Value rhs);
