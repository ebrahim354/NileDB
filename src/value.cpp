#include <cstdint>
#include <string>
#include <memory.h>
#include "column.cpp"

class Value {
    public:
        char* content_; 
        uint16_t size_;
        Type type_;
        Value(){} 
        ~Value(){}
        // handle memory leaks later.
        // constuctors for different value types.
        Value(std::string str){
            size_ = str.size(); 
            content_ = new char[size_];
            str.copy(content_, size_);
//            memcpy(content_, str.c_str(), size_);
            type_ = VARCHAR;
        }
        Value(bool val){
            size_ = 1; 
            content_ = new char[size_];
            memcpy(content_, &val, size_);
            type_ = BOOLEAN;
        }
        Value(int val){
            size_ = 4; 
            content_ = new char[size_];
            memcpy(content_, &val, size_);
            type_ = INT;
        }
        Value(long long val){
            size_ = 8; 
            content_ = new char[size_];
            memcpy(content_, &val, size_);
            type_ = BIGINT;
        }
        Value(float val){
            size_ = 4; 
            content_ = new char[size_];
            memcpy(content_, &val, size_);
            type_ = FLOAT;
        }
        Value(double val){
            size_ = 8; 
            content_ = new char[size_];
            memcpy(content_, &val, size_);
            type_ = DOUBLE;
        }

        std::string getStringVal() const {
            return std::string(content_, size_);  
        }
        bool getBoolVal() const{
            return *reinterpret_cast<bool*>(content_);
        }
        int getIntVal() const {
            return *reinterpret_cast<int*>(content_);
        }
        long long getBigIntVal() const {
            return *reinterpret_cast<long long*>(content_);
        }
        float getFloatVal() const {
            return *reinterpret_cast<float*>(content_);
        }
        double getDoubleVal() const {
            return *reinterpret_cast<double*>(content_);
        }
        // We assume that the user of these operators has already checked for the types to be equal.
        // If both values are not of the same type always return false.
        // The use of switch cases is redundent and can be replaced with generics or 
        // returning a void* and comparing bytes using the size_ parameter.
        //
        //
        // return true of lhs (this) == rhs else return false.
        bool operator==(const Value &rhs) const { 
            if(!checkSameType(type_, rhs.type_)) return false;
            // not the best solution to our current problem.
            switch (type_) {
                case VARCHAR: 
                    return getStringVal() == rhs.getStringVal();
                case BOOLEAN:
                    return getBoolVal()   == rhs.getBoolVal();
                case INT:
                    return getIntVal()    == rhs.getIntVal();
                case BIGINT:
                    return getBigIntVal() == rhs.getBigIntVal();
                case FLOAT:
                    return getFloatVal()  == rhs.getFloatVal();
                case DOUBLE:
                    return getDoubleVal() == rhs.getDoubleVal();
                default :
                    return false;
            }
        }

        // return true of lhs (this) < rhs else return false.
        bool operator<(const Value &rhs) const { 
            if(!checkSameType(type_, rhs.type_)) return false;
            // not the best solution to our current problem.
            switch (type_) {
                case VARCHAR: 
                    return getStringVal() < rhs.getStringVal();
                case BOOLEAN:
                    return getBoolVal()   < rhs.getBoolVal();
                case INT:
                    return getIntVal()    < rhs.getIntVal();
                case BIGINT:
                    return getBigIntVal() < rhs.getBigIntVal();
                case FLOAT:
                    return getFloatVal()  < rhs.getFloatVal();
                case DOUBLE:
                    return getDoubleVal() < rhs.getDoubleVal();
                default :
                    return false;
            }
        }

        // return true of lhs (this) > rhs else return false.
        bool operator>(const Value &rhs) const { 
            if(!checkSameType(type_, rhs.type_)) return false;
            // not the best solution to our current problem.
            switch (type_) {
                case VARCHAR: 
                    return getStringVal() > rhs.getStringVal();
                case BOOLEAN:
                    return getBoolVal()   > rhs.getBoolVal();
                case INT:
                    return getIntVal()    > rhs.getIntVal();
                case BIGINT:
                    return getBigIntVal() > rhs.getBigIntVal();
                case FLOAT:
                    return getFloatVal()  > rhs.getFloatVal();
                case DOUBLE:
                    return getDoubleVal() > rhs.getDoubleVal();
                default :
                    return false;
            }
        }
};
