#pragma once
#include <cstdint>
#include <string>
#include <memory.h>
#include "column.cpp"
#include "utils.cpp"

class Value {
    public:
        char* content_ = nullptr;  
        uint16_t size_ = 0;
        Type type_ = INVALID;
        //Value(){} 
        void value_from_size(int sz){
            size_ = sz;
            //delete [] content_;
            //content_ = new char[size_];
            free(content_);
            content_ = (char*) malloc(size_);
        }
        ~Value(){
            free(content_);
        }

        Value(Type type = NULL_TYPE){
            type_ = type;
        } 
        // handle memory leaks later.
        // constuctors for different value types.
        Value(const Value& rhs){
            this->size_ = rhs.size_;
            if(!rhs.isNull()){
                //delete[] this->content_;
                //this->content_ = new char[size_];
                free(this->content_);
                this->content_ = (char*) malloc(size_);
                memcpy(this->content_, rhs.content_, size_);
            }
            this->type_ = rhs.type_;
        } 

        Value& operator=(const Value& rhs){
            this->size_ = rhs.size_;
            if(!rhs.isNull()){
                //delete[] this->content_;
                //this->content_ = new char[size_];
                free(this->content_);
                this->content_ = (char*) malloc(size_);
                memcpy(this->content_, rhs.content_, size_);
            }
            this->type_ = rhs.type_;
            return *this;
        } 

        Value(const std::string& str){
            size_ = str.size(); 
            //content_ = new char[size_];
            this->content_ = (char*) malloc(size_);
            //str.copy(content_, size_);
            memcpy(content_, str.c_str(), size_);
            type_ = VARCHAR;
        }
        Value(bool val){
            size_ = 1; 
            //content_ = new char[size_];
            this->content_ = (char*) malloc(size_);
            memcpy(content_, &val, size_);
            type_ = BOOLEAN;
        }
        Value(int val){
            size_ = 4; 
            //content_ = new char[size_];
            this->content_ = (char*) malloc(size_);
            memcpy(content_, &val, size_);
            type_ = INT;
        }
        Value(long long val){
            size_ = 8; 
            //content_ = new char[size_];
            this->content_ = (char*) malloc(size_);
            memcpy(content_, &val, size_);
            type_ = BIGINT;
        }
        Value(float val){
            size_ = 4; 
            //content_ = new char[size_];
            this->content_ = (char*) malloc(size_);
            memcpy(content_, &val, size_);
            type_ = FLOAT;
        }
        Value(double val){
            size_ = 8; 
            //content_ = new char[size_];
            this->content_ = (char*) malloc(size_);
            memcpy(content_, &val, size_);
            type_ = DOUBLE;
        }


        std::string toString() const {
            switch (type_) {
                case VARCHAR: 
                    return getStringVal(); 
                case BOOLEAN:
                    return getBoolVal() ? "true" : "false";
                case INT:
                    return intToStr(getIntVal());
                case BIGINT:
                    return intToStr(getBigIntVal());
                case FLOAT:
                    return floatToStr(getFloatVal());
                case DOUBLE:
                    return doubleToStr(getDoubleVal());
                case NULL_TYPE:
                    return "NULL";
                case INVALID:
                    return "INVALID";
                default :
                    return "NOT SUPPORTED YET";
            }

        }

        inline bool isInvalid() const {
            return (type_ == INVALID);
        }
        inline bool isNull() const {
            return (type_ == NULL_TYPE);
        }
        std::string getStringVal() const {
            if(!content_) return "";
            return std::string(content_, size_);  
        }
        bool getBoolVal() const{
            if(!content_) return false;
            if(type_ == FLOAT){
                return getFloatVal();
            } else if(type_ == DOUBLE){
                return getDoubleVal();
            } else if(type_ == INT){
                return getIntVal();
            }
            return *reinterpret_cast<bool*>(content_);
        }
        int getIntVal() const {
            if(!content_) return 0;
            if(type_ == FLOAT){
                return getFloatVal();
            } else if(type_ == DOUBLE){
                return getDoubleVal();
            } else if(type_ == BOOLEAN){
                return getBoolVal();
            }
            return *reinterpret_cast<int*>(content_);
        }
        long long getBigIntVal() const {
            if(!content_) return 0;
            return *reinterpret_cast<long long*>(content_);
        }
        float getFloatVal() const {
            if(!content_) return 0.0f;
            return *reinterpret_cast<float*>(content_);
        }
        double getDoubleVal() const {
            if(!content_) return 0.0f;
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
            if(isNull() && rhs.isNull()) return true;
            if(type_ != rhs.type_) return false;
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

        bool operator!=(const Value &rhs) const { 
            if(isNull() && rhs.isNull()) return false;
            if(type_ != rhs.type_) return true;
            // not the best solution to our current problem.
            switch (type_) {
                case VARCHAR: 
                    return getStringVal() != rhs.getStringVal();
                case BOOLEAN:
                    return getBoolVal()   != rhs.getBoolVal();
                case INT:
                    return getIntVal()    != rhs.getIntVal();
                case BIGINT:
                    return getBigIntVal() != rhs.getBigIntVal();
                case FLOAT:
                    return getFloatVal()  != rhs.getFloatVal();
                case DOUBLE:
                    return getDoubleVal() != rhs.getDoubleVal();
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

        bool operator<=(const Value &rhs) const { 
            if(!checkSameType(type_, rhs.type_)) return false;
            // not the best solution to our current problem.
            switch (type_) {
                case VARCHAR: 
                    return getStringVal() <= rhs.getStringVal();
                case BOOLEAN:
                    return getBoolVal()   <= rhs.getBoolVal();
                case INT:
                    return getIntVal()    <= rhs.getIntVal();
                case BIGINT:
                    return getBigIntVal() <= rhs.getBigIntVal();
                case FLOAT:
                    return getFloatVal()  <= rhs.getFloatVal();
                case DOUBLE:
                    return getDoubleVal() <= rhs.getDoubleVal();
                default :
                    return false;
            }
        }

        // return true of lhs (this) > rhs else return false.
        bool operator>(const Value &rhs) const { 
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

        bool operator>=(const Value &rhs) const { 
            // not the best solution to our current problem.
            switch (type_) {
                case VARCHAR: 
                    return getStringVal() >= rhs.getStringVal();
                case BOOLEAN:
                    return getBoolVal()   >= rhs.getBoolVal();
                case INT:
                    return getIntVal()    >= rhs.getIntVal();
                case BIGINT:
                    return getBigIntVal() >= rhs.getBigIntVal();
                case FLOAT:
                    return getFloatVal()  >= rhs.getFloatVal();
                case DOUBLE:
                    return getDoubleVal() >= rhs.getDoubleVal();
                default :
                    return false;
            }
        }

        void setValue(int val){
            if(type_ == INT)
                memcpy(content_, &val, 4);
        }

        void setValue(long long val){
            if(type_ == BIGINT)
                memcpy(content_, &val, 8);
        }

        void setValue(float val){
            if(type_ == FLOAT)
                memcpy(content_, &val, 4);
        }

        void setValue(double val){
            if(type_ == DOUBLE)
                memcpy(content_, &val, 8);
        }

        Value& operator++() {
            switch (type_) {
                case INT:
                    setValue(getIntVal()+1);
                case BIGINT:
                    setValue(getBigIntVal()+1);
                case FLOAT:
                    setValue(getFloatVal()+1);
                case DOUBLE:
                    setValue(getDoubleVal()+1);
                default :
                    ;
            }
            return *this;
        }
        Value& operator+=(const Value& rhs) {
            if(!checkSameType(this->type_, rhs.type_)) return *this;
            switch (type_) {
                case INT:
                    setValue(getIntVal()+rhs.getIntVal());
                case BIGINT:
                    setValue(getBigIntVal()+rhs.getBigIntVal());
                case FLOAT:
                    setValue(getFloatVal()+rhs.getFloatVal());
                case DOUBLE:
                    setValue(getDoubleVal()+rhs.getDoubleVal());
                default :
                    ;
            }
            return *this;
        }
};
