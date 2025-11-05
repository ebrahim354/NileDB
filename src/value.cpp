#pragma once
#include <cstdint>
#include <string>
#include <memory.h>
#include "column.cpp"
#include "utils.cpp"

#define EPS 1e-6
class Value;
int value_cmp(Value lhs, Value rhs);

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

        Value& operator+=(const Value& rhs){
            if(rhs.isNull() ||  isNull()) {
                type_ = NULL_TYPE;
                return *this;
            } 
            switch (type_) {
                case INT : {
                               if(rhs.type_ == INT) {
                                   assert(size_ == 4 && rhs.size_ == 4);
                                   *(int*)content_ += rhs.getIntVal();
                               } else if(rhs.type_ == FLOAT) {
                                   assert(size_ == 4 && rhs.size_ == 4);
                                   float val = *(int*)content_;
                                   val += rhs.getFloatVal();
                                   *(float*)content_ = val; 
                                   type_ = FLOAT;
                               } else {
                                   assert(0 && "NOT SUPPORTED TYPE CONVERSION");
                               }
                           }
                    break;
                case FLOAT:{
                               if(rhs.type_ == INT) {
                                   assert(size_ == 4 && rhs.size_ == 4);
                                   *(float*)content_ += rhs.getIntVal();
                               } else if(rhs.type_ == FLOAT) {
                                   assert(size_ == 4 && rhs.size_ == 4);
                                   *(float*)content_ += rhs.getFloatVal();
                               } else {
                                   assert(0 && "NOT SUPPORTED TYPE CONVERSION");
                               }
                           }
                    break;
                case BIGINT:
                case VARCHAR: 
                case BOOLEAN:
                case DOUBLE:
                case NULL_TYPE:
                case INVALID:
                default :
                     assert(0 && "OPERATOR NOT SUPPORTED FOR THIS TYPE!");
            }
            return *this;
        } 

        Value& operator-=(const Value& rhs){
            if(rhs.isNull() ||  isNull()) {
                type_ = NULL_TYPE;
                return *this;
            } 
            switch (type_) {
                case INT : {
                               if(rhs.type_ == INT) {
                                   assert(size_ == 4 && rhs.size_ == 4);
                                   *(int*)content_ -= rhs.getIntVal();
                               } else if(rhs.type_ == FLOAT) {
                                   assert(size_ == 4 && rhs.size_ == 4);
                                   float val = *(int*)content_;
                                   val -= rhs.getFloatVal();
                                   *(float*)content_ = val; 
                                   type_ = FLOAT;
                               } else {
                                   assert(0 && "NOT SUPPORTED TYPE CONVERSION");
                               }
                           }
                    break;
                case FLOAT:{
                               if(rhs.type_ == INT) {
                                   assert(size_ == 4 && rhs.size_ == 4);
                                   *(float*)content_ -= rhs.getIntVal();
                               } else if(rhs.type_ == FLOAT) {
                                   assert(size_ == 4 && rhs.size_ == 4);
                                   *(float*)content_ -= rhs.getFloatVal();
                               } else {
                                   assert(0 && "NOT SUPPORTED TYPE CONVERSION");
                               }
                           }
                    break;
                case BIGINT:
                case VARCHAR: 
                case BOOLEAN:
                case DOUBLE:
                case NULL_TYPE:
                case INVALID:
                default :
                     assert(0 && "OPERATOR NOT SUPPORTED FOR THIS TYPE!");
            }
            return *this;
        } 

        Value& operator*=(const Value& rhs){
            if(rhs.isNull() ||  isNull()) {
                type_ = NULL_TYPE;
                return *this;
            } 
            switch (type_) {
                case INT : {
                               if(rhs.type_ == INT) {
                                   assert(size_ == 4 && rhs.size_ == 4);
                                   *(int*)content_ *= rhs.getIntVal();
                               } else if(rhs.type_ == FLOAT) {
                                   assert(size_ == 4 && rhs.size_ == 4);
                                   float val = *(int*)content_;
                                   val *= rhs.getFloatVal();
                                   *(float*)content_ = val; 
                                   type_ = FLOAT;
                               } else {
                                   assert(0 && "NOT SUPPORTED TYPE CONVERSION");
                               }
                           }
                    break;
                case FLOAT:{
                               if(rhs.type_ == INT) {
                                   assert(size_ == 4 && rhs.size_ == 4);
                                   *(float*)content_ *= rhs.getIntVal();
                               } else if(rhs.type_ == FLOAT) {
                                   assert(size_ == 4 && rhs.size_ == 4);
                                   *(float*)content_ *= rhs.getFloatVal();
                               } else {
                                   assert(0 && "NOT SUPPORTED TYPE CONVERSION");
                               }
                           }
                    break;
                case BIGINT:
                case VARCHAR: 
                case BOOLEAN:
                case DOUBLE:
                case NULL_TYPE:
                case INVALID:
                default :
                     assert(0 && "OPERATOR NOT SUPPORTED FOR THIS TYPE!");
            }
            return *this;
        } 

        Value& operator/=(const Value& rhs){
            if(rhs.isNull() ||  isNull()) {
                type_ = NULL_TYPE;
                return *this;
            } 
            switch (type_) {
                case INT : {
                               if(rhs.type_ == INT) {
                                   int rhs_int = rhs.getIntVal();
                                   assert(size_ == 4 && rhs.size_ == 4 && rhs_int != 0);
                                   *(int*)content_ /= rhs_int;
                               } else if(rhs.type_ == FLOAT) {
                                   float rhs_float = rhs.getFloatVal();
                                   assert(size_ == 4 && rhs.size_ == 4 && rhs_float != 0.0);
                                   float val = *(int*)content_;
                                   val /= rhs_float; 
                                   *(float*)content_ = val; 
                                   type_ = FLOAT;
                               } else {
                                   assert(0 && "NOT SUPPORTED TYPE CONVERSION");
                               }
                           }
                    break;
                case FLOAT:{
                               if(rhs.type_ == INT) {
                                   int rhs_int = rhs.getIntVal();
                                   assert(size_ == 4 && rhs.size_ == 4 && rhs_int != 0);
                                   *(float*)content_ /= rhs_int;
                               } else if(rhs.type_ == FLOAT) {
                                   float rhs_float = rhs.getFloatVal();
                                   assert(size_ == 4 && rhs.size_ == 4 && rhs_float != 0.0);
                                   *(float*)content_ /= rhs_float;
                               } else {
                                   assert(0 && "NOT SUPPORTED TYPE CONVERSION");
                               }
                           }
                    break;
                case BIGINT:
                case VARCHAR: 
                case BOOLEAN:
                case DOUBLE:
                case NULL_TYPE:
                case INVALID:
                default :
                     assert(0 && "OPERATOR NOT SUPPORTED FOR THIS TYPE!");
            }
            return *this;
        } 

        Value operator-(Value rhs) {
            assert(size_ == rhs.size_);
            if(rhs.isNull() ||  isNull()) {
                return Value(NULL_TYPE);
            } 
            switch (type_) {
                case INT : {
                               if(rhs.type_ == INT) {
                                   return Value(*(int*)content_ - rhs.getIntVal());
                               } else if(rhs.type_ == FLOAT) {
                                   float val = *(int*)content_;
                                   return Value(val - rhs.getFloatVal());
                               } else {
                                   assert(0 && "NOT SUPPORTED TYPE CONVERSION");
                               }
                           }
                    break;
                case FLOAT:{
                               if(rhs.type_ == INT) {
                                   return Value(*(float*)content_ - rhs.getIntVal());
                               } else if(rhs.type_ == FLOAT) {
                                   return Value(*(float*)content_ - rhs.getFloatVal());
                               } else {
                                   assert(0 && "NOT SUPPORTED TYPE CONVERSION");
                               }
                           }
                    break;
                case BOOLEAN:{
                                 if(rhs.type_ == BOOLEAN){
                                    return Value((int)(*(bool*)content_ - rhs.getBoolVal()));
                                 } else {
                                   assert(0 && "NOT SUPPORTED TYPE CONVERSION");
                                 }
                             }
                    break;
                case BIGINT:
                case VARCHAR: 
                case DOUBLE:
                case NULL_TYPE:
                case INVALID:
                default :
                     assert(0 && "OPERATOR NOT SUPPORTED FOR THIS TYPE!");
            }
            return Value(NULL_TYPE);
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
        bool operator==(const Value &rhs) const { 
            int cmp = value_cmp(*this, rhs);
            return cmp == 0;
        }

        bool operator!=(const Value &rhs) const { 
            int cmp = value_cmp(*this, rhs);
            return cmp != 0;
        }

        bool operator<(const Value &rhs) const { 
            int cmp = value_cmp(*this, rhs);
            return cmp < 0;
        }

        bool operator<=(const Value &rhs) const { 
            int cmp = value_cmp(*this, rhs);
            return cmp <= 0;
        }

        bool operator>(const Value &rhs) const { 
            int cmp = value_cmp(*this, rhs);
            return cmp > 0;
        }

        bool operator>=(const Value &rhs) const { 
            int cmp = value_cmp(*this, rhs);
            return cmp >= 0;
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
};

// -1 ==> lhs < rhs, 0 eq, 1 ==> lhs > rhs
int value_cmp(Value lhs, Value rhs) {
    assert(!lhs.isNull() && !rhs.isNull()); // can't compare null values.
    if(lhs.content_  && !rhs.content_ ) return 1;
    if(rhs.content_  && !lhs.content_ ) return -1;
    if(!rhs.content_ && !lhs.content_ ) return 0;

    if(lhs.size_ != rhs.size_){
        assert(0 && "INVALID COMPARISON");
    }
    Value v = lhs - rhs;
    int diff = 0;
    if(v.type_ == FLOAT){
        float fval = v.getFloatVal();
        if(fabsf(fval) > EPS){
            if(fval < 0.0) diff = -1;
            else diff = 1;
        }
    } else if(v.type_ == INT){
        diff = v.getIntVal();
    } else {
        assert(0 && "Type comparison not supported yet!");
    }
    return diff;
}
