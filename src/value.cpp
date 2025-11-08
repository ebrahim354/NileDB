#pragma once
#include <cstdint>
#include <string>
#include <memory.h>
#include "column.cpp"
#include "utils.cpp"

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
// check for overflows and underflows for 64 bi.
bool is_valid_operation64(int64_t a, int64_t b, MathOp op) {
    switch(op){
        case MATH_MINUS:  b = -b;
        case MATH_PLUS: {
                            if((a >= 0 && b < 0) || (b >= 0 && a < 0)) 
                                return true;
                            if(a > INT64_MX - b || a < INT64_MN - b) 
                                return false;
                        } break;
        case MATH_MULT: {
                            if(b == 0 || a == 0) return true;
                            if(a > 0 && b > 0 &&  a > INT64_MX/b) 
                                return false;
                            if(a < 0 && b < 0 &&  a < INT64_MX/b) 
                                return false;
                            if(a > 0 && b < 0 &&  a < INT64_MN/b) 
                                return false;
                            if(a < 0 && b > 0 &&  b < INT64_MN/a) 
                                return false;
                        } break;
        case MATH_DIV: 
                        return b != 0;
    }
    return true;
}

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

        Value& operator=(const Value& rhs) {
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

        bool cast_up() {
            if(type_ == INT){
                long val = *(int*)content_;
                free(content_);
                size_ = 8;
                content_ = (char*) malloc(size_);
                *(long*)content_ = val;
                type_ = BIGINT;
                return true;
            } 
            if(type_ == FLOAT){
                double val = *(float*)content_;
                free(content_);
                size_ = 8;
                content_ = (char*) malloc(size_);
                *(double*)content_ = val;
                type_ = DOUBLE;
                return true;
            } 
            if(type_ == BIGINT){
                double val = *(int64_t*)content_;
                *(double*) content_ = val;
                type_ = DOUBLE;
                return true;
            }
            assert(0);
            return false;
        }


        Value& operator+=(const Value& rhs) {
            if(rhs.isNull() ||  isNull()) {
                type_ = NULL_TYPE;
                return *this;
            } 
            for(;;){
                switch (type_) {
                    // TODO: check for overflows, underflows in double, bigint.
                    case DOUBLE:{
                                    if(rhs.type_ == INT || rhs.type_ == BIGINT) {
                                        *(double*)content_ += rhs.type_ == INT ? rhs.getIntVal() : rhs.getBigIntVal();
                                    } else if(rhs.type_ == FLOAT) {
                                        *(double*)content_ += rhs.getFloatVal();
                                    } else if(rhs.type_ == DOUBLE){
                                        *(double*)content_ += rhs.getDoubleVal();
                                    } else {
                                        assert(0 && "NOT SUPPORTED TYPE CONVERSION");
                                    }
                                }
                                return *this;
                    case BIGINT:{
                                    if(rhs.type_ == INT) {
                                        if(!is_valid_operation64(*(long*)content_, rhs.getIntVal(), MATH_PLUS)){
                                            cast_up();
                                            continue;
                                        }
                                        *(long*)content_ += rhs.getIntVal();
                                    } else if(rhs.type_ == FLOAT || rhs.type_ == DOUBLE) {
                                        double val = getBigIntVal();
                                        val += rhs.type_ == FLOAT ? rhs.getFloatVal() : rhs.getDoubleVal();
                                        *(double*)content_ = val; 
                                        type_ = DOUBLE;
                                    } else if(rhs.type_ == BIGINT) {
                                        if(!is_valid_operation64(*(long*)content_, rhs.getBigIntVal(), MATH_PLUS)){
                                            cast_up();
                                            continue;
                                        }
                                        *(long*)content_ += rhs.getBigIntVal();
                                    } else {
                                        assert(0 && "NOT SUPPORTED TYPE CONVERSION");
                                    }
                                }
                                return *this;
                    case INT : {
                                   if(rhs.type_ == INT) {
                                       long res = (long) getIntVal() + (long) rhs.getIntVal();
                                       if(res > INT32_MX || res < INT32_MN) {
                                           cast_up();
                                           continue;
                                       }
                                       *(int*)content_ = (int) res; 
                                   } else if(rhs.type_ == FLOAT) {
                                       float res = (float) getIntVal() + rhs.getFloatVal();
                                       if(isinf(res) || isnan(res)) {
                                           cast_up();
                                           continue;
                                       }
                                       *(float*)content_ = (float) res; 
                                       type_ = FLOAT;
                                   } else if(rhs.type_ == BIGINT || rhs.type_ == DOUBLE) {
                                       cast_up();
                                       continue;
                                   } else {
                                       assert(0 && "NOT SUPPORTED TYPE CONVERSION");
                                   }
                               }
                               return *this;
                    case FLOAT:{
                                   if(rhs.type_ == INT) {
                                       float res = getFloatVal() + rhs.getIntVal();
                                       if(isinf(res) || isnan(res)) {
                                           cast_up();
                                           continue;
                                       }
                                       *(float*)content_ = res;
                                   } else if(rhs.type_ == FLOAT) {
                                       float res = getFloatVal() + rhs.getFloatVal();
                                       if(isinf(res) || isnan(res)) {
                                           cast_up();
                                           continue;
                                       }
                                       *(float*)content_ = res; 
                                   } else if(rhs.type_ == BIGINT || rhs.type_ == DOUBLE) {
                                       cast_up();
                                       continue;
                                   } else {
                                       assert(0 && "NOT SUPPORTED TYPE CONVERSION");
                                   }
                               }
                               return *this;
                    case VARCHAR: 
                    case BOOLEAN:
                    case NULL_TYPE:
                    case INVALID:
                    default :
                               assert(0 && "OPERATOR NOT SUPPORTED FOR THIS TYPE!");
                }
            }
            return *this;
        } 

        Value& operator-=(const Value& rhs){
            if(rhs.isNull() ||  isNull()) {
                type_ = NULL_TYPE;
                return *this;
            } 
            for(;;){
                switch (type_) {
                    // TODO: check for overflows, underflows in double, bigint.
                    case DOUBLE:{
                                    if(rhs.type_ == INT || rhs.type_ == BIGINT) {
                                        *(double*)content_ -= rhs.type_ == INT ? rhs.getIntVal() : rhs.getBigIntVal();
                                    } else if(rhs.type_ == FLOAT) {
                                        *(double*)content_ -= rhs.getFloatVal();
                                    } else if(rhs.type_ == DOUBLE){
                                        *(double*)content_ -= rhs.getDoubleVal();
                                    } else {
                                        assert(0 && "NOT SUPPORTED TYPE CONVERSION");
                                    }
                                }
                                return *this;
                    case BIGINT:{
                                    if(rhs.type_ == INT) {
                                        if(!is_valid_operation64(*(long*)content_, rhs.getIntVal(), MATH_MINUS)){
                                            cast_up();
                                            continue;
                                        }
                                        *(long*)content_ -= rhs.getIntVal();
                                    } else if(rhs.type_ == FLOAT || rhs.type_ == DOUBLE) {
                                        double val = getBigIntVal();
                                        val -= rhs.type_ == FLOAT ? rhs.getFloatVal() : rhs.getDoubleVal();
                                        *(double*)content_ = val; 
                                        type_ = DOUBLE;
                                    } else if(rhs.type_ == BIGINT) {
                                        if(!is_valid_operation64(*(long*)content_, rhs.getBigIntVal(), MATH_MINUS)){
                                            cast_up();
                                            continue;
                                        }
                                        *(long*)content_ -= rhs.getBigIntVal();
                                    } else {
                                        assert(0 && "NOT SUPPORTED TYPE CONVERSION");
                                    }
                                }
                                return *this;
                    case INT : {
                                   if(rhs.type_ == INT) {
                                       long res = (long) getIntVal() - (long) rhs.getIntVal();
                                       if(res > INT32_MX || res < INT32_MN) {
                                           cast_up();
                                           continue;
                                       }
                                       *(int*)content_ = (int) res; 
                                   } else if(rhs.type_ == FLOAT) {
                                       float res = (float) getIntVal() - rhs.getFloatVal();
                                       if(isinf(res) || isnan(res)) {
                                           cast_up();
                                           continue;
                                       }
                                       *(float*)content_ = (float) res; 
                                       type_ = FLOAT;
                                   } else if(rhs.type_ == BIGINT || rhs.type_ == DOUBLE) {
                                       cast_up();
                                       continue;
                                   } else {
                                       assert(0 && "NOT SUPPORTED TYPE CONVERSION");
                                   }
                               }
                               return *this;
                    case FLOAT:{
                                   if(rhs.type_ == INT) {
                                       float res = getFloatVal() - rhs.getIntVal();
                                       if(isinf(res) || isnan(res)) {
                                           cast_up();
                                           continue;
                                       }
                                       *(float*)content_ = res;
                                   } else if(rhs.type_ == FLOAT) {
                                       float res = getFloatVal() - rhs.getFloatVal();
                                       if(isinf(res) || isnan(res)) {
                                           cast_up();
                                           continue;
                                       }
                                       *(float*)content_ = res; 
                                   } else if(rhs.type_ == BIGINT || rhs.type_ == DOUBLE) {
                                       cast_up();
                                       continue;
                                   } else {
                                       assert(0 && "NOT SUPPORTED TYPE CONVERSION");
                                   }
                               }
                               return *this;
                    case VARCHAR: 
                    case BOOLEAN:
                    case NULL_TYPE:
                    case INVALID:
                    default :
                               assert(0 && "OPERATOR NOT SUPPORTED FOR THIS TYPE!");
                }
            }
            return *this;
        } 

        Value& operator*=(const Value& rhs){
            if(rhs.isNull() ||  isNull()) {
                type_ = NULL_TYPE;
                return *this;
            } 
            for(;;){
                switch (type_) {
                    // TODO: check for overflows, underflows in double, bigint.
                    case DOUBLE:{
                                    if(rhs.type_ == INT || rhs.type_ == BIGINT) {
                                        *(double*)content_ *= rhs.type_ == INT ? rhs.getIntVal() : rhs.getBigIntVal();
                                    } else if(rhs.type_ == FLOAT) {
                                        *(double*)content_ *= rhs.getFloatVal();
                                    } else if(rhs.type_ == DOUBLE){
                                        *(double*)content_ *= rhs.getDoubleVal();
                                    } else {
                                        assert(0 && "NOT SUPPORTED TYPE CONVERSION");
                                    }
                                }
                                return *this;
                    case BIGINT:{
                                    if(rhs.type_ == INT) {
                                        if(!is_valid_operation64(*(long*)content_, rhs.getIntVal(), MATH_MULT)){
                                            cast_up();
                                            continue;
                                        }
                                        *(long*)content_ *= rhs.getIntVal();
                                    } else if(rhs.type_ == FLOAT || rhs.type_ == DOUBLE) {
                                        double val = getBigIntVal();
                                        val *= rhs.type_ == FLOAT ? rhs.getFloatVal() : rhs.getDoubleVal();
                                        *(double*)content_ = val; 
                                        type_ = DOUBLE;
                                    } else if(rhs.type_ == BIGINT) {
                                        if(!is_valid_operation64(*(long*)content_, rhs.getBigIntVal(), MATH_MULT)){
                                            cast_up();
                                            continue;
                                        }
                                        *(long*)content_ *= rhs.getBigIntVal();
                                    } else {
                                        assert(0 && "NOT SUPPORTED TYPE CONVERSION");
                                    }
                                }
                                return *this;
                    case INT : {
                                   if(rhs.type_ == INT) {
                                       long res = (long) getIntVal() * (long) rhs.getIntVal();
                                       if(res > INT32_MX || res < INT32_MN) {
                                           cast_up();
                                           continue;
                                       }
                                       *(int*)content_ = (int) res; 
                                   } else if(rhs.type_ == FLOAT) {
                                       float res = (float) getIntVal() * rhs.getFloatVal();
                                       if(isinf(res) || isnan(res)) {
                                           cast_up();
                                           continue;
                                       }
                                       *(float*)content_ = (float) res; 
                                       type_ = FLOAT;
                                   } else if(rhs.type_ == BIGINT || rhs.type_ == DOUBLE) {
                                       cast_up();
                                       continue;
                                   } else {
                                       assert(0 && "NOT SUPPORTED TYPE CONVERSION");
                                   }
                               }
                               return *this;
                    case FLOAT:{
                                   if(rhs.type_ == INT) {
                                       float res = getFloatVal() * rhs.getIntVal();
                                       if(isinf(res) || isnan(res)) {
                                           cast_up();
                                           continue;
                                       }
                                       *(float*)content_ = res;
                                   } else if(rhs.type_ == FLOAT) {
                                       float res = getFloatVal() * rhs.getFloatVal();
                                       if(isinf(res) || isnan(res)) {
                                           cast_up();
                                           continue;
                                       }
                                       *(float*)content_ = res;
                                   } else if(rhs.type_ == BIGINT || rhs.type_ == DOUBLE) {
                                       cast_up();
                                       continue;
                                   } else {
                                       assert(0 && "NOT SUPPORTED TYPE CONVERSION");
                                   }
                               }
                               return *this;
                    case VARCHAR: 
                    case BOOLEAN:
                    case NULL_TYPE:
                    case INVALID:
                    default :
                               assert(0 && "OPERATOR NOT SUPPORTED FOR THIS TYPE!");
                }
            }
            return *this;
        } 

        Value& operator/=(const Value& rhs){
            if(rhs.isNull() ||  isNull()) {
                type_ = NULL_TYPE;
                return *this;
            } 
            for(;;){
                switch (type_) {
                    // TODO: check for overflows, underflows in double, bigint.
                    case DOUBLE:{
                                    if(rhs.type_ == INT || rhs.type_ == BIGINT) {
                                        long val = rhs.type_ == INT ? rhs.getIntVal() : rhs.getBigIntVal();
                                        assert(val != 0 && "division by 0");
                                        *(double*)content_ /= val;
                                    } else if(rhs.type_ == FLOAT) {
                                        float val = rhs.getFloatVal(); 
                                        assert(val != 0 && "division by 0");
                                        *(double*)content_ /= val; 
                                    } else if(rhs.type_ == DOUBLE){
                                        double val = rhs.getDoubleVal(); 
                                        assert(val != 0 && "division by 0");
                                        *(double*)content_ /= val;
                                    } else {
                                        assert(0 && "NOT SUPPORTED TYPE CONVERSION");
                                    }
                                }
                                return *this;
                    case BIGINT:{
                                    if(rhs.type_ == INT) {
                                        long val = rhs.getIntVal();
                                        assert(val != 0 && "division by 0");
                                        if(!is_valid_operation64(*(long*)content_, rhs.getIntVal(), MATH_DIV)){
                                            cast_up();
                                            continue;
                                        }
                                        *(long*)content_ /= val; 
                                    } else if(rhs.type_ == FLOAT || rhs.type_ == DOUBLE) {
                                        double val = getBigIntVal();
                                        double denom = rhs.type_ == FLOAT ? rhs.getFloatVal() : rhs.getDoubleVal();
                                        assert(denom != 0 && "division by 0");
                                        val /= denom;
                                        *(double*)content_ = val; 
                                        type_ = DOUBLE;
                                    } else if(rhs.type_ == BIGINT) {
                                        long val = rhs.getBigIntVal();
                                        assert(val != 0 && "division by 0");
                                        if(!is_valid_operation64(*(long*)content_, rhs.getBigIntVal(), MATH_DIV)){
                                            cast_up();
                                            continue;
                                        }
                                        *(long*)content_ /= val;
                                    } else {
                                        assert(0 && "NOT SUPPORTED TYPE CONVERSION");
                                    }
                                }
                                return *this;
                    case INT : {
                                   if(rhs.type_ == INT) {
                                       assert(rhs.getIntVal() != 0 && "division by 0");
                                       long res = (long) getIntVal() / (long) rhs.getIntVal();
                                       if(res > INT32_MX || res < INT32_MN) {
                                           cast_up();
                                           continue;
                                       }
                                       *(int*)content_ = (int) res; 
                                   } else if(rhs.type_ == FLOAT) {
                                       assert(rhs.getFloatVal() != 0 && "division by 0");
                                       float res = (float) getIntVal() / rhs.getFloatVal();
                                       if(isinf(res) || isnan(res)) {
                                           cast_up();
                                           continue;
                                       }
                                       *(float*)content_ = (float) res; 
                                       type_ = FLOAT;
                                   } else if(rhs.type_ == BIGINT || rhs.type_ == DOUBLE) {
                                       cast_up();
                                       continue;
                                   } else {
                                       assert(0 && "NOT SUPPORTED TYPE CONVERSION");
                                   }
                               }
                               return *this;
                    case FLOAT:{
                                   if(rhs.type_ == INT) {
                                       assert(rhs.getIntVal() != 0 && "division by 0");
                                       float res = getFloatVal() / rhs.getIntVal();
                                       if(isinf(res) || isnan(res)) {
                                           cast_up();
                                           continue;
                                       }
                                       *(float*)content_ = res;
                                   } else if(rhs.type_ == FLOAT) {
                                       assert(rhs.getFloatVal() != 0 && "division by 0");
                                       float res = getFloatVal() / rhs.getFloatVal();
                                       if(isinf(res) || isnan(res)) {
                                           cast_up();
                                           continue;
                                       }
                                       *(float*)content_ = res; 
                                   } else if(rhs.type_ == BIGINT || rhs.type_ == DOUBLE) {
                                       cast_up();
                                       continue;
                                   } else {
                                       assert(0 && "NOT SUPPORTED TYPE CONVERSION");
                                   }
                               }
                               return *this;
                    case VARCHAR: 
                    case BOOLEAN:
                    case NULL_TYPE:
                    case INVALID:
                    default :
                               assert(0 && "OPERATOR NOT SUPPORTED FOR THIS TYPE!");
                }
            }
            return *this;
        }

        Value operator-(Value rhs) {
            assert(size_ == rhs.size_);
            if(rhs.isNull() ||  isNull()) {
                return Value(NULL_TYPE);
            } 
            switch (type_) {
                    // TODO: check for overflows, underflows in double, bigint.
                    case DOUBLE:{
                                    if(rhs.type_ == INT || rhs.type_ == BIGINT) {
                                        return Value(
                                                getDoubleVal() - (
                                                        rhs.type_ == INT ? 
                                                        rhs.getIntVal() : rhs.getBigIntVal()
                                                        )
                                                );
                                    } else if(rhs.type_ == FLOAT) {
                                        return Value(getDoubleVal() - rhs.getFloatVal());
                                    } else if(rhs.type_ == DOUBLE){
                                        return Value(getDoubleVal() - rhs.getDoubleVal());
                                    } else {
                                        assert(0 && "NOT SUPPORTED TYPE CONVERSION");
                                    }
                                }
                    case BIGINT:{
                                    if(rhs.type_ == INT) {
                                        return Value(getBigIntVal() - rhs.getIntVal());
                                    } else if(rhs.type_ == FLOAT || rhs.type_ == DOUBLE) {
                                        double val = getBigIntVal();
                                        val -= rhs.type_ == FLOAT ? rhs.getFloatVal() : rhs.getDoubleVal();
                                        return Value(val);
                                    } else if(rhs.type_ == BIGINT) {
                                        return Value(getBigIntVal() - rhs.getBigIntVal());
                                    } else {
                                        assert(0 && "NOT SUPPORTED TYPE CONVERSION");
                                    }
                                }
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
                case VARCHAR: 
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
                    //return getBoolVal() ? "1" : "0";
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

    if((lhs.type_ == BIGINT || lhs.type_ == DOUBLE) && (rhs.type_ == FLOAT || rhs.type_ == INT))
        rhs.cast_up();

    if((rhs.type_ == BIGINT || rhs.type_ == DOUBLE) && (lhs.type_ == FLOAT || lhs.type_ == INT))
        lhs.cast_up();

    if(lhs.size_ != rhs.size_){
        assert(0 && "INVALID COMPARISON");
    }
    Value v = lhs - rhs;
    int diff = 0;
    if(v.type_ == FLOAT || v.type_ == DOUBLE){
        double dval = (v.type_ == FLOAT ? v.getFloatVal() : v.getDoubleVal());
        if(fabs(dval) > EPS){
            if(dval < 0.0) diff = -1;
            else diff = 1;
        }
    } else if(v.type_ == INT || v.type_ == BIGINT){
        diff = v.type_ == INT ? v.getIntVal() : v.getBigIntVal();
    } else {
        assert(0 && "Type comparison not supported yet!");
    }
    return diff;
}
