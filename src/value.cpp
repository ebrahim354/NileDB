#pragma once
#include <cstdint>
#include <string>
#include "memory.h"
#include "column.cpp"
#include "utils.h"
#include "value.h"



#define cast_as(type, ptr) *((type*)&ptr)

// check for overflows and underflows for 64 bit.
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

Value::~Value(){}

Value::Value(Type type){
    type_ = type;
} 

Value::Value(char* content, Type t, u32 size) {
    type_ = t;
    size_ = size;
    setValue(t, content);
}

Value::Value(char* str, u32 size) {
    content_ = (uintptr_t)str;
    size_ = size;
    type_ = VARCHAR;
}

char* Value::get_ptr() const {
    if(type_ == VARCHAR) return (char*) content_;
    return (char*)&content_;
}

Value::Value(Arena* arena, const String& str){
    size_ = str.size(); 
    this->content_ = (uintptr_t) arena->alloc(size_);
    memcpy((char*)content_, str.c_str(), size_);
    type_ = VARCHAR;
}

Value::Value(String8 str){
    size_ = str.size_; 
    content_ = (uintptr_t)str.str_;
    type_ = VARCHAR;
}

Value Value::get_copy(Arena* arena) { 
    if(type_ != VARCHAR) return *this; // copy by value if we don't have a var length type.
    char* tmp = (char*)arena->alloc(size_);
    memcpy(tmp, (char*)content_, size_);
    return Value(tmp, VARCHAR, size_);
}

bool Value::cast_up() {
    if(type_ == INT){
        int64_t val = cast_as(int, content_);
        size_ = 8;
        type_ = BIGINT;
        setBigintValue(val);
        return true;
    } 
    if(type_ == FLOAT){
        double val = cast_as(float, content_);;
        size_ = 8;
        type_ = DOUBLE;
        setDoubleValue(val);
        return true;
    } 
    if(type_ == BIGINT){
        double val = cast_as(int64_t, content_);
        type_ = DOUBLE;
        size_ = 8;
        setDoubleValue(val);
        return true;
    }
    assert(0);
    return false;
}


Value& Value::operator+=(const Value& rhs) {
    if(rhs.isNull() ||  isNull()) {
        type_ = NULL_TYPE;
        return *this;
    } 
    for(;;){
        switch (type_) {
            // TODO: check for overflows, underflows in double, bigint.
            case DOUBLE:{
                            if(rhs.type_ == INT || rhs.type_ == BIGINT) {
                                cast_as(double, content_) += rhs.type_ == INT ? rhs.getIntVal() : rhs.getBigIntVal();
                            } else if(rhs.type_ == FLOAT) {
                                cast_as(double, content_) += rhs.getFloatVal();
                            } else if(rhs.type_ == DOUBLE){
                                cast_as(double, content_) += rhs.getDoubleVal();
                            } else {
                                assert(0 && "NOT SUPPORTED TYPE CONVERSION");
                            }
                        }
                        return *this;
            case BIGINT:{
                            if(rhs.type_ == INT) {
                                if(!is_valid_operation64(cast_as(long, content_), rhs.getIntVal(), MATH_PLUS)){
                                    cast_up();
                                    continue;
                                }
                                cast_as(long, content_) += rhs.getIntVal();
                            } else if(rhs.type_ == FLOAT || rhs.type_ == DOUBLE) {
                                double val = getBigIntVal();
                                val += rhs.type_ == FLOAT ? rhs.getFloatVal() : rhs.getDoubleVal();
                                cast_as(double, content_) = val; 
                                type_ = DOUBLE;
                            } else if(rhs.type_ == BIGINT) {
                                if(!is_valid_operation64(cast_as(long, content_), rhs.getBigIntVal(), MATH_PLUS)){
                                    cast_up();
                                    continue;
                                }
                                cast_as(long, content_) += rhs.getBigIntVal();
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
                               cast_as(int, content_) = (int) res; 
                           } else if(rhs.type_ == FLOAT) {
                               float res = (float) getIntVal() + rhs.getFloatVal();
                               if(isinf(res) || isnan(res)) {
                                   cast_up();
                                   continue;
                               }
                               cast_as(float, content_) = (float) res; 
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
                               cast_as(float, content_) = res;
                           } else if(rhs.type_ == FLOAT) {
                               float res = getFloatVal() + rhs.getFloatVal();
                               if(isinf(res) || isnan(res)) {
                                   cast_up();
                                   continue;
                               }
                               cast_as(float, content_) = res; 
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

Value& Value::operator-=(const Value& rhs){
    if(rhs.isNull() ||  isNull()) {
        type_ = NULL_TYPE;
        return *this;
    } 
    for(;;){
        switch (type_) {
            // TODO: check for overflows, underflows in double, bigint.
            case DOUBLE:{
                            if(rhs.type_ == INT || rhs.type_ == BIGINT) {
                                cast_as(double, content_) -= rhs.type_ == INT ? rhs.getIntVal() : rhs.getBigIntVal();
                            } else if(rhs.type_ == FLOAT) {
                                cast_as(double, content_) -= rhs.getFloatVal();
                            } else if(rhs.type_ == DOUBLE){
                                cast_as(double, content_) -= rhs.getDoubleVal();
                            } else {
                                assert(0 && "NOT SUPPORTED TYPE CONVERSION");
                            }
                        }
                        return *this;
            case BIGINT:{
                            if(rhs.type_ == INT) {
                                if(!is_valid_operation64(cast_as(long, content_), rhs.getIntVal(), MATH_MINUS)){
                                    cast_up();
                                    continue;
                                }
                                cast_as(long, content_) -= rhs.getIntVal();
                            } else if(rhs.type_ == FLOAT || rhs.type_ == DOUBLE) {
                                double val = getBigIntVal();
                                val -= rhs.type_ == FLOAT ? rhs.getFloatVal() : rhs.getDoubleVal();
                                cast_as(double, content_) = val; 
                                type_ = DOUBLE;
                            } else if(rhs.type_ == BIGINT) {
                                if(!is_valid_operation64(cast_as(long, content_), rhs.getBigIntVal(), MATH_MINUS)){
                                    cast_up();
                                    continue;
                                }
                                cast_as(long, content_) -= rhs.getBigIntVal();
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
                               cast_as(int, content_) = (int) res; 
                           } else if(rhs.type_ == FLOAT) {
                               float res = (float) getIntVal() - rhs.getFloatVal();
                               if(isinf(res) || isnan(res)) {
                                   cast_up();
                                   continue;
                               }
                               cast_as(float, content_) = (float) res; 
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
                               cast_as(float, content_) = res;
                           } else if(rhs.type_ == FLOAT) {
                               float res = getFloatVal() - rhs.getFloatVal();
                               if(isinf(res) || isnan(res)) {
                                   cast_up();
                                   continue;
                               }
                               cast_as(float, content_) = res; 
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

Value& Value::operator*=(const Value& rhs){
    if(rhs.isNull() ||  isNull()) {
        type_ = NULL_TYPE;
        return *this;
    } 
    for(;;){
        switch (type_) {
            // TODO: check for overflows, underflows in double, bigint.
            case DOUBLE:{
                            if(rhs.type_ == INT || rhs.type_ == BIGINT) {
                                cast_as(double, content_) *= rhs.type_ == INT ? rhs.getIntVal() : rhs.getBigIntVal();
                            } else if(rhs.type_ == FLOAT) {
                                cast_as(double, content_) *= rhs.getFloatVal();
                            } else if(rhs.type_ == DOUBLE){
                                cast_as(double, content_) *= rhs.getDoubleVal();
                            } else {
                                assert(0 && "NOT SUPPORTED TYPE CONVERSION");
                            }
                        }
                        return *this;
            case BIGINT:{
                            if(rhs.type_ == INT) {
                                if(!is_valid_operation64(cast_as(long, content_), rhs.getIntVal(), MATH_MULT)){
                                    cast_up();
                                    continue;
                                }
                                cast_as(long, content_) *= rhs.getIntVal();
                            } else if(rhs.type_ == FLOAT || rhs.type_ == DOUBLE) {
                                double val = getBigIntVal();
                                val *= rhs.type_ == FLOAT ? rhs.getFloatVal() : rhs.getDoubleVal();
                                cast_as(double, content_) = val; 
                                type_ = DOUBLE;
                            } else if(rhs.type_ == BIGINT) {
                                if(!is_valid_operation64(cast_as(long, content_), rhs.getBigIntVal(), MATH_MULT)){
                                    cast_up();
                                    continue;
                                }
                                cast_as(long, content_) *= rhs.getBigIntVal();
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
                               cast_as(int, content_) = (int) res; 
                           } else if(rhs.type_ == FLOAT) {
                               float res = (float) getIntVal() * rhs.getFloatVal();
                               if(isinf(res) || isnan(res)) {
                                   cast_up();
                                   continue;
                               }
                               cast_as(float, content_) = (float) res; 
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
                               cast_as(float, content_) = res;
                           } else if(rhs.type_ == FLOAT) {
                               float res = getFloatVal() * rhs.getFloatVal();
                               if(isinf(res) || isnan(res)) {
                                   cast_up();
                                   continue;
                               }
                               cast_as(float, content_) = res;
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

Value& Value::operator/=(const Value& rhs){
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
                                cast_as(double, content_) /= val;
                            } else if(rhs.type_ == FLOAT) {
                                float val = rhs.getFloatVal(); 
                                assert(val != 0 && "division by 0");
                                cast_as(double, content_) /= val; 
                            } else if(rhs.type_ == DOUBLE){
                                double val = rhs.getDoubleVal(); 
                                assert(val != 0 && "division by 0");
                                cast_as(double, content_) /= val;
                            } else {
                                assert(0 && "NOT SUPPORTED TYPE CONVERSION");
                            }
                        }
                        return *this;
            case BIGINT:{
                            if(rhs.type_ == INT) {
                                long val = rhs.getIntVal();
                                assert(val != 0 && "division by 0");
                                if(!is_valid_operation64(cast_as(long, content_), rhs.getIntVal(), MATH_DIV)){
                                    cast_up();
                                    continue;
                                }
                                cast_as(long, content_) /= val; 
                            } else if(rhs.type_ == FLOAT || rhs.type_ == DOUBLE) {
                                double val = getBigIntVal();
                                double denom = rhs.type_ == FLOAT ? rhs.getFloatVal() : rhs.getDoubleVal();
                                assert(denom != 0 && "division by 0");
                                val /= denom;
                                cast_as(double, content_) = val; 
                                type_ = DOUBLE;
                            } else if(rhs.type_ == BIGINT) {
                                long val = rhs.getBigIntVal();
                                assert(val != 0 && "division by 0");
                                if(!is_valid_operation64(cast_as(long, content_), rhs.getBigIntVal(), MATH_DIV)){
                                    cast_up();
                                    continue;
                                }
                                cast_as(long, content_) /= val;
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
                               cast_as(int, content_) = (int) res; 
                           } else if(rhs.type_ == FLOAT) {
                               assert(rhs.getFloatVal() != 0 && "division by 0");
                               float res = (float) getIntVal() / rhs.getFloatVal();
                               if(isinf(res) || isnan(res)) {
                                   cast_up();
                                   continue;
                               }
                               cast_as(float, content_) = (float) res; 
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
                               cast_as(float, content_) = res;
                           } else if(rhs.type_ == FLOAT) {
                               assert(rhs.getFloatVal() != 0 && "division by 0");
                               float res = getFloatVal() / rhs.getFloatVal();
                               if(isinf(res) || isnan(res)) {
                                   cast_up();
                                   continue;
                               }
                               cast_as(float, content_) = res; 
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

Value Value::operator-(Value rhs) {
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
                            return Value((i64)(getBigIntVal() - rhs.getIntVal()));
                        } else if(rhs.type_ == FLOAT || rhs.type_ == DOUBLE) {
                            double val = getBigIntVal();
                            val -= rhs.type_ == FLOAT ? rhs.getFloatVal() : rhs.getDoubleVal();
                            return Value(val);
                        } else if(rhs.type_ == BIGINT) {
                            return Value((i64)(getBigIntVal() - rhs.getBigIntVal()));
                        } else {
                            assert(0 && "NOT SUPPORTED TYPE CONVERSION");
                        }
                    }
        case INT : {
                       if(rhs.type_ == INT) {
                           return Value(cast_as(int, content_) - rhs.getIntVal());
                       } else if(rhs.type_ == FLOAT) {
                           float val = cast_as(int, content_);
                           return Value(val - rhs.getFloatVal());
                       } else {
                           assert(0 && "NOT SUPPORTED TYPE CONVERSION");
                       }
                   }
                   break;
        case FLOAT:{
                       if(rhs.type_ == INT) {
                           return Value(cast_as(float, content_) - rhs.getIntVal());
                       } else if(rhs.type_ == FLOAT) {
                           return Value(cast_as(float, content_) - rhs.getFloatVal());
                       } else {
                           assert(0 && "NOT SUPPORTED TYPE CONVERSION");
                       }
                   }
                   break;
        case BOOLEAN:
                   {
                       if(rhs.type_ == NULL_TYPE){
                           return Value(NULL_TYPE);
                       }
                       return Value((int)((bool)content_ - rhs.getBoolVal()));
                     break;
                   }
        case VARCHAR: 
                     {
                         if(rhs.type_ == VARCHAR) {
                             int n = std::min(size_, rhs.size_);
                             return Value(strncmp((char*)content_, (char*)rhs.content_, n));
                         } else if(rhs.type_ == INT || rhs.type_ == FLOAT || rhs.type_ == BIGINT || rhs.type_ == DOUBLE) {
                             return Value(Value(strtod((char*)content_, NULL)) - rhs);
                         } else {
                             assert(0 && "NOT SUPPORTED TYPE CONVERSION");
                         }
                     } 
                     break;
        case NULL_TYPE:
        case INVALID:
        default :
                     assert(0 && "OPERATOR NOT SUPPORTED FOR THIS TYPE!");
    }
    return Value(NULL_TYPE);
} 

Value::Value(bool val){
    size_ = 1; 
    type_ = BOOLEAN;
    cast_as(bool, content_) = val;
}
Value::Value(int val){
    size_ = 4; 
    type_ = INT;
    cast_as(int, content_) = val;
}
Value::Value(i64 val){
    size_ = 8; 
    type_ = BIGINT;
    cast_as(i64 , content_) = val;
}
Value::Value(float val){
    size_ = 4; 
    type_ = FLOAT;
    cast_as(float, content_) = val;
}
Value::Value(double val){
    size_ = 8; 
    type_ = DOUBLE;
    cast_as(double, content_) = val;
}

void Value::setValue(Type t, char* content) { 
    switch (t) {
        case VARCHAR: 
            content_ = (uintptr_t) content;
            break;
        case BOOLEAN:
            cast_as(bool, content_) = *(bool*)content;
            break;
        case INT:
            setIntValue(*(int32_t*)content);
            break;
        case BIGINT:
            setBigintValue(*(int64_t*)content);
            break;
        case FLOAT:
            setFloatValue(*(float*)content);
            break;
        case DOUBLE:
            setDoubleValue(*(double*)content);
            break;
        case OVERFLOW_ITERATOR:
            content_ = (uintptr_t) content;
            break;
        case NULL_TYPE:
            return;
        case INVALID:
        default :
            assert(0);
    }
}


String Value::toString() const {
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
        case OVERFLOW_ITERATOR:
            return getLargeStringVal();
        case NULL_TYPE:
            return "NULL";
        case INVALID:
            return "INVALID";
        default :
            return "NOT SUPPORTED YET";
    }

}

inline bool Value::isInvalid() const {
    return (type_ == INVALID);
}
inline bool Value::isNull() const {
    return (type_ == NULL_TYPE);
}

// does not copy the string, Only get's a view over it,
// and in case the string is a large string (stored in an overflow page)
// the arena is used to allocate memory for it, and the user is responsible for it's lifetime.
String8 Value::getStringView(Arena* arena) {
    assert(type_ == OVERFLOW_ITERATOR || type_ == VARCHAR);
    if(type_ == VARCHAR) {
        return {
            .str_ = (u8*) content_,
            .size_= size_,
        };
    } else if(type_ == OVERFLOW_ITERATOR){
        OverflowIterator* it_ = (OverflowIterator*) content_;
        u16 bytes_read = 0;
        const char* ptr = nullptr;
        u64 total_size = 0;
        ptr = it_->get_data_cpy_and_advance(arena, &bytes_read);
        total_size += bytes_read;
        if(!ptr) return {};
        while(it_->get_data_cpy_and_advance(arena, &bytes_read)) {
            assert(bytes_read > 0);
            total_size += bytes_read;
        }
        arena->realign();
        return {
            .str_ = (u8*)ptr,
            .size_ = total_size,
        };
    }
    assert(0 && "type is not a text type");
    return {};
}

String Value::getStringVal() const {
    if(!content_) return "";
    if(type_ == OVERFLOW_ITERATOR) return getLargeStringVal();
    String str = "";
    char* ptr = (char*) content_;
    for(int i = 0; i < size_; ++i, ptr++)
        str += *ptr;
    return str;
}

String Value::getLargeStringVal() const {
    if(!content_) return "";
    String str = "";
    OverflowIterator* it_ = (OverflowIterator*) content_;
    Arena tmp;
    tmp.init();
    u16 bytes_read = 0;
    const char* ptr = nullptr;
    while((ptr = it_->get_data_cpy_and_advance(&tmp, &bytes_read))) {
        assert(bytes_read > 0);
        for(int i = 0; i < bytes_read; ++i, ptr++)
            str += *ptr;
    }
    tmp.destroy();
    return str;
}

bool Value::getBoolVal() const{
    if(!content_) return false;
    if(type_ == BOOLEAN) return (bool)content_;
    else if(type_ == FLOAT){
        return getFloatVal();
    } else if(type_ == DOUBLE){
        return getDoubleVal();
    } else if(type_ == INT){
        return getIntVal();
    }
    assert(0);
    return false;
}
int Value::getIntVal() const {
    if(!content_) return 0;
    if(type_ == INT || type_ == BIGINT || type_ == EXECUTOR_ID) return cast_as(int, content_);
    else if(type_ == FLOAT){
        return getFloatVal();
    } else if(type_ == DOUBLE){
        return getDoubleVal();
    } else if(type_ == BOOLEAN){
        return getBoolVal();
    }
    assert(0);
    return 0;
}
long long Value::getBigIntVal() const {
    if(!content_) return 0;
    return cast_as(long long, content_);
}
float Value::getFloatVal() const {
    if(!content_) return 0.0f;
    return cast_as(float, content_);;
}
double Value::getDoubleVal() const {
    if(!content_) return 0.0f;
    return cast_as(double, content_);
}
bool Value::operator==(const Value &rhs) const { 
    int cmp = value_cmp(*this, rhs);
    return cmp == 0;
}

bool Value::operator!=(const Value &rhs) const { 
    int cmp = value_cmp(*this, rhs);
    return cmp != 0;
}

bool Value::operator<(const Value &rhs) const { 
    int cmp = value_cmp(*this, rhs);
    return cmp < 0;
}

bool Value::operator<=(const Value &rhs) const { 
    int cmp = value_cmp(*this, rhs);
    return cmp <= 0;
}

bool Value::operator>(const Value &rhs) const { 
    int cmp = value_cmp(*this, rhs);
    return cmp > 0;
}

bool Value::operator>=(const Value &rhs) const { 
    int cmp = value_cmp(*this, rhs);
    return cmp >= 0;
}

void Value::setIntValue(int32_t val){
    if(type_ == INT) 
        *((int32_t*)&content_) = val;
    else 
        assert(0);
}

void Value::setBigintValue(int64_t val){
    if(type_ == BIGINT)
        *((int64_t*)&content_) = val;
    else 
        assert(0);
}

void Value::setFloatValue(float val){
    if(type_ == FLOAT)
        *((float*)&content_) = val;
    else 
        assert(0);

}

void Value::setDoubleValue(double val){
    if(type_ == DOUBLE)
        *((double*)&content_) = val;
    else 
        assert(0);
}

Value& Value::operator++() {
    switch (type_) {
        case INT:
            setIntValue(getIntVal()+1);
            break;
        case BIGINT:
            setBigintValue(getBigIntVal()+1);
            break;
        case FLOAT:
            setFloatValue(getFloatVal()+1);
            break;
        case DOUBLE:
            setDoubleValue(getDoubleVal()+1);
            break;
        default :
            assert(0 && "Can't increment this type");
    }
    return *this;
}

// -1 ==> lhs < rhs, 0 eq, 1 ==> lhs > rhs
int value_cmp(Value lhs, Value rhs) {
    assert(!lhs.isNull() && !rhs.isNull()); // can't compare null values.
    /*
    if(lhs.content_  && !rhs.content_ ) return 1;
    if(rhs.content_  && !lhs.content_ ) return -1;
    if(!rhs.content_ && !lhs.content_ ) return 0;
    */
    /*
    if(lhs.content_  && !rhs.content_ ) assert(0);
    if(rhs.content_  && !lhs.content_ ) assert(0);
    if(!rhs.content_ && !lhs.content_ ) assert(0);
    */

    if((lhs.type_ == BIGINT || lhs.type_ == DOUBLE) && (rhs.type_ == FLOAT || rhs.type_ == INT))
        rhs.cast_up();

    if((rhs.type_ == BIGINT || rhs.type_ == DOUBLE) && (lhs.type_ == FLOAT || lhs.type_ == INT))
        lhs.cast_up();

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
