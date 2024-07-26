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
        std::string getStringVal(){
            return std::string(content_, size_);  
        }
        bool getBoolVal(){
            return *reinterpret_cast<bool*>(content_);
        }
        int getIntVal(){
            return *reinterpret_cast<int*>(content_);
        }
        long long getBigIntVal(){
            return *reinterpret_cast<long long*>(content_);
        }
        float getFloatVal(){
            return *reinterpret_cast<float*>(content_);
        }
        double getDoubleVal(){
            return *reinterpret_cast<double*>(content_);
        }
};
