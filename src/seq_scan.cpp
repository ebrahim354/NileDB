#pragma once
#include "table.cpp"


class SequentialScan {
    public:
        SequentialScan(Table* t){
            it = t->begin();
        }
        ~SequentialScan() {
            delete it;
        }
        // r output record
        // return false in case of an error or end of scan.
        bool next(Record *r) {
            if(it == nullptr) return false;
            *r = it->getCurRecordCpy();
            if(!it->advance()) return false;
            return true;
        }
    private:
        TableIterator *it = nullptr;
        
};
