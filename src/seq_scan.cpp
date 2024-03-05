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
            if(!it->advance()) return false;
            *r = it->getCurRecordCpy();
            return true;
        }
    private:
        TableIterator *it;
        
};
