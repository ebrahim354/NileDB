#pragma once
#include "table.cpp"
#include "table_iterator.cpp"

/* The execution engine that holds all execution operators that could be 
 * (sequential scan, index scan) which are access operators,
 * (filter, join, projection) that are relational algebra operators, 
 * other operator such as (sorting, aggrecations) 
 * or modification queries (delete, insert, update).
 * Each one of these operators implements a next() method that will produce the next record of the current table that
 * is being scanned filtered joined etc... This method has so many names ( volcano, pipleline, iterator ) as apposed to
 * building the entire output table and returning it all at once.
 * In order to initialize an operator we need some sorts of meta data that should be passed into it via the constructor,
 * The larger the project gets the more data we are going to need, Which might require using a wrapper class 
 * around the that data, But for now we are just going to pass everything to the constructor.
 */

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

