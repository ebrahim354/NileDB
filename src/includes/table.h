#ifndef TABLE_H 
#define TABLE_H 

#include "cache_manager.h"
#include "free_space_map.h"
#include "page.h"
#include "table_iterator.h"
#include "record.h"
#include "table_data_page.h"

class TableIterator;

/*
 * the current model we are using is:              "Heap File Organization".
 *
 * this class assumes that the "DirectoryManager", "MetaDataManager" or "CatalogManager" (multiple names same thing)
 * has done all the checks and created a Record wrapper arround the data in case of inserting or just wants the next
 * Record wrapper returned in case of table scans, Which means that the job of this class is to organize pages inside 
 * of the table file and to emplement an effecient access to records.
 */

/*  
 * what do we expect from this class:
 * 1- allow full table scan using TableIterator (wrapper around Record type).
 * 2- allow inserting a Record.
 * 3- allow updating  a Record.
 * 4- allow deleting  a Record.
 * 5- inserting updating and deleting should be space effecient using a simple free-space map (for now),
 *    a free-space map is a data structure that tells the user which page they are likely to find enough space 
 *    for inserting a new record.
 * 6- implement a table iterator to help doing scans.
 */
class Table {
    public:
        void init(CacheManager* cm, PageID first_page_id);
        void destroy();

        // should use the free space map to find the closest free space inside of the file
        // or just append it to the end of the file.

        // rid (output)
        // return 1 in case of an error.
        int insertRecord(RecordID* rid, Record &record);

        // return 1 in case of an error.
        int deleteRecord(RecordID &rid);

        // return 1 in case of an error.
        // rid is both an input to find the record and an output of the new position of the updated record. 
        // updates are performed by deleting the old record followed by an insertion of the new one.
        int updateRecord(RecordID *rid, Record &new_record);

        // we allow only forward scans for now via tableIterator.advance().
        TableIterator begin();
        FileID get_fid();
    private:
        CacheManager* cache_manager_ = nullptr;
        PageID first_page_id_ = INVALID_PAGE_ID;
        FreeSpaceMap free_space_map_;
};

#endif // TABLE_H 
