#ifndef CATALOG_H
#define CATALOG_H


#include "cache_manager.h"
#include "table.h"
#include "column.h"
#include "value.h"
#include "tokenizer.h"
#include "btree_index.h"
#include "table_schema.h"
#include "arena.h"
#include "index_key.h"
#include <sstream>
#include <algorithm>
#include <cstdint>
#include <cstring>
#include <vector>


/* The system catalog is just a special table with hard coded schema and table name,
 * It is loaded from disk once at booting process of the system.
 * The special table name is :  NILEDB_META_DATA.
 * The schema of the table is as follows: 
 * table_name (var char), col_name (var char), fid(int), col_type (integer), col_offset (integer), 4 boolean type columns.
 * fid is an id that is assigned to the name of the file that the table will be stored in
 * (storing fid for every column is redundent but doesn't matter too much because the meta table is small anyway).
 * col_position is indicating the index of the column within the columns_ vector of the schema.
 * The 4 columns are some sort of a bitmap to store weather this column has the constraints of this index or not, 
 * For example: 1 0 1 0 means this column has constraints:  NULLABLE, not a PRIMARY KEY, FOREIGHT KEY, not UNIQUE.
 * this approach is not very performant or memory effecient, 
 * But it doesn't matter tables are only loaded once and rarely updated or deleted.
 * The name of the file that stores a specific table is not needed because the catalog assumes that the name of the
 * file is just : the name of the table .ndb.
 * This meta data system should be suffecient for now.
 */

#define META_DATA_FILE   "NILEDB_META_DATA.ndb"
#define META_DATA_FSM    "NILEDB_META_DATA_fsm.ndb"
#define META_DATA_TABLE  "NILEDB_META_DATA"
#define INDEX_KEYS_TABLE "NDB_INDEX_KEYS"
#define INDEX_META_TABLE "NDB_INDEX_META"


class Catalog {
    public:

        void init(CacheManager *cm);
        void destroy ();

        TableSchema* createTable(QueryCTX* ctx, const String &table_name, Vector<Column> &columns);
        TableSchema* getTableSchema(const String &table_name);

        bool createIndex(QueryCTX* ctx, const String &table_name,
                const String& index_name, Vector<IndexField> &fields, bool is_unique);
        bool load_indexes();
        IndexHeader getIndexHeader(String& iname);

        Vector<String> getTableNames();
        Vector<String> getTablesByField(String field);
        bool isValidTable(const String& table_name);
        //TODO: change unnecessary indirection.
        Vector<IndexHeader> getIndexesOfTable(String& tname);

        int deleteIndex(QueryCTX* ctx, const String& index_name);
        int deleteTable(QueryCTX* ctx, const String& table_name);

        // TODO: implement alter index and alter table.
    private:
        CacheManager* cache_manager_;
        Arena arena_;
        std::unordered_map<String, TableSchema*> tables_;
        std::unordered_map<String, Vector<String>> indexes_of_table_;
        std::unordered_map<String, IndexHeader> indexes_;
        FreeSpaceMap* free_space_map_;
        // hard coded data:
        TableSchema* meta_table_schema_;
        //TODO:provide some locking mechanism on all of the catalog data.
};

#endif //CATALOG_H
