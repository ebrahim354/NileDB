#pragma once
#include "cache_manager.cpp"
#include "table.cpp"
#include "column.cpp"
#include "value.cpp"
#include "tokenizer.cpp"
#include "btree_index.cpp"
#include "table_schema.cpp"
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


struct IndexHeader{
    BTreeIndex* index_;
    std::vector<NumberedIndexField> fields_numbers_;
};

class Catalog {
    public:
        Catalog(CacheManager *cm)
            : cache_manager_ (cm)
        {

            // the fid of an fsm is always the fid of the table + 1.
            const FileID meta_data_fid = 0;

            fid_to_fname[meta_data_fid] = META_DATA_FILE;
            fid_to_fname[meta_data_fid+1] = META_DATA_FSM;
            // change the size after adding free space map support.
            PageID meta_fsm_pid = {.fid_ = meta_data_fid + 1, .page_num_ = 1};
            free_space_map_ = new FreeSpaceMap(cm, meta_fsm_pid);


            // loading the hard coded meta data table schema.
            PageID meta_pid = {.fid_ = meta_data_fid, .page_num_ = 1};
            auto meta_data_table = new Table(cm, meta_pid, free_space_map_);

            std::vector<Column> meta_data_columns;
            meta_data_columns.emplace_back(Column("table_name", VARCHAR, 0));
            meta_data_columns.emplace_back(Column("col_name"  , VARCHAR, 4));
            meta_data_columns.emplace_back(Column("fid"       , INT    , 8));
            meta_data_columns.emplace_back(Column("col_type"  , INT    , 12));
            meta_data_columns.emplace_back(Column("col_offset", INT    , 16));
            meta_data_columns.emplace_back(Column("nullable"  , BOOLEAN, 20));
            meta_data_columns.emplace_back(Column("primary"   , BOOLEAN, 21));
            meta_data_columns.emplace_back(Column("foreign"   , BOOLEAN, 22));
            meta_data_columns.emplace_back(Column("unique"    , BOOLEAN, 23));
            meta_table_schema_ = new TableSchema(META_DATA_TABLE, meta_data_table, meta_data_columns);
            tables_[META_DATA_TABLE] = meta_table_schema_;

            // loading TableSchema of each table into memory.
            TableIterator* it = meta_data_table->begin();
            while(it->advance()){
                Record r = it->getCurRecordCpy();
                std::vector<Value> values;
                int err = meta_table_schema_->translateToValues(r, values);
                if(err) break;
                // extract the data of this row.
                std::string table_name = values[0].getStringVal();
                std::string col_name = values[1].getStringVal();
                FileID fid = values[2].getIntVal();
                Type col_type = static_cast<Type>(values[3].getIntVal());
                int col_offset = values[4].getIntVal();
                int not_null = values[5].getBoolVal();
                int primary_key = values[6].getBoolVal();
                int foreign_key = values[7].getBoolVal();
                int unique = values[8].getBoolVal();

                // first time seeing this table? if yes then we need to initialize it.
                if(!tables_.count(table_name)){
                    std::string fname = table_name+".ndb";
                    std::string fsm = table_name+"_fsm.ndb";
                    assert((fid_to_fname.count(fid) == 0) && "[FATAL] fid already exists!"); 
                    fid_to_fname[fid] = fname;
                    fid_to_fname[fid+1] = fsm;

                    PageID first_page = {.fid_ = fid, .page_num_ = 1};
                    PageID first_fsm_page = {.fid_ = fid+1, .page_num_ = 1};
                    // the table owns its free space map pointer and is responsible for deleting it.
                    FreeSpaceMap* free_space = new FreeSpaceMap(cm, first_fsm_page);
                    Table* table = new Table(cm, first_page, free_space);
                    TableSchema* schema = new TableSchema(table_name, table, {});
                    tables_.insert({table_name, schema});
                }

                // add the column to the table's meta data.
                std::vector<Column> cols;
                std::vector<Constraint> cons;
                if(not_null) cons.push_back(NOT_NULL); 
                if(primary_key) cons.push_back(PRIMARY_KEY); 
                if(foreign_key) cons.push_back(FOREIGN_KEY); 
                if(unique) cons.push_back(UNIQUE); 

                tables_[table_name]->addColumn(Column(col_name, col_type, col_offset, cons));
            }
            delete it;
            // load indexes meta data:
            // indexes_meta_data (text index_name, text table_name, int fid, int root_page_num).
            // indexs_keys      (text index_name, int field_number_in_table, int field_number_in_index).
            assert(tables_.count(INDEX_META_TABLE) == tables_.count(INDEX_KEYS_TABLE)); // both should either exist or not.
            if(tables_.count(INDEX_META_TABLE)){
                load_indexes();
            } else {
                std::vector<Column> index_meta_columns;
                index_meta_columns.emplace_back(Column("index_name"    , VARCHAR, 0));
                index_meta_columns.emplace_back(Column("table_name"    , VARCHAR, 4));
                index_meta_columns.emplace_back(Column("fid"           , INT    , 8));
                index_meta_columns.emplace_back(Column("root_page_num" , INT    , 12));
                TableSchema* ret = createTable(INDEX_META_TABLE, index_meta_columns); 
                assert(ret != nullptr);

                std::vector<Column> index_keys_columns;
                index_keys_columns.emplace_back(Column("index_name"            , VARCHAR   , 0));
                index_keys_columns.emplace_back(Column("field_number_in_table" , INT       , 4));
                index_keys_columns.emplace_back(Column("field_number_in_index" , INT       , 8));
                index_keys_columns.emplace_back(Column("is_desc_order"         , BOOLEAN   , 12));
                ret = createTable(INDEX_KEYS_TABLE, index_keys_columns); 
                assert(ret != nullptr);
            }
        }
        ~Catalog() {
            delete meta_table_schema_;
            // don't delete it twice, the meta table will delete it.
            // delete free_space_map_;
            for(auto table : tables_){
                delete tables_[table.first];
            }
        }

        TableSchema* createTable(const std::string &table_name, std::vector<Column> &columns) {
            FileID nfid = fid_to_fname.size();// TODO: Dropping tables is going to break this method.
            assert(fid_to_fname.count(nfid) == 0 && "[FATAL] fid already exists!"); // TODO: Remove this assertion.
            if (tables_.count(table_name) || fid_to_fname.count(nfid))
                return nullptr;
            std::string fname = table_name+".ndb";
            std::string fsm = table_name+"_fsm.ndb";
            fid_to_fname[nfid] = fname;
            fid_to_fname[nfid+1] = fsm;

            // initialize the table
            PageID first_page = {.fid_ = nfid, .page_num_ = 1};
            PageID first_fsm_page = {.fid_ = nfid+1, .page_num_ = 1};
            FreeSpaceMap* free_space = new FreeSpaceMap(cache_manager_, first_fsm_page);
            Table* table = new Table(cache_manager_, first_page, free_space);
            TableSchema* schema = new TableSchema(table_name, table, columns);
            tables_.insert({table_name, schema});
            // persist the table schema in the meta data table.
            // create a vector of Values per column,
            // then insert it to the meta table.
            for(auto c : columns){
                std::vector<Value> vals;
                vals.emplace_back(Value(table_name));
                vals.emplace_back(Value(c.getName()));
                vals.emplace_back(Value(nfid));
                vals.emplace_back(Value((int)c.getType()));
                vals.emplace_back(Value(c.getOffset()));
                vals.emplace_back(Value(c.isNullable()));
                vals.emplace_back(Value(c.isPrimaryKey()));
                vals.emplace_back(Value(c.isForeignKey()));
                vals.emplace_back(Value(c.isUnique()));
                // translate the vals to a record and persist them.
                Record* record = meta_table_schema_->translateToRecord(vals);

                /*
                   if(record == nullptr) std::cout << " invalid record " << std::endl;
                   std::cout << " translated a schema to a record with size: " 
                   << record->getRecordSize() << std::endl;
                   for(int i = 0; i < record->getRecordSize(); i++){
                   std::cout << +(char)(*record->getFixedPtr(i)) << ",";
                   }
                   std::cout << std::endl;
                   */

                // rid is not used for now.
                RecordID* rid = new RecordID();
                int err = meta_table_schema_->getTable()->insertRecord(rid, *record);
                delete rid;
                delete record;
                if(err) return nullptr;
            }
            return schema;
        }

        bool createIndex(const std::string &table_name, const std::string& index_name, std::vector<IndexField> &fields) {
            if (!tables_.count(table_name) || indexes_.count(index_name))
                return 1;
            TableSchema* table = tables_[table_name];
            std::vector<NumberedIndexField> cols;
            for(int i = 0; i < fields.size(); ++i){
                int col = table->colExist(fields[i].name_);
                if(col == -1) return 1;
                cols.push_back({col, fields[i].desc_});
            }
            // initialize the index
            //PageID first_page = {.file_name_ = index_name+"_INDEX.ndb", .page_num_ = -1};
            std::string index_fname = index_name+"_INDEX.ndb";
            FileID nfid = fid_to_fname.size();
            assert(fid_to_fname.count(nfid) == 0); // TODO: replace assertion with error.
            fid_to_fname[nfid] = index_fname;


            // persist the index.
            // indexes_meta_data (text index_name, text table_name, int fid, int root_page_num).
            assert(tables_.count(INDEX_META_TABLE) && tables_.count(INDEX_KEYS_TABLE));
            TableSchema* index_meta_data = tables_[INDEX_META_TABLE];
            TableSchema* index_keys      = tables_[INDEX_KEYS_TABLE];

            BTreeIndex* index = new BTreeIndex(cache_manager_, nfid, INVALID_PAGE_ID, index_meta_data);
            indexes_.insert({index_name, {index , cols}});

            if(indexes_of_table_.count(table_name))
                indexes_of_table_[table_name].push_back(index_name);
            else 
                indexes_of_table_[table_name] = {index_name};

            // store index_meta_data.
            std::vector<Value> vals;
            vals.emplace_back(Value(index_name));
            vals.emplace_back(Value(table_name));
            vals.emplace_back(Value(nfid));
            vals.emplace_back(Value(-1)); // -1 means no root yet.
            Record* record = index_meta_data->translateToRecord(vals);
            RecordID* rid = new RecordID();
            int err = index_meta_data->getTable()->insertRecord(rid, *record);
            delete record;
            assert(err == 0);

            // store index_keys.
            // indexs_keys  (text index_name, int field_number_in_table, int field_number_in_index).
            for(int i = 0; i < cols.size(); ++i){
                NumberedIndexField c = cols[i];
                std::vector<Value> vals;
                vals.emplace_back(Value(index_name));
                vals.emplace_back(Value(c.idx_));         // c is the offset inside of the table.
                vals.emplace_back(Value(i));              // i is the offset inside of the index  key.
                vals.emplace_back(Value((bool)c.desc_));  // asc or desc ordering.
                // translate the vals to a record and persist them.
                Record* record = index_keys->translateToRecord(vals);
                int err = index_keys->getTable()->insertRecord(rid, *record);
                assert(err == 0);
                delete record;
                if(err) return false;
            }

            // insert rows of the table.
            TableIterator* table_it = table->getTable()->begin();
            while(table_it->advance()) {
                Record r = table_it->getCurRecordCpy();
                *rid = table_it->getCurRecordID();
                std::vector<Value> vals;
                int err = table->translateToValues(r, vals);
                assert(err == 0 && "Could not traverse the table.");
                IndexKey k = getIndexKeyFromTuple(indexes_[index_name].fields_numbers_, vals);
                assert(k.size_ != 0);
                bool success = indexes_[index_name].index_->Insert(k, *rid);
                assert(success);
                delete k.data_;
            }
            delete table_it;
            delete rid;
            return false;
        }

        TableSchema* getTableSchema(const std::string &table_name) {
            if (!tables_.count(table_name))
                return nullptr;
            return tables_[table_name];
        }

        bool isValidTable(const std::string& table_name) {
            if (!tables_.count(table_name)) return false;
            return true;
        }

        Type stringToType(std::string t){
            if(t == "BOOLEAN")        return BOOLEAN;
            else if(t == "INTEGER")   return INT;
            else if(t == "BIGINT")    return BIGINT;
            else if(t == "FLOAT")     return FLOAT;
            else if(t == "DOUBLE")    return DOUBLE;
            else if(t == "TIMESTAMP") return TIMESTAMP;
            else if(t == "VARCHAR")   return VARCHAR;

            return INVALID;
        }

        Type tokenTypeToColType(TokenType t){
            switch(t){
                case TokenType::BOOLEAN:
                    return Type::BOOLEAN;
                case TokenType::INTEGER:
                    return Type::INT;
                case TokenType::BIGINT:
                    return Type::BIGINT;
                case TokenType::FLOAT:
                    return Type::FLOAT;
                case TokenType::REAL:
                    return Type::DOUBLE;
                case TokenType::TIMESTAMP:
                    return Type::TIMESTAMP;
                case TokenType::VARCHAR:
                    return Type::VARCHAR;
                default:
                    return INVALID;
            }
        }

        std::vector<std::string> getTableNames(){
            std::vector<std::string> output;
            for(auto& t : tables_){
                output.push_back(t.first);
            }
            return output;
        }

        std::vector<std::string> getTablesByField(std::string field){
            std::vector<std::string> output;
            for(auto& t : tables_){
                if(t.second->isValidCol(field))
                    output.push_back(t.first);
            }
            return output;
        }

        //TODO: change unnecessary indirection.
        std::vector<IndexHeader> getIndexesOfTable(std::string& tname){
            std::vector<IndexHeader> idxs;
            if(indexes_of_table_.count(tname)){
                for(int i = 0; i < indexes_of_table_[tname].size(); ++i){
                    std::string idx_name = indexes_of_table_[tname][i];
                    if(indexes_.count(idx_name))
                        idxs.push_back(indexes_[idx_name]);
                }
            }
            return idxs;
        }

        bool load_indexes() {
            // indexes_meta_data (text index_name, text table_name, int fid, int root_page_num).
            TableSchema* indexes_meta_schema = tables_[INDEX_META_TABLE];
            TableIterator* it_meta = indexes_meta_schema->getTable()->begin();

            while(it_meta->advance()){
                Record r = it_meta->getCurRecordCpy();
                std::vector<Value> values;
                int err = indexes_meta_schema->translateToValues(r, values);
                assert(err == 0 && "Could not traverse the indexes schema.");
                if(err) return 0;

                std::string index_name = values[0].getStringVal();
                std::string table_name = values[1].getStringVal();
                FileID fid = values[2].getIntVal();
                PageNum root_page_num = values[3].getIntVal();

                // each index must exist only once on this table.
                assert(indexes_.count(index_name) == 0 && "Index accured multiple times on meta data!");

                // set up file ids mappings.
                std::string index_fname = index_name+"_INDEX.ndb";
                assert((fid_to_fname.count(fid) == 0) && "[FATAL] fid already exists!"); 
                fid_to_fname[fid] = index_fname;

                // save results into memory.
                PageID root_page_id = {.fid_ = fid, .page_num_ = root_page_num};
                BTreeIndex* index_ptr = new BTreeIndex(cache_manager_, fid, root_page_id, indexes_meta_schema);
                indexes_.insert({index_name, {.index_ = index_ptr}});
                if(indexes_of_table_.count(table_name))
                    indexes_of_table_[table_name].push_back(index_name);
                else 
                    indexes_of_table_[table_name] = {index_name};
            }
            delete it_meta;

            // indexs_keys      (text index_name, int field_number_in_table, int field_number_in_index).
            // this table holds the data for all key mappings of all indexes of the system.
            TableSchema* index_keys_schema = tables_[INDEX_KEYS_TABLE];
            TableIterator* it_keys = index_keys_schema->getTable()->begin();

            while(it_keys->advance()){
                Record r = it_keys->getCurRecordCpy();
                std::vector<Value> values;
                int err = index_keys_schema->translateToValues(r, values);
                assert(err == 0 && "Could not traverse the indexes keys.");
                if(err) return 0;

                // we need field_number_in_index to know the order of the fields used to form keys.
                std::string index_name = values[0].getStringVal();
                int field_number_in_table = values[1].getIntVal();
                int field_number_in_index = values[2].getIntVal();
                bool is_desc_order = values[3].getBoolVal();

                assert(indexes_.count(index_name) == 1); // index must exist.
                IndexHeader* header = &indexes_[index_name];
                if(field_number_in_index >= header->fields_numbers_.size()) 
                    header->fields_numbers_.resize(field_number_in_index+1);

                header->fields_numbers_[field_number_in_index] = {
                    .idx_ = field_number_in_table,
                    .desc_ = is_desc_order
                };
            }
            delete it_keys;
            return 1;
        }

        // TODO: implement delete and alter table.
        // TODO: implement delete and alter index.
    private:

        CacheManager* cache_manager_;
        std::unordered_map<std::string, TableSchema*> tables_;
        std::unordered_map<std::string, std::vector<std::string>> indexes_of_table_;
        std::unordered_map<std::string, IndexHeader> indexes_;
        FreeSpaceMap* free_space_map_;

        // hard coded data:
        TableSchema* meta_table_schema_;

};
