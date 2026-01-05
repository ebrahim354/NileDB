#pragma once
#include "arena.h"
#include "catalog.h"
#include "cache_manager.cpp"
#include "table.cpp"
#include "column.cpp"
#include "value.cpp"
#include "tokenizer.cpp"
#include "btree_index.cpp"
#include "table_schema.cpp"
#include "parser.cpp"
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


int generate_max_fid() {
    int mx = -1;
    for(auto& fid: fid_to_fname){
        if(fid.first > mx) mx = fid.first;
    }
    mx+= 2; // always append by 2 in case of an fsm.
    return mx;
}

void Catalog::init(CacheManager *cm) {
    assert(cm);
    cache_manager_ = cm;
    arena_.init();

    // the fid of an fsm is always the fid of the table + 1.
    const FileID meta_data_fid = 0;

    fid_to_fname[meta_data_fid]   = META_DATA_FILE;
    fid_to_fname[meta_data_fid+1] = META_DATA_FSM;


    // loading the hard coded meta data table schema.
    Table* meta_data_table = nullptr; 
    ALLOCATE_INIT(arena_, meta_data_table, Table, cm, meta_data_fid);

    Vector<Column> meta_data_columns;
    meta_data_columns.reserve(5);
    meta_data_columns.emplace_back(&arena_, "table_name"      , VARCHAR, 0);
    meta_data_columns.emplace_back(&arena_, "query"           , VARCHAR, 4); // query used to create the table.
    meta_data_columns.emplace_back(&arena_, "fid"             , INT    , 8);
    meta_table_schema_ =  New(TableSchema, arena_, META_DATA_TABLE, meta_data_table, meta_data_columns);
    tables_[META_DATA_TABLE] = meta_table_schema_;

    // temporary parser.
    Parser parser(nullptr);
    // loading TableSchema of each table into memory.
    TableIterator it = meta_table_schema_->begin();
    // meta table's records live in memory for the entire lifetime of the catalog,
    // there for we don't use a temporary memory.
    it.init();
    while(it.advance()){
        Tuple t;
        int err = it.getCurTupleCpy(arena_, &t);
        if(err) break;
        // extract the data of this row.
        String8  table_name = t.get_val_at(0).getStringView(&arena_);
        String8  query      = t.get_val_at(1).getStringView(&arena_);
        FileID  fid        = t.get_val_at(2).getIntVal();
        // "temporary" query ctx.
        QueryCTX pctx;
        pctx.init(query);
        parser.parse(pctx);
        assert(pctx.queries_call_stack_.size() == 1);
        auto table_data =  (CreateTableStatementData*) pctx.queries_call_stack_[0];
        assert(table_data->type_ == CREATE_TABLE_DATA && table_data->table_name_ == table_name);
        // collect column data.
        Vector<FieldDef>* fields = &table_data->field_defs_;
        assert(fields->size());
        Vector<Column> cols; cols.reserve(fields->size());
        
        u8 col_offset = 0;
        for(int i = 0; i < fields->size(); ++i){
            Type type = tokenTypeToColType((*fields)[i].type_);
            if(type == INVALID) {
                std::cout << "[ERROR] Invalid type\n";
                assert(0);
            }
            cols.emplace_back(&arena_, to_string((*fields)[i].field_name_), type, col_offset, (*fields)[i].constraints_);
            col_offset += getSizeFromType(type);
        }

        // initialize the table.
        String fname = to_string(table_name);
        fname +=".ndb";
        String fsm = to_string(table_name);
        fsm += "_fsm.ndb";
        assert((fid_to_fname.count(fid) == 0) && "[FATAL] fid already exists!"); 
        fid_to_fname[fid] = fname;
        fid_to_fname[fid+1] = fsm;
        Table* table = nullptr; 
        ALLOCATE_INIT(arena_, table, Table, cm, fid);
        TableSchema* schema = New(TableSchema, arena_, to_string(table_name), table, cols);
        tables_.insert({to_string(table_name), schema});

        // clean the temporary query ctx.
        pctx.clean();
    }
    it.destroy();
    // load indexes meta data:
    // indexes_meta_data (text index_name, text table_name, int fid, boolean is_unique).
    // indexs_keys       (text index_name, int field_number_in_table, int field_number_in_index).
    assert(tables_.count(INDEX_META_TABLE) == tables_.count(INDEX_KEYS_TABLE)); // both should either exist or not.
    if(tables_.count(INDEX_META_TABLE)){
        load_indexes();
    } else {
        // create a pseudo context and destroy it after.
        QueryCTX pctx;
        String8 index_query = 
            str_lit("CREATE TABLE " 
            INDEX_META_TABLE
            "(index_name TEXT, table_name TEXT, fid INTEGER, is_unique BOOLEAN)");
        pctx.init(index_query);
        Vector<Column> index_meta_columns;
        index_meta_columns.emplace_back(&arena_, "index_name"    , VARCHAR, 0 );
        index_meta_columns.emplace_back(&arena_, "table_name"    , VARCHAR, 4 );
        index_meta_columns.emplace_back(&arena_, "fid"           , INT    , 8 );
        index_meta_columns.emplace_back(&arena_, "is_unique"     , BOOLEAN, 12);
        TableSchema* ret = createTable(&pctx, INDEX_META_TABLE, index_meta_columns); 
        assert(ret != nullptr);

        String8 index_keys_query = 
            str_lit(
                "CREATE TABLE "
                INDEX_KEYS_TABLE 
                "(index_name TEXT, field_number_in_table INTEGER, field_number_in_index INTEGER, is_desc_order BOOLEAN)"
            );
        pctx.query_ = index_keys_query;

        Vector<Column> index_keys_columns;
        index_keys_columns.emplace_back(&arena_, "index_name"            , VARCHAR   , 0);
        index_keys_columns.emplace_back(&arena_, "field_number_in_table" , INT       , 4);
        index_keys_columns.emplace_back(&arena_, "field_number_in_index" , INT       , 8);
        index_keys_columns.emplace_back(&arena_, "is_desc_order"         , BOOLEAN   , 12);
        ret = createTable(&pctx, INDEX_KEYS_TABLE, index_keys_columns); 
        assert(ret != nullptr);
        pctx.clean();
    }
}

void Catalog::destroy () {
    arena_.destroy();
}

TableSchema* Catalog::createTable(QueryCTX* ctx, const String &table_name, Vector<Column> &columns) {
    FileID nfid = generate_max_fid();
    assert((fid_to_fname.count(nfid) == 0 && fid_to_fname.count(nfid+1) == 0) && "[FATAL] fid already exists!");
    if (tables_.count(table_name) || fid_to_fname.count(nfid))
        return nullptr;
    String fname = table_name+".ndb";
    String fsm = table_name+"_fsm.ndb";
    fid_to_fname[nfid] = fname;
    fid_to_fname[nfid+1] = fsm;

    // initialize the table
    Table* table = nullptr;
    ALLOCATE_INIT(arena_, table, Table, cache_manager_, nfid);
    TableSchema* schema = New(TableSchema, arena_, table_name, table, columns);
    tables_.insert({table_name, schema});
    // persist the table schema in the meta data table.
    Tuple t(&ctx->arena_);
    t.setNewSchema(meta_table_schema_);
    t.put_val_at(0, Value(&ctx->arena_, table_name));
    //t.put_val_at(1, Value(&ctx->arena_, ctx->query_));
    t.put_val_at(1, Value(ctx->query_));
    t.put_val_at(2, Value(nfid));

    RecordID rid = RecordID();
    int err = meta_table_schema_->insert(ctx->arena_, t, &rid);
    if(err) return nullptr;
    return schema;
}

bool Catalog::createIndex(QueryCTX* ctx, const String &table_name, const String& index_name,
        Vector<IndexField> &fields, bool is_unique) {
    if (!tables_.count(table_name) || indexes_.count(index_name))
        return 1;
    TableSchema* table = tables_[table_name];
    Vector<NumberedIndexField> cols;
    for(int i = 0; i < fields.size(); ++i){
        int col = table->colExist(fields[i].name_);
        if(col == -1) return 1;
        cols.push_back({col, fields[i].desc_});
    }
    // initialize the index
    String index_fname = index_name+"_INDEX.ndb";
    FileID nfid = generate_max_fid(); 
    assert(fid_to_fname.count(nfid) == 0); // TODO: replace assertion with error.
    fid_to_fname[nfid] = index_fname;


    // persist the index.
    assert(tables_.count(INDEX_META_TABLE) && tables_.count(INDEX_KEYS_TABLE));
    TableSchema* index_meta_data = tables_[INDEX_META_TABLE];
    TableSchema* index_keys      = tables_[INDEX_KEYS_TABLE];

    BTreeIndex* index = nullptr; 
    ALLOCATE_INIT(arena_, index, 
            BTreeIndex, cache_manager_, nfid, cols.size(), is_unique);
    IndexHeader header = IndexHeader(index, index_name, cols);
    indexes_.insert({index_name, header});

    if(indexes_of_table_.count(table_name))
        indexes_of_table_[table_name].push_back(index_name);
    else 
        indexes_of_table_[table_name] = {index_name};

    // store index_meta_data.
    Tuple t(&ctx->arena_);
    t.setNewSchema(index_meta_data);
    t.put_val_at(0, Value(&ctx->arena_, index_name));
    t.put_val_at(1, Value(&ctx->arena_, table_name));
    t.put_val_at(2, Value(nfid));
    t.put_val_at(3, Value((bool) is_unique));
    RecordID rid = RecordID();
    int err = index_meta_data->insert(ctx->arena_, t, &rid);
    assert(err == 0);

    // store index_keys.
    // indexs_keys  (text index_name, int field_number_in_table, int field_number_in_index).
    for(int i = 0; i < cols.size(); ++i){
        NumberedIndexField c = cols[i];
        Tuple t(&ctx->arena_);
        t.setNewSchema(index_keys);
        t.put_val_at(0, Value(&ctx->arena_, index_name));
        t.put_val_at(1, Value(c.idx_));
        t.put_val_at(2, Value(i));
        t.put_val_at(3, Value((bool)c.desc_));
        int err = index_keys->insert(ctx->arena_, t, &rid);
        assert(err == 0);
        if(err) return false;
    }

    // insert rows of the table.
    TableIterator table_it = table->begin();
    table_it.init();
    ArenaTemp tmp = ctx->arena_.start_temp_arena();
    while(table_it.advance()) {
        ctx->arena_.clear_temp_arena(tmp);
        Tuple t;
        int err = table_it.getCurTupleCpy(ctx->arena_, &t);
        assert(err == 0 && "Could not traverse the table.");
        IndexKey k = getIndexKeyFromTuple(tmp.arena_, indexes_[index_name].fields_numbers_, t, table_it.getCurRecordID());
        assert(k.size_ != 0);
        bool success = indexes_[index_name].index_->Insert(ctx, k);
        assert(success);
    }
    table_it.destroy();
    return false;
}

TableSchema* Catalog::getTableSchema(const String &table_name) {
    if (!tables_.count(table_name))
        return nullptr;
    return tables_[table_name];
}

TableSchema* Catalog::get_table_schema(String8 table_name) {
    if (!tables_.count(to_string(table_name)))
        return nullptr;
    return tables_[to_string(table_name)];
}

bool Catalog::isValidTable(const String& table_name) {
    if (!tables_.count(table_name)) return false;
    return true;
}

Vector<String> Catalog::getTableNames() {
    Vector<String> output;
    for(auto& t : tables_){
        output.push_back(t.first);
    }
    return output;
}

Vector<String> Catalog::getTablesByField(String field) {
    Vector<String> output;
    for(auto& t : tables_){
        if(t.second->isValidCol(field))
            output.push_back(t.first);
    }
    return output;
}

Vector<String> Catalog::get_tables_by_field(String8 field) {
    Vector<String> output;
    for(auto& t : tables_){
        if(t.second->is_valid_col(field))
            output.push_back(t.first);
    }
    return output;
}

//TODO: change unnecessary indirection.
Vector<IndexHeader> Catalog::getIndexesOfTable(String& tname) {
    Vector<IndexHeader> idxs;
    if(indexes_of_table_.count(tname)){
        for(int i = 0; i < indexes_of_table_[tname].size(); ++i){
            String idx_name = indexes_of_table_[tname][i];
            if(indexes_.count(idx_name))
                idxs.push_back(indexes_[idx_name]);
        }
    }
    return idxs;
}

Vector<IndexHeader> Catalog::get_indexes_of_table(String8 t) {
    String tname = to_string(t);
    Vector<IndexHeader> idxs;
    if(indexes_of_table_.count(tname)){
        for(int i = 0; i < indexes_of_table_[tname].size(); ++i){
            String idx_name = indexes_of_table_[tname][i];
            if(indexes_.count(idx_name))
                idxs.push_back(indexes_[idx_name]);
        }
    }
    return idxs;
}

bool Catalog::load_indexes() {
    // indexes_meta_data (text index_name, text table_name, int fid, is_unique).
    TableSchema* indexes_meta_schema = tables_[INDEX_META_TABLE];
    //TableIterator it_meta = indexes_meta_schema->getTable()->begin();
    TableIterator it_meta = indexes_meta_schema->begin();
    bool success = true;

    it_meta.init();
    while(it_meta.advance()){
        Tuple t;
        int err = it_meta.getCurTupleCpy(arena_, &t);
        assert(err == 0 && "Could not traverse the indexes schema.");
        if(err){
            success = false;
            break;
        };

        String index_name = t.get_val_at(0).getStringVal();
        String table_name = t.get_val_at(1).getStringVal();
        FileID fid        = t.get_val_at(2).getIntVal();
        bool is_unique    = t.get_val_at(3).getBoolVal();

        // each index must exist only once on this table.
        assert(indexes_.count(index_name) == 0 && "Index accured multiple times on meta data!");

        // set up file ids mappings.
        String index_fname = index_name+"_INDEX.ndb";
        assert((fid_to_fname.count(fid) == 0) && "[FATAL] fid already exists!"); 
        fid_to_fname[fid] = index_fname;

        // save results into memory.
        BTreeIndex* index_ptr = nullptr; 
        ALLOCATE_INIT(arena_, index_ptr, 
                        BTreeIndex, cache_manager_, fid, 1, is_unique);
        std::pair<String, IndexHeader> entry = {index_name, IndexHeader(index_ptr, index_name)};
        indexes_.insert(entry);
        if(indexes_of_table_.count(table_name))
            indexes_of_table_[table_name].push_back(index_name);
        else 
            indexes_of_table_[table_name] = {index_name};
    }
    it_meta.destroy();
    if(!success) {
        return false;
    }

    // indexs_keys      (text index_name, int field_number_in_table, int field_number_in_index).
    // this table holds the data for all key mappings of all indexes of the system.
    TableSchema* index_keys_schema = tables_[INDEX_KEYS_TABLE];
    TableIterator it_keys = index_keys_schema->begin();
    it_keys.init();

    while(it_keys.advance()){
        Tuple t;
        int err = it_keys.getCurTupleCpy(arena_, &t);
        assert(err == 0 && "Could not traverse the indexes keys.");
        if(err) {
            success = false;
            break;
        };

        // we need field_number_in_index to know the order of the fields used to form keys.
        String index_name            = t.get_val_at(0).getStringVal();
        int field_number_in_table    = t.get_val_at(1).getIntVal();
        int field_number_in_index    = t.get_val_at(2).getIntVal();
        bool is_desc_order           = t.get_val_at(3).getBoolVal();

        assert(indexes_.count(index_name) == 1); // index must exist.
        IndexHeader* header = &indexes_[index_name];
        if(field_number_in_index >= header->fields_numbers_.size()) 
            header->fields_numbers_.resize(field_number_in_index+1);

        header->fields_numbers_[field_number_in_index] = {
            .idx_ = field_number_in_table,
            .desc_ = is_desc_order
        };
        header->index_->resize_nkey_cols(header->fields_numbers_.size());
    }
    it_keys.destroy();
    return success;
}

IndexHeader Catalog::getIndexHeader(String& iname) {
    if(indexes_.count(iname)) return indexes_[iname];
    std::cout << iname << std::endl;
    assert(0 && "index should exist");
    return {};
}

// ret => 0 on success.
int Catalog::deleteIndex(QueryCTX* ctx, const String& index_name) {
    // TODO: check that the index is not attatched to fields with 'uinque' or 'primary key' constraints.
    if (!indexes_.count(index_name))
        return 1;
    IndexHeader header = indexes_[index_name];
    assert(header.index_);

    // gather data related to the index in the system.
    BTreeIndex* index = indexes_[index_name].index_; 
    FileID fid = index->get_fid();
    String index_table_name = "";

    assert(fid_to_fname.count(fid));

    // clear persisted data of the index.
    assert(tables_.count(INDEX_META_TABLE) && tables_.count(INDEX_KEYS_TABLE));
    TableSchema* index_meta_data = tables_[INDEX_META_TABLE];
    TableSchema* index_keys      = tables_[INDEX_KEYS_TABLE];

    // clear index_meta_data.
    // indexes_meta_data (text index_name, text table_name, int fid).
    //TableIterator it_meta = index_meta_data->getTable()->begin();
    TableIterator it_meta = index_meta_data->begin();
    std::set<u64> halloween_preventer;
    it_meta.init();
    while(it_meta.advance()){
        if(halloween_preventer.count(it_meta.getCurRecordID().get_hash())) continue;
        ArenaTemp tmp = ctx->arena_.start_temp_arena();

        Tuple t;
        //Record r = it_meta.getCurRecordCpy(&ctx->arena_);
        //assert(!r.isInvalidRecord());
        //Vector<Value> values;
        //int err = index_meta_data->translateToValues(r, values);
        int err = it_meta.getCurTupleCpy(ctx->arena_, &t);
        assert(err == 0 && "Could not traverse the indexes meta table.");

        //String cur_table_name = values[1].getStringVal();
        //FileID index_fid      = values[2].getIntVal();
        String cur_table_name = t.get_val_at(1).getStringVal();
        FileID index_fid      = t.get_val_at(2).getIntVal();
        if(index_fid != fid) {
            ctx->arena_.clear_temp_arena(tmp);
            continue;
        }
        if(index_table_name.size() == 0) 
            index_table_name = cur_table_name;

        RecordID rid = it_meta.getCurRecordID();

        err = index_meta_data->remove(rid);
        ctx->arena_.clear_temp_arena(tmp);
        assert(err == 0 && "Could not delete record.");
        halloween_preventer.insert(rid.get_hash());
    }
    it_meta.destroy();
    halloween_preventer.clear();
    

    // clear index_keys.
    // indexs_keys  (text index_name, int field_number_in_table, int field_number_in_index).
    //TableIterator it_keys = index_keys->getTable()->begin();
    TableIterator it_keys = index_keys->begin();
    it_keys.init();
    while(it_keys.advance()){
        if(halloween_preventer.count(it_keys.getCurRecordID().get_hash())) continue;
        ArenaTemp tmp = ctx->arena_.start_temp_arena();

        //Record r = it_keys.getCurRecordCpy(&ctx->arena_);
        //assert(!r.isInvalidRecord());
        //Vector<Value> values;
        //int err = index_keys->translateToValues(r, values);
        Tuple t;
        int err = it_keys.getCurTupleCpy(ctx->arena_, &t);
        assert(err == 0 && "Could not traverse the indexe keys table.");

        String cur_index_name = t.get_val_at(0).getStringVal();
        if(cur_index_name != index_name) {
            ctx->arena_.clear_temp_arena(tmp);
            continue;
        }

        RecordID rid = it_keys.getCurRecordID();
        err = index_keys->remove(rid);
        ctx->arena_.clear_temp_arena(tmp);
        assert(err == 0 && "Could not delete record.");
        halloween_preventer.insert(rid.get_hash());
    }
    it_keys.destroy();
    
    // delete the index file.
    int err = cache_manager_->deleteFile(fid);
    // clear the catalog's in-memory data.
    indexes_.erase(index_name);
    fid_to_fname.erase(fid);
    assert(indexes_of_table_.count(index_table_name));
    auto table_indexes = indexes_of_table_[index_table_name];
    for(int i = 0; i < table_indexes.size(); ++i){
        if(table_indexes[i] == index_name){
            indexes_of_table_[index_table_name].erase(indexes_of_table_[index_table_name].begin() + i);
            break;
        }
    }
    if(!err) std::cout << "Dropped Index: " << index_name << "\n";
    return err;
}

int Catalog::deleteTable(QueryCTX* ctx, const String& table_name) {
    if (!tables_.count(table_name))
        return 1;

    // gather data related to the table in the system.
    TableSchema* table_schema = tables_[table_name];
    assert(table_schema->getTable());

    Table* table = table_schema->getTable();
    FileID table_fid = table->get_fid();
    // fsm of any table is by convention the second file after that table.
    FileID fsm_fid   = table_fid + 1;
    assert(fid_to_fname.count(table_fid) && fid_to_fname.count(fsm_fid));
    // delete the indexes of this table first before continuting.
    int err = 0;
    if(indexes_of_table_.count(table_name)){
        // take a copy because deleting will affect the loop.
        auto indexes = indexes_of_table_[table_name];
        for(int i = 0; err == 0 && i < indexes.size(); ++i)
            err = deleteIndex(ctx, indexes[i]);
        if(err) return err;
        indexes_of_table_.erase(table_name);
    }

    //TableIterator it_meta = meta_table_schema_->getTable()->begin();
    TableIterator it_meta = meta_table_schema_->begin();
    std::set<u64> halloween_preventer;
    it_meta.init();
    while(it_meta.advance()){
        if(halloween_preventer.count(it_meta.getCurRecordID().get_hash())) continue;
        ArenaTemp tmp = ctx->arena_.start_temp_arena();

        //Record r = it_meta.getCurRecordCpy(&ctx->arena_);
        //assert(!r.isInvalidRecord());
        //Vector<Value> values;
        //err = meta_table_schema_->translateToValues(r, values);
        Tuple t;
        int err = it_meta.getCurTupleCpy(ctx->arena_, &t);
        assert(err == 0 && "Could not traverse the meta data table.");
        FileID cur_fid = t.get_val_at(2).getIntVal();
        if(cur_fid != table_fid) {
            ctx->arena_.clear_temp_arena(tmp);
            continue;
        }

        RecordID rid = it_meta.getCurRecordID();
        err = meta_table_schema_->remove(rid);
        ctx->arena_.clear_temp_arena(tmp);
        assert(err == 0 && "Could not delete record.");
        halloween_preventer.insert(rid.get_hash());
    }
    it_meta.destroy();
    
    // delete the table file.
    err = cache_manager_->deleteFile(table_fid);
    assert(err == 0);
    // delete the fsm file.
    err = cache_manager_->deleteFile(fsm_fid);
    assert(err == 0);
    // clear the catalog's in-memory data.
    tables_.erase(table_name);
    fid_to_fname.erase(table_fid);
    fid_to_fname.erase(fsm_fid);

    if(!err) std::cout << "Dropped Table: " << table_name << "\n";
    return err;
}

// TODO: implement alter index and table.
