#pragma once

enum QueryType {
    SELECT_DATA = 0,
    CREATE_TABLE_DATA,
    CREATE_INDEX_DATA,
    INSERT_DATA,
    DELETE_DATA,
    UPDATE_DATA,
    // set operations.
    UNION,
    INTERSECT,
    EXCEPT
};

struct QueryData {
    QueryData(QueryType type, int parent_idx): type_(type), parent_idx_(parent_idx)
    {}
    virtual ~QueryData(){};
    QueryType type_;
    int idx_ = -1;          // every query must have an id starting from 0 even the top level query.
    int parent_idx_ = -1;   // -1 means this query is the top level query.
};
