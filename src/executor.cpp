#pragma once

#include "catalog.cpp"
#include "string"

struct QueryCTX;
enum ExecutorType {
    SEQUENTIAL_SCAN_EXECUTOR = 0,
    INSERTION_EXECUTOR,
    FILTER_EXECUTOR,
    AGGREGATION_EXECUTOR,
    PROJECTION_EXECUTOR,
    SORT_EXECUTOR,
    DISTINCT_EXECUTOR,
    PRODUCT_EXECUTOR,
    // TODO: implement sort joins and index based joins.
    JOIN_EXECUTOR, // only in-memory hash joins for now. 

    UNION_EXECUTOR,
    EXCEPT_EXECUTOR,
    INTERSECT_EXECUTOR,
};

std::string exec_type_to_string(ExecutorType t){
    switch(t){
        case SEQUENTIAL_SCAN_EXECUTOR:
            return "SEQUENTIAL SCAN";
        case INSERTION_EXECUTOR:
            return "INSERTION";
        case FILTER_EXECUTOR:
            return "FILTER";
        case AGGREGATION_EXECUTOR: 
            return "AGGREGATION";
        case PROJECTION_EXECUTOR: 
            return "PROJECTION";
        case SORT_EXECUTOR:
            return "SORT";
        case DISTINCT_EXECUTOR:
            return "DISTINCT";
        case PRODUCT_EXECUTOR: 
            return "PRODUCT";
        case JOIN_EXECUTOR: 
            return "JOIN";
        case UNION_EXECUTOR:
            return "UNION";
        case EXCEPT_EXECUTOR:
            return "EXCEPT";
        case INTERSECT_EXECUTOR:
            return "INTERSECT";
        default:
            return "INVALID EXECUTOR";
    }
}

struct Executor {
    public:
        Executor(ExecutorType type, TableSchema* output_schema, QueryCTX& ctx,int query_idx, int parent_query_idx, Executor* child): 
            type_(type), output_schema_(output_schema), ctx_(ctx), 
            query_idx_(query_idx), parent_query_idx_(parent_query_idx), child_executor_(child)
        {}
        virtual ~Executor(){};
        virtual void init() = 0;
        virtual std::vector<Value> next() = 0;

        ExecutorType type_;
        Executor* child_executor_ = nullptr;
        TableSchema* output_schema_ = nullptr;
        std::vector<Value> output_ = {};
        bool error_status_ = 0;
        bool finished_ = 0;
        int query_idx_ = -1;
        int parent_query_idx_ = -1;
        QueryCTX& ctx_;
};

