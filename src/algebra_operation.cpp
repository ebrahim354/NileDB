#pragma once
#include "query_ctx.cpp"
 enum AlgebraOperationType {
    // single table operations.
    SCAN,
    FILTER, 
    AGGREGATION,
    PROJECTION,
    SORT,
    LIMIT,
    RENAME,
    INSERTION,

    // two table operations.
    PRODUCT,
    JOIN,
    // set operations
    // AL => algebra.
    AL_UNION, 
    AL_EXCEPT,
    AL_INTERSECT,
};


struct AlgebraOperation {
    public:
        AlgebraOperation (AlgebraOperationType type, QueryCTX& ctx) : 
            type_(type), ctx_(ctx)
        {}
        virtual ~AlgebraOperation(){};
        virtual void print(int prefix_space_cnt) = 0;
        QueryCTX& ctx_;
        AlgebraOperationType type_;
        int query_idx_ = -1;
        int query_parent_idx_ = -1;
        bool distinct_ = false;
};
