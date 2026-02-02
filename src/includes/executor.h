#ifndef EXECUTOR_H
#define EXECUTOR_H

#include "string"
#include "ast_nodes.h"
#include "algebra_operation.h"

struct QueryCTX;
struct FlatExpr;
struct InsertStatementData;
struct DeleteStatementData;
struct UpdateStatementData;
enum ExecutorType {
    SEQUENTIAL_SCAN_EXECUTOR = 0,
    INDEX_SCAN_EXECUTOR,

    INSERTION_EXECUTOR,
    DELETION_EXECUTOR,
    UPDATE_EXECUTOR,

    FILTER_EXECUTOR,
    AGGREGATION_EXECUTOR,
    PROJECTION_EXECUTOR,
    SORT_EXECUTOR,
    DISTINCT_EXECUTOR,
    PRODUCT_EXECUTOR,

    NESTED_LOOP_JOIN_EXECUTOR, 
    HASH_JOIN_EXECUTOR, // only in-memory hash joins for now. 

    SUB_QUERY_EXECUTOR, // used as a cache for non-corelated subqueries.

    UNION_EXECUTOR,
    EXCEPT_EXECUTOR,
    INTERSECT_EXECUTOR,
};

struct Executor {

    Executor(Arena* arena, QueryCTX* ctx, AlgebraOperation* plan_node, TableSchema* output_schema,
            Executor* child,
            ExecutorType type);
    virtual void init() = 0;
    virtual Tuple next() = 0;

    Tuple output_;
    QueryCTX* ctx_ = nullptr;
    AlgebraOperation* plan_node_ = nullptr;
    Executor* child_executor_ = nullptr;
    TableSchema* output_schema_ = nullptr;
    int query_idx_ = -1;
    int parent_query_idx_ = -1;
    ExecutorType type_;
    bool error_status_ = 0;
    bool finished_ = 0;
};

struct FilterExecutor : public Executor {

    FilterExecutor(Arena* arena, QueryCTX* ctx, AlgebraOperation* plan_node, Executor* child);
    void init();
    Tuple next();

    FlatExpr* filter_ = nullptr;
    /*
    Vector<ExpressionNode*> fields_ = {};
    Vector<String> field_names_ = {};
    */
};

struct NestedLoopJoinExecutor : public Executor {

    NestedLoopJoinExecutor(Arena* arena, QueryCTX* ctx, AlgebraOperation* plan_node, Executor* lhs, Executor* rhs);
    void init();
    Tuple next();

    Tuple left_output_;
    Executor* left_child_ = nullptr;
    Executor* right_child_ = nullptr;
    FlatExpr* filter_ = nullptr;
    JoinType join_type_ = INNER_JOIN;
    bool right_child_have_reset_ = false;
    bool left_output_visited_ = false;
};

struct ProductExecutor : public Executor {

    ProductExecutor(Arena* arena, QueryCTX* ctx, AlgebraOperation* plan_node, Executor* lhs, Executor* rhs);
    void init();
    Tuple next();

    Executor* left_child_ = nullptr;
    Executor* right_child_ = nullptr;
};


struct HashJoinExecutor : public Executor {

    HashJoinExecutor(Arena* arena, QueryCTX* ctx, AlgebraOperation* plan_node, Executor* lhs, Executor* rhs);
    void init();
    // TODO: implement merg and nested loop joines, 
    // hash join is not good for cases of none equality conditions, and full outer joins.
    Tuple next();

    Executor* left_child_ = nullptr;
    Executor* right_child_ = nullptr;
    Vector<int> left_child_fields_;
    Vector<int> right_child_fields_;
    // duplicated_idx tracks last used hashed value in case of hashing on non unique keys.
    // for example: the hash key is the field 'a' and this field is not unique and may have a duplicated value of 1,
    // in that case when a join happens we store it in the hash table as 1 -> {tuple 1, tuple 2, tuple 3}.
    // when we call next() on a a key such as the previous example we track the last tuple that has been returned.
    String8 prev_key_ = {};
    std::pmr::unordered_map<String8, Vector<Tuple>, String_hash, String_eq> hashed_left_child_;
    // tracks left keys that didn't find a match and can be used for left and full outer joins.
    std::pmr::set<String8> non_visited_left_keys_; 
    FlatExpr* filter_ = nullptr;
    JoinType join_type_ = INNER_JOIN;
    // the value of -1 means that we will use the first tuple.
    int duplicated_idx_ = -1; 
};

struct UnionExecutor : public Executor {

    UnionExecutor(Arena* arena, QueryCTX* ctx, AlgebraOperation* plan_node, Executor* lhs, Executor* rhs);
    void init();
    Tuple next();

    Executor* left_child_ = nullptr;
    Executor* right_child_ = nullptr;
};

struct ExceptExecutor : public Executor {

    ExceptExecutor(Arena* arena, QueryCTX* ctx, AlgebraOperation* plan_node, Executor* lhs, Executor* rhs);
    void init();
    Tuple next();

    Executor* left_child_ = nullptr;
    Executor* right_child_ = nullptr;
    std::pmr::unordered_map<String8, int, String_hash, String_eq> hashed_tuples_;
};

struct IntersectExecutor : public Executor {

    IntersectExecutor(Arena* arena, QueryCTX* ctx, AlgebraOperation* plan_node, Executor* lhs, Executor* rhs);
    void init();
    Tuple next();

    Executor* left_child_ = nullptr;
    Executor* right_child_ = nullptr;
    std::pmr::unordered_map<String8, int, String_hash, String_eq> hashed_tuples_;
};



struct SeqScanExecutor : public Executor {

    SeqScanExecutor(Arena* arena, QueryCTX* ctx, AlgebraOperation* plan_node, TableSchema* table);
    void init();
    Tuple next();

    TableSchema* table_        = nullptr;
    Vector<FlatExpr*> filters_;
    TableIterator it_;
};

struct IndexScanExecutor : public Executor {

    IndexScanExecutor(Arena* arena, QueryCTX* ctx, AlgebraOperation* plan_node, TableSchema* table, IndexHeader index);
    void assign_iterators();
    void init();
    Tuple next();

    IndexHeader index_header_ = {};
    TableSchema* table_ = nullptr;
    FileID table_fid_ = INVALID_FID;
    Vector<FlatExpr*> index_filters_;
    Vector<FlatExpr*> filters_;
    IndexIterator start_it_{};
    IndexKey search_key_;
};

struct InsertionExecutor : public Executor {

    InsertionExecutor(Arena* arena, QueryCTX* ctx, AlgebraOperation* plan_node, TableSchema* table, Vector<IndexHeader> indexes, int select_idx);
    void init();
    Tuple next();

    Vector<IndexHeader> indexes_;
    TableSchema* table_ = nullptr;
    InsertStatementData* statement_ = nullptr;
    int select_idx_ = -1;
};

struct DeletionExecutor : public Executor {

    DeletionExecutor(Arena* arena, QueryCTX* ctx, AlgebraOperation* plan_node, Executor* child, TableSchema* table,
            Vector<IndexHeader> indexes);
    void init();
    Tuple next();

    Vector<IndexHeader> indexes_;
    TableSchema* table_ = nullptr;
    DeleteStatementData* statement_ = nullptr;
};

struct UpdateExecutor : public Executor {

    UpdateExecutor(Arena* arena, QueryCTX* ctx, AlgebraOperation* plan_node, Executor* child, TableSchema* table,
            Vector<IndexHeader> indexes);
    void init();
    Tuple next();

    Vector<IndexHeader> indexes_;
    TableSchema* table_ = nullptr;
    UpdateStatementData* statement_ = nullptr;
};

struct AggregationExecutor : public Executor {

    AggregationExecutor(Arena* arena, QueryCTX* ctx, AlgebraOperation* plan_node, Executor* child_executor);
    void init();
    Tuple next();

    Vector<AggregateFuncNode*> *aggregates_;
    Vector<ASTNode*> *group_by_;
    std::pmr::unordered_map<String8, std::pair<Tuple, int>, String_hash, String_eq> aggregated_values_;
    // this hash table holds a set of values for each aggregate function 
    // if that function uses the distinct keyword inside the clause e.g: count(distinct a).
    // the mapping string is the hashed_key that is generated from the group by clause,
    // and each key has it's own distinct couter map inside of it, and each map has two values,
    // first value is the index of the function inside of the aggregates_ array,
    // second value is the set of values that has been parameters to that function so far.
    // TODO: pick a better structure to implement this functionality
    std::pmr::unordered_map<String8, std::unordered_map<int, std::set<String8>>, String_hash, String_eq> distinct_counters_; 
    std::pmr::unordered_map<String8, std::pair<Tuple, int>>::iterator it_;
};

struct ProjectionExecutor : public Executor {

    ProjectionExecutor(Arena* arena, QueryCTX* ctx, AlgebraOperation* plan_node, Executor* child_executor);
    void init();
    Tuple next();

    // child_executor_ is optional in case of projection for example : select 1 + 1 should work without a from clause.
    Vector<FlatExpr*> fields_ {};
};

struct SortExecutor : public Executor {

    SortExecutor(Arena* arena, QueryCTX* ctx, AlgebraOperation* plan_node, Executor* child_executor);
    void init();
    Tuple next();

    Vector<int> *order_by_list_;
    Vector<Tuple> tuples_;
    int idx_ = 0;
};

struct DistinctExecutor : public Executor {

    DistinctExecutor(Arena* arena, QueryCTX* ctx, Executor* child_executor);
    void init();
    Tuple next();

    std::pmr::unordered_map<String8, int, String_hash, String_eq> hashed_tuples_;
};

struct SubQueryExecutor : public Executor {

    SubQueryExecutor(Arena* arena, QueryCTX* ctx, Executor* child_executor);
    void init();
    Tuple next();

    // TODO: replace this with a tempory table.
    std::pmr::list<Tuple> tuple_list_; 
    std::pmr::list<Tuple>::iterator it_; 
    bool cached_ = false;
};

#endif // EXECUTOR_H
