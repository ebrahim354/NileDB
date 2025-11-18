#ifndef EXECUTOR_H
#define EXECUTOR_H

#include "string"
#include "ast_nodes.h"

struct QueryCTX;
struct InsertStatementData;
enum ExecutorType {
    SEQUENTIAL_SCAN_EXECUTOR = 0,
    INDEX_SCAN_EXECUTOR,
    INSERTION_EXECUTOR,
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

std::string exec_type_to_string(ExecutorType t);

struct Executor {
    Executor(ExecutorType type, TableSchema* output_schema, QueryCTX& ctx, 
            int query_idx, int parent_query_idx, Executor* child);

    virtual ~Executor();

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

struct FilterExecutor : public Executor {
    FilterExecutor(Executor* child, TableSchema* output_schema, ExpressionNode* filter, 
            std::vector<ExpressionNode*>& fields, 
            std::vector<std::string>& field_names,
            QueryCTX& ctx,
            int query_idx,
            int parent_query_idx);
    ~FilterExecutor();

    void init();
    std::vector<Value> next();

    ExpressionNode* filter_ = nullptr;
    std::vector<ExpressionNode*> fields_ = {};
    std::vector<std::string> field_names_ = {};
};

struct NestedLoopJoinExecutor : public Executor {
        NestedLoopJoinExecutor(TableSchema* output_schema, QueryCTX& ctx, int query_idx, int parent_query_idx, Executor* lhs, Executor* rhs, ExpressionNode* filter, JoinType type);
        ~NestedLoopJoinExecutor();

        void init();
        std::vector<Value> next();

        Executor* left_child_ = nullptr;
        std::vector<Value> left_output_;
        Executor* right_child_ = nullptr;
        ExpressionNode* filter_ = nullptr;
        JoinType join_type_ = INNER_JOIN;
        bool right_child_have_reset_ = false;
        bool left_output_visited_ = false;
};

struct ProductExecutor : public Executor {
        ProductExecutor(TableSchema* output_schema, QueryCTX& ctx, int query_idx, int parent_query_idx, Executor* lhs, Executor* rhs);
        ~ProductExecutor();

        void init();
        std::vector<Value> next();

        Executor* left_child_ = nullptr;
        Executor* right_child_ = nullptr;
};


struct HashJoinExecutor : public Executor {
        HashJoinExecutor(TableSchema* output_schema, QueryCTX& ctx, int query_idx, int parent_query_idx, Executor* lhs, Executor* rhs, ExpressionNode* filter, JoinType type);
        ~HashJoinExecutor();

        void init();
        // TODO: implement merg and nested loop joines, 
        // hash join is not good for cases of none equality conditions, and full outer joins.
        std::vector<Value> next();

        Executor* left_child_ = nullptr;
        Executor* right_child_ = nullptr;
        JoinType join_type_ = INNER_JOIN;
        std::vector<int> left_child_fields_;
        // duplicated_idx tracks last used hashed value in case of hashing on non unique keys.
        // for example: the hash key is the field 'a' and this field is not unique and may have a duplicated value of 1,
        // in that case when a join happens we store it in the hash table as 1 -> {tuple 1, tuple 2, tuple 3}.
        // when we call next() on a a key such as the previous example we track the last tuple that has been returned.
        // the value of -1 means that we will use the first tuple.
        int duplicated_idx_ = -1; 
        std::string prev_key_ = "";
        std::vector<int> right_child_fields_;
        ExpressionNode* filter_ = nullptr;
        std::unordered_map<std::string, std::vector<std::vector<Value>>> hashed_left_child_;
        // tracks left keys that didn't find a match and can be used for left and full outer joins.
        std::set<std::string> non_visited_left_keys_; 
};

struct UnionExecutor : public Executor {
        UnionExecutor(TableSchema* output_schema, QueryCTX& ctx, int query_idx, int parent_query_idx, Executor* lhs, Executor* rhs);
        // doesn't own its children, so no cleaning needed.
        ~UnionExecutor();

        void init();
        std::vector<Value> next();

        Executor* left_child_ = nullptr;
        Executor* right_child_ = nullptr;
};

struct ExceptExecutor : public Executor {
        ExceptExecutor(TableSchema* output_schema, QueryCTX& ctx, int query_idx, int parent_query_idx, Executor* lhs, Executor* rhs);
        // doesn't own its children, so no cleaning needed.
        ~ExceptExecutor();

        void init();
        std::vector<Value> next();

        Executor* left_child_ = nullptr;
        Executor* right_child_ = nullptr;
        std::unordered_map<std::string, int> hashed_tuples_;
};

struct IntersectExecutor : public Executor {
    public:
        IntersectExecutor(TableSchema* output_schema, QueryCTX& ctx, int query_idx, int parent_query_idx, Executor* lhs, Executor* rhs);
        // doesn't own its children, so no cleaning needed.
        ~IntersectExecutor();

        void init();
        std::vector<Value> next();

        Executor* left_child_ = nullptr;
        Executor* right_child_ = nullptr;
        std::unordered_map<std::string, int> hashed_tuples_;
};



struct SeqScanExecutor : public Executor {
        SeqScanExecutor(TableSchema* table, QueryCTX& ctx, int query_idx, int parent_query_idx);
        ~SeqScanExecutor();

        void init();
        std::vector<Value> next();

        TableSchema* table_ = nullptr;
        TableIterator* it_ = nullptr;
};

struct IndexScanExecutor : public Executor {
        // TODO: change BTreeIndex type to be a generic ( just Index ) that might be a btree or hash index.
        IndexScanExecutor(IndexHeader index, ASTNode* filter,
                TableSchema* table, QueryCTX& ctx, int query_idx, int parent_query_idx);
        ~IndexScanExecutor();

        void assign_iterators();
        void init();
        std::vector<Value> next();

        TableSchema* table_ = nullptr;
        IndexHeader index_header_ = {};
        ASTNode* filter_ = nullptr;
        IndexIterator start_it_{};
        IndexIterator end_it_{};
};

struct InsertionExecutor : public Executor {
        InsertionExecutor(TableSchema* table, std::vector<IndexHeader> indexes, QueryCTX& ctx, int query_idx, int parent_query_idx, int select_idx);
        ~InsertionExecutor();

        void init();
        std::vector<Value> next();

        TableSchema* table_ = nullptr;
        std::vector<IndexHeader> indexes_;
        InsertStatementData* statement = nullptr;
        int select_idx_ = -1;
        std::vector<Value> vals_  {};
};

struct AggregationExecutor : public Executor {
        AggregationExecutor(Executor* child_executor, TableSchema* output_schema, 
                std::vector<AggregateFuncNode*> aggregates, std::vector<ASTNode*> group_by, QueryCTX& ctx, int query_idx, int parent_query_idx);
        ~AggregationExecutor();

        void init();
        std::vector<Value> next();

        std::vector<AggregateFuncNode*> aggregates_;
        std::vector<ASTNode*> group_by_;
        std::unordered_map<std::string, std::vector<Value>> aggregated_values_;
        // this hash table holds a set of values for each aggregate function 
        // if that function uses the distinct keyword inside the clause e.g: count(distinct a).
        // the mapping string is the hashed_key that is generated from the group by clause,
        // and each key has it's own distinct couter map inside of it, and each map has two values,
        // first value is the index of the function inside of the aggregates_ array,
        // second value is the set of values that has been parameters to that function so far.
        // TODO: pick a better structure to implement this functionality
        std::unordered_map<std::string, std::unordered_map<int, std::set<std::string>>> distinct_counters_; 
        std::unordered_map<std::string, std::vector<Value>>::iterator it_;
};

struct ProjectionExecutor : public Executor {
        ProjectionExecutor(Executor* child_executor, TableSchema* output_schema, std::vector<ExpressionNode*> fields, QueryCTX& ctx, int query_idx, int parent_query_idx);
        ~ProjectionExecutor();

        void init();
        std::vector<Value> next();

        // child_executor_ is optional in case of projection for example : select 1 + 1 should work without a from clause.
        std::vector<ExpressionNode*> fields_ {};
};

struct SortExecutor : public Executor {
        SortExecutor(Executor* child_executor , TableSchema* output_schema, std::vector<int> order_by_list, QueryCTX& ctx, int query_idx, int parent_query_idx);
        ~SortExecutor();

        void init();
        std::vector<Value> next();

        std::vector<int> order_by_list_;
        std::vector<std::vector<Value>> tuples_;
        int idx_ = 0;
};

struct DistinctExecutor : public Executor {
        DistinctExecutor(Executor* child_executor , TableSchema* output_schema, QueryCTX& ctx, int query_idx, int parent_query_idx);
        ~DistinctExecutor();

        void init();
        std::vector<Value> next();

        std::unordered_map<std::string, int> hashed_tuples_;
};

struct SubQueryExecutor : public Executor {
        SubQueryExecutor(Executor* child_executor, TableSchema* output_schema, QueryCTX& ctx, int query_idx, int parent_query_idx);
        ~SubQueryExecutor();

        void init();
        std::vector<Value> next();

        // TODO: replace this with a tempory table.
        std::list<std::vector<Value>> tuple_list_; 
        std::list<std::vector<Value>>::iterator it_; 
        bool cached_ = false;
};

#endif // EXECUTOR_H
