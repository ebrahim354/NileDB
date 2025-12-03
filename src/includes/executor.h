#ifndef EXECUTOR_H
#define EXECUTOR_H

#include "string"
#include "ast_nodes.h"
#include "algebra_operation.h"

struct QueryCTX;
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

std::string exec_type_to_string(ExecutorType t);

struct Executor {

    void construct(QueryCTX* ctx, AlgebraOperation* plan_node, TableSchema* output_schema,
            Executor* child,
            ExecutorType type);
    virtual void init() = 0;
    virtual Tuple next() = 0;

    QueryCTX* ctx_ = nullptr;
    AlgebraOperation* plan_node_ = nullptr;
    Executor* child_executor_ = nullptr;
    TableSchema* output_schema_ = nullptr;
    Tuple output_;
    int query_idx_ = -1;
    int parent_query_idx_ = -1;
    ExecutorType type_;
    bool error_status_ = 0;
    bool finished_ = 0;
};

struct FilterExecutor : public Executor {

    void construct(QueryCTX* ctx, AlgebraOperation* plan_node, Executor* child);
    void init();
    Tuple next();

    ExpressionNode* filter_ = nullptr;
    std::vector<ExpressionNode*> fields_ = {};
    std::vector<std::string> field_names_ = {};
};

struct NestedLoopJoinExecutor : public Executor {

    void construct(QueryCTX* ctx, AlgebraOperation* plan_node, Executor* lhs, Executor* rhs);
    void init();
    Tuple next();

    Executor* left_child_ = nullptr;
    Tuple left_output_;
    Executor* right_child_ = nullptr;
    ExpressionNode* filter_ = nullptr;
    JoinType join_type_ = INNER_JOIN;
    bool right_child_have_reset_ = false;
    bool left_output_visited_ = false;
};

struct ProductExecutor : public Executor {

    void construct(QueryCTX* ctx, AlgebraOperation* plan_node, Executor* lhs, Executor* rhs);
    void init();
    Tuple next();

    Executor* left_child_ = nullptr;
    Executor* right_child_ = nullptr;
};


struct HashJoinExecutor : public Executor {

    void construct(QueryCTX* ctx, AlgebraOperation* plan_node, Executor* lhs, Executor* rhs);
    void init();
    // TODO: implement merg and nested loop joines, 
    // hash join is not good for cases of none equality conditions, and full outer joins.
    Tuple next();

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
    std::unordered_map<std::string, std::vector<Tuple>> hashed_left_child_;
    // tracks left keys that didn't find a match and can be used for left and full outer joins.
    std::set<std::string> non_visited_left_keys_; 
};

struct UnionExecutor : public Executor {

    void construct(QueryCTX* ctx, AlgebraOperation* plan_node, Executor* lhs, Executor* rhs);
    void init();
    Tuple next();

    Executor* left_child_ = nullptr;
    Executor* right_child_ = nullptr;
};

struct ExceptExecutor : public Executor {

    void construct(QueryCTX* ctx, AlgebraOperation* plan_node, Executor* lhs, Executor* rhs);
    void init();
    Tuple next();

    Executor* left_child_ = nullptr;
    Executor* right_child_ = nullptr;
    std::unordered_map<std::string, int> hashed_tuples_;
};

struct IntersectExecutor : public Executor {

    void construct(QueryCTX* ctx, AlgebraOperation* plan_node, Executor* lhs, Executor* rhs);
    void init();
    Tuple next();

    Executor* left_child_ = nullptr;
    Executor* right_child_ = nullptr;
    std::unordered_map<std::string, int> hashed_tuples_;
};



struct SeqScanExecutor : public Executor {

    void construct(QueryCTX* ctx, AlgebraOperation* plan_node, TableSchema* table);
    void init();
    Tuple next();

    TableSchema* table_ = nullptr;
    TableIterator it_;
};

struct IndexScanExecutor : public Executor {
        // TODO: change BTreeIndex type to be a generic ( just Index ) that might be a btree or hash index.

        void construct(QueryCTX* ctx, AlgebraOperation* plan_node, TableSchema* table, IndexHeader index);
        void assign_iterators();
        void init();
        Tuple next();

        TableSchema* table_ = nullptr;
        IndexHeader index_header_ = {};
        ASTNode* filter_ = nullptr;
        IndexIterator start_it_{};
        IndexIterator end_it_{};
};

struct InsertionExecutor : public Executor {

        void construct(QueryCTX* ctx, AlgebraOperation* plan_node, TableSchema* table, std::vector<IndexHeader> indexes,
            int select_idx);
        void init();
        Tuple next();

        TableSchema* table_ = nullptr;
        std::vector<IndexHeader> indexes_;
        InsertStatementData* statement_ = nullptr;
        int select_idx_ = -1;
};

struct DeletionExecutor : public Executor {

        void construct(QueryCTX* ctx, AlgebraOperation* plan_node, Executor* child, TableSchema* table,
        std::vector<IndexHeader> indexes);
        void init();
        Tuple next();

        TableSchema* table_ = nullptr;
        std::vector<IndexHeader> indexes_;
        DeleteStatementData* statement_ = nullptr;
        std::set<u64> affected_records; // store hashes of affected record id-s To prevent the halloween problem.
};

struct UpdateExecutor : public Executor {

        void construct(QueryCTX* ctx, AlgebraOperation* plan_node, Executor* child, TableSchema* table,
        std::vector<IndexHeader> indexes);
        void init();
        Tuple next();

        TableSchema* table_ = nullptr;
        std::vector<IndexHeader> indexes_;
        UpdateStatementData* statement_ = nullptr;
        std::set<u64> affected_records; // store hashes of affected record id-s To prevent the halloween problem.
};

struct AggregationExecutor : public Executor {

        void construct(QueryCTX* ctx, AlgebraOperation* plan_node, Executor* child_executor);
        void init();
        Tuple next();

        std::vector<AggregateFuncNode*> aggregates_;
        std::vector<ASTNode*> group_by_;
        std::unordered_map<std::string, std::pair<Tuple, int>> aggregated_values_;
        // this hash table holds a set of values for each aggregate function 
        // if that function uses the distinct keyword inside the clause e.g: count(distinct a).
        // the mapping string is the hashed_key that is generated from the group by clause,
        // and each key has it's own distinct couter map inside of it, and each map has two values,
        // first value is the index of the function inside of the aggregates_ array,
        // second value is the set of values that has been parameters to that function so far.
        // TODO: pick a better structure to implement this functionality
        std::unordered_map<std::string, std::unordered_map<int, std::set<std::string>>> distinct_counters_; 
        std::unordered_map<std::string, std::pair<Tuple, int>>::iterator it_;
};

struct ProjectionExecutor : public Executor {

        void construct(QueryCTX* ctx, AlgebraOperation* plan_node, Executor* child_executor);
        void init();
        Tuple next();

        // child_executor_ is optional in case of projection for example : select 1 + 1 should work without a from clause.
        std::vector<ExpressionNode*> fields_ {};
};

struct SortExecutor : public Executor {

        void construct(QueryCTX* ctx, AlgebraOperation* plan_node, Executor* child_executor);
        void init();
        Tuple next();

        std::vector<int> order_by_list_;
        std::vector<Tuple> tuples_;
        int idx_ = 0;
};

struct DistinctExecutor : public Executor {

        void construct(QueryCTX* ctx, Executor* child_executor);
        void init();
        Tuple next();

        std::unordered_map<std::string, int> hashed_tuples_;
};

struct SubQueryExecutor : public Executor {

        void construct(QueryCTX* ctx, Executor* child_executor);
        void init();
        Tuple next();

        // TODO: replace this with a tempory table.
        std::list<Tuple> tuple_list_; 
        std::list<Tuple>::iterator it_; 
        bool cached_ = false;
};

#endif // EXECUTOR_H
