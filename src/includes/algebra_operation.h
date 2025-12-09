#ifndef ALGEBRA_OPERATION_H
#define ALGEBRA_OPERATION_H


#include "ast_nodes.h"


//void accessed_tables(ASTNode* expression ,Vector<String>& tables, Catalog* catalog, bool only_one);
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
    DELETION,
    UPDATE,

    // two table operations.
    PRODUCT,
    JOIN,
    // set operations
    // AL => algebra.
    AL_UNION, 
    AL_EXCEPT,
    AL_INTERSECT,
};

enum ScanType {
    SEQ_SCAN,
    INDEX_SCAN,
};

struct AlgebraOperation {
    virtual void print(int prefix_space_cnt) = 0;
    void init(int query_idx, AlgebraOperationType type);

    AlgebraOperationType type_;
    int query_idx_ = -1;
    int query_parent_idx_ = -1;
    bool distinct_ = false;

};

struct ScanOperation: AlgebraOperation {
    void init(int query_idx, String table_name, String table_rename);
    void print(int prefix_space_cnt);

    String table_name_   = {};
    String table_rename_ = {};
    String index_name_   = {};
    ScanType scan_type_       = SEQ_SCAN;
    ASTNode* filter_   = nullptr;
};

struct UnionOperation: AlgebraOperation {
    void init(int query_idx, AlgebraOperation* lhs, AlgebraOperation* rhs, bool all);
    void print(int prefix_space_cnt);

    AlgebraOperation* lhs_ = nullptr;
    AlgebraOperation* rhs_ = nullptr;
    bool all_ = false;
};

struct ExceptOperation: AlgebraOperation {
    void init(int query_idx, AlgebraOperation* lhs, AlgebraOperation* rhs, bool all);
    void print(int prefix_space_cnt);

    AlgebraOperation* lhs_ = nullptr;
    AlgebraOperation* rhs_ = nullptr;
    bool all_ = false;
};

struct IntersectOperation: AlgebraOperation {
    void init(int query_idx, AlgebraOperation* lhs, AlgebraOperation* rhs, bool all);
    void print(int prefix_space_cnt);

    AlgebraOperation* lhs_ = nullptr;
    AlgebraOperation* rhs_ = nullptr;
    bool all_ = false;
};

struct ProductOperation: AlgebraOperation {
    void init(int query_idx, AlgebraOperation* lhs, AlgebraOperation* rhs);
    void print(int prefix_space_cnt);

    AlgebraOperation* lhs_ = nullptr;
    AlgebraOperation* rhs_ = nullptr;
};

enum JoinAlgorithm {
    NESTED_LOOP_JOIN,
    HASH_JOIN
};

struct JoinOperation: AlgebraOperation {
    void init(int query_idx, AlgebraOperation* lhs,
            AlgebraOperation* rhs,
            ExpressionNode* filter,
            JoinType type, JoinAlgorithm join_algo);

    void print(int prefix_space_cnt);

    AlgebraOperation* lhs_ = nullptr;
    AlgebraOperation* rhs_ = nullptr;
    ExpressionNode* filter_;
    JoinType join_type_ = INNER_JOIN;
    JoinAlgorithm join_algo_ = NESTED_LOOP_JOIN;
};

struct InsertionOperation: AlgebraOperation {
    void init(int query_idx);
    void print(int prefix_space_cnt);

    AlgebraOperation* child_ = nullptr;
};

struct DeletionOperation: AlgebraOperation {
    void init(AlgebraOperation* child_,int query_idx);
    void print(int prefix_space_cnt);

    AlgebraOperation* child_ = nullptr;
};

struct UpdateOperation: AlgebraOperation {
    void init(AlgebraOperation*, int query_idx);
    void print(int prefix_space_cnt);

    AlgebraOperation* child_ = nullptr;
};

struct FilterOperation: AlgebraOperation {
    void init(int query_idx, AlgebraOperation* child, ExpressionNode* filter);
            /*
            ,Vector<ExpressionNode*>& fields, 
            Vector<String>& field_names);*/
    void print(int prefix_space_cnt);

    ExpressionNode* filter_;
    /*
    Vector<ExpressionNode*> fields_;
    Vector<String> field_names_;
    */
    AlgebraOperation* child_;
};

struct AggregationOperation: AlgebraOperation {
    public:
        void init(int query_idx, AlgebraOperation* child, Vector<AggregateFuncNode*> aggregates,
                Vector<ASTNode*> group_by);
        void print(int prefix_space_cnt);

        AlgebraOperation* child_ = nullptr;
        Vector<AggregateFuncNode*> aggregates_;
        Vector<ASTNode*> group_by_;
};


struct ProjectionOperation: AlgebraOperation {
    public:
        void init(int query_idx, AlgebraOperation* child, Vector<ExpressionNode*> fields);
        void print(int prefix_space_cnt);

        AlgebraOperation* child_ = nullptr;
        Vector<ExpressionNode*> fields_;
};

struct SortOperation: AlgebraOperation {
    AlgebraOperation* child_ = nullptr;
    Vector<int> order_by_list_;

    void init(int query_idx, AlgebraOperation* child, Vector<int> order_by_list);
    void print(int prefix_space_cnt);
};

#endif // ALGEBRA_OPERATION_H
