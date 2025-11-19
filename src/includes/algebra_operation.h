#ifndef ALGEBRA_OPERATION_H
#define ALGEBRA_OPERATION_H


#include "ast_nodes.h"


//void accessed_tables(ASTNode* expression ,std::vector<std::string>& tables, Catalog* catalog, bool only_one);
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
    void init(int query_idx, std::string table_name, std::string table_rename);
    void print(int prefix_space_cnt);

    std::string table_name_   = {};
    std::string table_rename_ = {};
    std::string index_name_   = {};
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
};

struct FilterOperation: AlgebraOperation {
    void init(int query_idx, AlgebraOperation* child, ExpressionNode* filter, 
            std::vector<ExpressionNode*>& fields, 
            std::vector<std::string>& field_names);
    void print(int prefix_space_cnt);

    ExpressionNode* filter_;
    std::vector<ExpressionNode*> fields_;
    std::vector<std::string> field_names_;
    AlgebraOperation* child_;
};

struct AggregationOperation: AlgebraOperation {
    public:
        void init(int query_idx, AlgebraOperation* child, std::vector<AggregateFuncNode*> aggregates,
                std::vector<ASTNode*> group_by);
        void print(int prefix_space_cnt);

        AlgebraOperation* child_ = nullptr;
        std::vector<AggregateFuncNode*> aggregates_;
        std::vector<ASTNode*> group_by_;
};


struct ProjectionOperation: AlgebraOperation {
    public:
        void init(int query_idx, AlgebraOperation* child, std::vector<ExpressionNode*> fields);
        void print(int prefix_space_cnt);

        AlgebraOperation* child_ = nullptr;
        std::vector<ExpressionNode*> fields_;
};

struct SortOperation: AlgebraOperation {
    AlgebraOperation* child_ = nullptr;
    std::vector<int> order_by_list_;

    void init(int query_idx, AlgebraOperation* child, std::vector<int> order_by_list);
    void print(int prefix_space_cnt);
};

#endif // ALGEBRA_OPERATION_H
