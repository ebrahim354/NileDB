#pragma once
//#include "query_ctx.cpp"
#include "ast_nodes.cpp"
void accessed_tables(ASTNode* expression ,std::vector<std::string>& tables, Catalog* catalog, bool only_one);


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

enum ScanType {
    SEQ_SCAN,
    INDEX_SCAN,
};

struct ScanOperation: AlgebraOperation {
    public:
        ScanOperation(QueryCTX& ctx,std::string table_name, std::string table_rename): 
            AlgebraOperation(SCAN, ctx),
            table_name_(table_name),
            table_rename_(table_rename)
        {}
        ~ScanOperation()
        {}
        void print(int prefix_space_cnt) {
            for(int i = 0; i < prefix_space_cnt; ++i)
                std::cout << " ";
            std::cout << "Scan operation, name: " << table_name_ << " rename: " << table_rename_;
            std::cout << " type: " << (scan_type_ == SEQ_SCAN ? "SEQ_SCAN " : "INDEX_SCAN ");
            if(filter_)
                std::cout << "filter: " << filter_->token_.val_;
            std::cout << "\n";
        }
        std::string table_name_   = {};
        std::string table_rename_ = {};
        std::string index_name_   = {};
        ScanType scan_type_       = SEQ_SCAN;
        ASTNode* filter_   = nullptr;
};

struct UnionOperation: AlgebraOperation {
    public:
        UnionOperation(QueryCTX& ctx, AlgebraOperation* lhs, AlgebraOperation* rhs, bool all): 
            AlgebraOperation(AL_UNION, ctx), lhs_(lhs), rhs_(rhs), all_(all)
        {}
        ~UnionOperation()
        {
            delete lhs_;
            delete rhs_;
        }
        void print(int prefix_space_cnt) {
            for(int i = 0; i < prefix_space_cnt; ++i)
                std::cout << " ";
          std::cout << "union operation\n"; 
          std::cout << " lhs:\n "; 
          lhs_->print(prefix_space_cnt + 1);
          std::cout << " rhs:\n "; 
          rhs_->print(prefix_space_cnt + 1);
        }

        AlgebraOperation* lhs_ = nullptr;
        AlgebraOperation* rhs_ = nullptr;
        bool all_ = false;
};

struct ExceptOperation: AlgebraOperation {
    public:
        ExceptOperation(QueryCTX& ctx, AlgebraOperation* lhs, AlgebraOperation* rhs, bool all): 
            AlgebraOperation(AL_EXCEPT, ctx), lhs_(lhs), rhs_(rhs), all_(all)
        {}
        ~ExceptOperation()
        {
            delete lhs_;
            delete rhs_;
        }
        void print(int prefix_space_cnt) {
            for(int i = 0; i < prefix_space_cnt; ++i)
                std::cout << " ";
          std::cout << "except operation\n"; 
          lhs_->print(prefix_space_cnt + 1);
          rhs_->print(prefix_space_cnt + 1);
        }

        AlgebraOperation* lhs_ = nullptr;
        AlgebraOperation* rhs_ = nullptr;
        bool all_ = false;
};

struct IntersectOperation: AlgebraOperation {
    public:
        IntersectOperation(QueryCTX& ctx, AlgebraOperation* lhs, AlgebraOperation* rhs, bool all): 
            AlgebraOperation(AL_INTERSECT, ctx), lhs_(lhs), rhs_(rhs), all_(all)
        {}
        ~IntersectOperation()
        {
            delete lhs_;
            delete rhs_;
        }
        void print(int prefix_space_cnt) {
            for(int i = 0; i < prefix_space_cnt; ++i)
                std::cout << " ";
          std::cout << "intersect operation\n"; 
          lhs_->print(prefix_space_cnt + 1);
          rhs_->print(prefix_space_cnt + 1);
        }

        AlgebraOperation* lhs_ = nullptr;
        AlgebraOperation* rhs_ = nullptr;
        bool all_ = false;
};

struct ProductOperation: AlgebraOperation {
    public:
        ProductOperation(QueryCTX& ctx,AlgebraOperation* lhs, AlgebraOperation* rhs): 
            AlgebraOperation(PRODUCT, ctx), lhs_(lhs), rhs_(rhs)
        {}
        ~ProductOperation()
        {
            delete lhs_;
            delete rhs_;
        }
        void print(int prefix_space_cnt) {
            for(int i = 0; i < prefix_space_cnt; ++i)
                std::cout << " ";
          std::cout << "product operation\n"; 
          lhs_->print(prefix_space_cnt + 1);
          rhs_->print(prefix_space_cnt + 1);
        }

        AlgebraOperation* lhs_ = nullptr;
        AlgebraOperation* rhs_ = nullptr;
};

enum JoinAlgorithm {
    NESTED_LOOP_JOIN,
    HASH_JOIN
};

struct JoinOperation: AlgebraOperation {
    public:
        JoinOperation(QueryCTX& ctx, AlgebraOperation* lhs, AlgebraOperation* rhs, 
            ExpressionNode* filter, JoinType type, JoinAlgorithm join_algo): 
            AlgebraOperation(JOIN, ctx), lhs_(lhs), rhs_(rhs),
            filter_(filter), 
            join_type_(type),
            join_algo_(join_algo)
        {}
        ~JoinOperation()
        {
            delete lhs_;
            delete rhs_;
        }
        void print(int prefix_space_cnt) {
            for(int i = 0; i < prefix_space_cnt; ++i)
                std::cout << " ";
          std::cout << "join operation: "; 
          std::cout << (join_algo_ == NESTED_LOOP_JOIN ? "NESTED_LOOP_JOIN" : "HASH_JOIN") << "\n";
          lhs_->print(prefix_space_cnt + 1);
          rhs_->print(prefix_space_cnt + 1);
        }

        AlgebraOperation* lhs_ = nullptr;
        AlgebraOperation* rhs_ = nullptr;
        ExpressionNode* filter_;
        JoinType join_type_ = INNER_JOIN;
        JoinAlgorithm join_algo_ = NESTED_LOOP_JOIN;
};

struct InsertionOperation: AlgebraOperation {
    public:
        InsertionOperation(QueryCTX& ctx): 
            AlgebraOperation(INSERTION, ctx)
        {}
        void print(int prefix_space_cnt) {
            for(int i = 0; i < prefix_space_cnt; ++i)
                std::cout << " ";
          std::cout << "insertion operation\n"; 
        }
        ~InsertionOperation()
        {}
};

struct FilterOperation: AlgebraOperation {
    public:
        Catalog* catalog_ = nullptr;
        FilterOperation(QueryCTX& ctx,AlgebraOperation* child, ExpressionNode* filter, 
                std::vector<ExpressionNode*>& fields, 
                std::vector<std::string>& field_names,
                Catalog* catalog
                ): 
            AlgebraOperation(FILTER, ctx),
            child_(child), 
            filter_(filter),
            fields_(fields),
            field_names_(field_names),
            catalog_(catalog)
        {}
        ~FilterOperation()
        {
            delete child_;
        }

        void print(int prefix_space_cnt) {
            std::vector<std::string> table_access;
            accessed_tables(filter_, table_access, catalog_, true);
            for(int i = 0; i < prefix_space_cnt; ++i)
                std::cout << " ";
            std::cout << "filter operation "; 
            for(int i = 0; i < table_access.size(); ++i) std::cout << table_access[i] << " ";
            std::cout << "\n";
            if(child_)
                child_->print(prefix_space_cnt + 1);
        }
        ExpressionNode* filter_;
        std::vector<ExpressionNode*> fields_;
        std::vector<std::string> field_names_;
        AlgebraOperation* child_;
};

struct AggregationOperation: AlgebraOperation {
    public:
        AggregationOperation(QueryCTX& ctx, AlgebraOperation* child, std::vector<AggregateFuncNode*> aggregates, std::vector<ASTNode*> group_by): 
            AlgebraOperation(AGGREGATION, ctx),
            child_(child), 
            aggregates_(aggregates),
            group_by_(group_by)
        {}
        ~AggregationOperation()
        {
            delete child_;
        }
        void print(int prefix_space_cnt) {
            for(int i = 0; i < prefix_space_cnt; ++i)
                std::cout << " ";
          std::cout << "agg operation\n"; 
          if(child_)
            child_->print(prefix_space_cnt + 1);
        }
        AlgebraOperation* child_ = nullptr;
        std::vector<AggregateFuncNode*> aggregates_;
        std::vector<ASTNode*> group_by_;
};


struct ProjectionOperation: AlgebraOperation {
    public:
        ProjectionOperation(QueryCTX& ctx, AlgebraOperation* child, std::vector<ExpressionNode*> fields): 
            AlgebraOperation(PROJECTION, ctx),
            child_(child), 
            fields_(fields)
        {}
        ~ProjectionOperation()
        {
            delete child_;
        }
        void print(int prefix_space_cnt) {
            for(int i = 0; i < prefix_space_cnt; ++i)
                std::cout << " ";
          std::cout << "projection operation\n"; 
          if(child_)
            child_->print(prefix_space_cnt + 1);
        }
        AlgebraOperation* child_ = nullptr;
        std::vector<ExpressionNode*> fields_;
};

struct SortOperation: AlgebraOperation {
    public:
        SortOperation(QueryCTX& ctx, AlgebraOperation* child, std::vector<int> order_by_list): 
            AlgebraOperation(SORT, ctx),
            child_(child), 
            order_by_list_(order_by_list)
        {}
        ~SortOperation()
        {
            delete child_;
        }
        void print(int prefix_space_cnt) {
            for(int i = 0; i < prefix_space_cnt; ++i)
                std::cout << " ";
          std::cout << "sort operation\n"; 
          if(child_)
            child_->print(prefix_space_cnt + 1);
        }
        AlgebraOperation* child_ = nullptr;
        std::vector<int> order_by_list_;

};
