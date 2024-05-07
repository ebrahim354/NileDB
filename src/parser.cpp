#pragma once
#include "tokenizer.cpp"
#include "catalog.cpp"
#include <cstdint>

// The grammer rules are defined as structures, each struct is following the name convention: CategoryNameNode,
// for example: the constant category the struct is named ConstantNode.
// The reason they are followed by node is because the end result of the parser is suppose to 
// return an abstract syntax tree, the tree is returned by the root node (not as a stand alone AST class for example),
// the reason for this is to allow nested queries and recusion (or at least that's how I view it for now).
// different kinds of category are just structs that inherit the ASTNode's main attributes with 
// additional specialized attributes.
// inheritance is just an easy solution for now, It should be removed entirely when it starts to cause problems.
// The current implementation is going to cause a lot of memory leaks because we are using the heap with the "new"
// keyword without deleting nodes after we are done or in cases of an error, That is bad but we will replace heap
// allocations with some sort of an arena allocator or just handle the leaks later.


enum CategoryType {
    FIELD,
    TABLE,
    STRING_CONSTANT,
    INTEGER_CONSTANT,
    EXPRESSION,
    TERM,
    PREDICATE,

    SELECT_STATEMENT,
    SELECT_LIST,
    TABLE_LIST,

    UPDATE_STATEMENT,

    INSERT_STATEMENT,
    FIELD_LIST,
    CONST_LIST,

    DELETE_STATEMENT,


    TYPE_DEF,
    FIELD_DEF,
    FIELD_DEF_LIST,
    CREATE_TABLE_STATEMENT,
    // add views and indexes later.
};



struct ASTNode {
    ASTNode(CategoryType ct, Token val = {}): category_(ct), token_(val)
    {}
    CategoryType category_;
    Token token_; 
};

struct TermNode : ASTNode {
    TermNode(ASTNode* lhs, ASTNode* rhs, Token op): ASTNode(TERM, op), left_(lhs), right_(rhs)
    {}
    void clean(){
        delete left_;
        delete right_;
    }
    ASTNode* left_;
    ASTNode* right_;
};

struct FieldDefNode : ASTNode {
    FieldDefNode(): ASTNode(FIELD_DEF)
    {}
    void clean() {
        delete field_;
        delete type_;
    }
    ASTNode* field_;
    ASTNode* type_;
};




// this amound of specific list types is why inheritance is just bad. or maybe I'm doing it the wrong way.


// a linked list of constants linked by the ',' sympol.
struct FieldDefListNode : ASTNode {
    FieldDefListNode(): ASTNode(FIELD_DEF_LIST)
    {}
    void clean (){
        if(field_def_) field_def_->clean();
        if(next_) next_->clean();
        delete field_def_;
        delete next_;
    }
    FieldDefNode* field_def_ = nullptr;
    FieldDefListNode* next_ = nullptr;
};

// a linked list of constants linked by the ',' sympol.
struct ConstListNode : ASTNode {
    ConstListNode(): ASTNode(CONST_LIST)
    {}
    void clean (){
        if(next_) next_->clean();
        delete next_;
    }
    ConstListNode* next_ = nullptr;
};

// a linked list of table identifiers linked with the ',' sympol.
struct TableListNode : ASTNode {
    TableListNode(): ASTNode(TABLE_LIST)
    {}
    void clean (){
        if(next_) next_->clean();
        delete next_;
    }
    TableListNode* next_ = nullptr;
};

// a linked list of fields linked with the ',' sympol.
struct FieldListNode : ASTNode {
    FieldListNode(): ASTNode(SELECT_LIST)
    {}
    void clean(){
        if(next_) next_->clean();
        delete field_;
        delete next_;
    }
    ASTNode* field_ = nullptr;
    FieldListNode* next_ = nullptr;
};

// a linked list of terms linked with keywords (and, or etc...).
struct PredicateNode : ASTNode {
    PredicateNode(Token val = {}): ASTNode(PREDICATE, val)
    {}
    void clean () {
        if(next_) next_->clean();
        if(term_) term_->clean();
        delete term_;
        delete next_;
    }
    TermNode* term_ = nullptr;
    PredicateNode* next_ = nullptr;
};


// SQL statements.
struct SelectStatementNode : ASTNode {

    SelectStatementNode(): ASTNode(SELECT_STATEMENT)
    {}

    void clean (){
        if(fields_) fields_->clean();
        if(tables_) tables_->clean();
        if(predicate_) predicate_->clean();
        delete fields_;
        delete tables_;
        delete predicate_;
    }
    FieldListNode* fields_;
    TableListNode* tables_;
    PredicateNode* predicate_;
};

struct CreateTableStatementNode : ASTNode {

    CreateTableStatementNode(): ASTNode(CREATE_TABLE_STATEMENT)
    {}
    void clean() {
        if(field_defs_) field_defs_->clean();
        delete field_defs_;
        delete table_;
    }
    FieldDefListNode* field_defs_;
    ASTNode* table_;
};

struct DeleteStatementNode : ASTNode {

    DeleteStatementNode(): ASTNode(DELETE_STATEMENT)
    {}
    void clean (){
        if(predicate_) predicate_->clean();
        delete table_;
        delete predicate_;
    }
    ASTNode*  table_;
    PredicateNode* predicate_;
};

struct UpdateStatementNode : ASTNode {

    UpdateStatementNode(): ASTNode(UPDATE_STATEMENT)
    {}
    void clean (){
        if(predicate_) predicate_->clean();
        delete field_;
        delete table_;
        delete predicate_;
    }
    ASTNode*  table_;
    ASTNode*  field_;
    ASTNode*  expression_;
    PredicateNode* predicate_;
};

struct InsertStatementNode : ASTNode {

    InsertStatementNode(): ASTNode(INSERT_STATEMENT)
    {}
    void clean (){
        if(fields_) fields_->clean();
        if(values_) values_->clean();
        delete fields_;
        delete values_;
        delete table_;
    }
    ASTNode*  table_;
    FieldListNode* fields_;
    ConstListNode*  values_;
};




class Parser {
    public:
        Parser(Catalog* c): catalog_(c)
        {}
        ~Parser(){}

        ASTNode* field(){
            if(cur_pos_ < cur_size_ && tokens_[cur_pos_].type_ == IDENTIFIER) {
                return new ASTNode(FIELD, tokens_[cur_pos_++]);
            }
            return nullptr;
        }

        ASTNode* type(){
            if(cur_pos_ < cur_size_ && tokens_[cur_pos_].type_ == KEYWORD 
                    && tokenizer_.isDataType(tokens_[cur_pos_].val_)) {
                return new ASTNode(TYPE_DEF, tokens_[cur_pos_++]);
            }
            return nullptr;
        }

        FieldDefNode* fieldDef(){
            auto f = field();
            auto t = type();
            if(!f || !t){ 
                delete f;
                delete t;
                return  nullptr;
            }
            FieldDefNode* fd = new FieldDefNode();
            fd->field_ = f;
            fd->type_  = t;
            return fd;
        }


        // the user of this method is the one who should check to see if it's ok to use, this table name or not
        // using the catalog.
        // for example: a create statement should check if this table name is used before or not to avoid duplication,
        // however a select statement should check if this table name exists to search inside of it.
        // this usage is applied for all premitive Categories.
        ASTNode* table(){
            if(cur_pos_ < cur_size_ && tokens_[cur_pos_].type_ == IDENTIFIER){
                return new ASTNode(TABLE, tokens_[cur_pos_++]);
            }
            return nullptr;
        }
        
        ASTNode* constant(){
            if(cur_pos_ >= cur_size_)
                return nullptr;
            if(tokens_[cur_pos_].type_ == STR_CONSTANT)
                return new ASTNode(STRING_CONSTANT, tokens_[cur_pos_++]);
            if(tokens_[cur_pos_].type_ == INT_CONSTANT)
                return new ASTNode(INTEGER_CONSTANT, tokens_[cur_pos_++]);
            return nullptr;
        }

        ASTNode* expression(){
            ASTNode* ex = nullptr;
            ex = field();
            if(!ex) 
                ex = constant();
            if(ex)
                ex->category_ = EXPRESSION;

            return ex;
        }

        TermNode* term(){
            if(cur_size_ - cur_pos_ < 2 ) return nullptr;
            ASTNode* left = expression();
            if(!left) return nullptr;
            Token op = tokens_[cur_pos_++];
            ASTNode* right = expression();
            if(!right){
                delete left;
                return nullptr;
            }
            // works only for the = operator for now, extend later..
            if(op.val_ != "=") {
                delete left;
                delete right;
                return nullptr;
            }
            return new TermNode(left, right,op);
        }

        PredicateNode* predicate(){
            TermNode* t = term();
            if(!t) return nullptr;
            PredicateNode* nw_p = new PredicateNode();
            nw_p->term_ = t;
            // add support for different predicates later.
            if(cur_pos_ < cur_size_ && tokens_[cur_pos_].val_ == "AND"){
                nw_p->token_ = tokens_[cur_pos_++];
                nw_p->next_ = predicate();
            }
            return nw_p;
        }


        FieldDefListNode* fieldDefList(){
            FieldDefNode* f = fieldDef();
            if(!f) return nullptr;
            FieldDefListNode* nw_fdl = new FieldDefListNode();
            nw_fdl->field_def_ = f;
            if(cur_pos_ < cur_size_ && tokens_[cur_pos_].val_ == ","){
                cur_pos_++;
                nw_fdl->next_ = fieldDefList();
            }
            return nw_fdl;
        }

        FieldListNode* fieldList(){
            ASTNode* f = field();
            if(!f) return nullptr;
            FieldListNode* nw_fl = new FieldListNode();
            nw_fl->field_ = f;
            if(cur_pos_ < cur_size_ && tokens_[cur_pos_].val_ == ","){
                nw_fl->token_ = tokens_[cur_pos_++];
                nw_fl->next_ = fieldList();
            }
            return nw_fl;
        }

        TableListNode* tableList(){
            ASTNode* table = this->table();
            if(!table || !catalog_->isValidTable(table->token_.val_)) {
                delete table;
                return nullptr;
            }
            TableListNode* nw_tl = new TableListNode();
            nw_tl->token_ = table->token_;
            if(cur_pos_ < cur_size_ && tokens_[cur_pos_].val_ == ","){
                nw_tl->token_ = tokens_[cur_pos_++];
                nw_tl->next_ = tableList();
            }
            return nw_tl;
        }

        ConstListNode* constList(){
            ASTNode* c = this->constant();
            if(!c) return nullptr;
            ConstListNode* nw_cl = new ConstListNode();
            nw_cl->token_ = c->token_;
            nw_cl->category_ = c->category_;
            if(cur_pos_ < cur_size_ && tokens_[cur_pos_].val_ == ","){
                nw_cl->token_ = tokens_[cur_pos_++];
                nw_cl->next_ = constList();
            }
            return nw_cl;
        }
        // predicate, selectlist, tablelist, constlist and fieldlist 
        // can be improved by using just one list node struct for example. (later)


        
        SelectStatementNode* selectStatement(){
            // if there is no filter predicate it is just going to be null.
            SelectStatementNode* statement = new SelectStatementNode();

            statement->fields_ = fieldList();
            if(!statement->fields_ || cur_pos_ >= cur_size_ || tokens_[cur_pos_].val_ != "FROM"){
                statement->clean();
                return nullptr;
            }

            cur_pos_++;
            statement->tables_ = tableList();
            if(!statement->tables_){
                statement->clean();
                return nullptr;
            }
            if(cur_pos_ < cur_size_ && tokens_[cur_pos_].val_ == "WHERE"){
                cur_pos_++;
                statement->predicate_ = predicate();
            }
            return statement; 
        }

        CreateTableStatementNode* createTableStatement(){
            if(cur_pos_ >= cur_size_ || tokens_[cur_pos_].val_ != "TABLE")
                return nullptr;
            cur_pos_++;
            auto statement = new CreateTableStatementNode();

            statement->table_ = table();

            if(!statement->table_   || cur_pos_ >= cur_size_ 
                    || tokens_[cur_pos_++].val_ != "(" 
                    || catalog_->isValidTable(statement->table_->token_.val_)){
                statement->clean();
                return nullptr;
            }

            statement->field_defs_ = fieldDefList();


            if(!statement->field_defs_ || cur_pos_ >= cur_size_ 
                       || tokens_[cur_pos_++].val_ != ")"){
                statement->clean();
                return nullptr;
            }
            cur_pos_++;

            return statement;
        }



        InsertStatementNode* insertStatement(){
            if(cur_pos_ >= cur_size_ || tokens_[cur_pos_].val_ != "INTO")
                return nullptr;
            cur_pos_++;

            InsertStatementNode* statement = new InsertStatementNode();

            statement->table_ = table();
            if(!statement->table_ || cur_pos_ >= cur_size_ 
                    || tokens_[cur_pos_++].val_ != "(" 
                    || !catalog_->isValidTable(statement->table_->token_.val_)){
                statement->clean();
                return nullptr;
            }

            
            statement->fields_ = fieldList();
            if(!statement->fields_ || cur_pos_ >= cur_size_ 
                       || tokens_[cur_pos_].val_ != ")"){
                statement->clean();
                return nullptr;
            }
            cur_pos_++;

            if(cur_pos_+1 < cur_size_ 
                    && tokens_[cur_pos_].val_ == "VALUES" 
                    && tokens_[cur_pos_+1].val_ == "("){
                cur_pos_+=2;
                statement->values_ = constList();
                if(!statement->values_ || cur_pos_ >= cur_size_ || tokens_[cur_pos_].val_ != ")"){
                    statement->clean();
                    return nullptr;
                }
                cur_pos_++;
            }
            return statement; 
        }


        DeleteStatementNode* deleteStatement(){
            if(cur_pos_ >= cur_size_ || tokens_[cur_pos_].val_ != "FROM")
                return nullptr;
            cur_pos_++;

            // if there is no filter predicate it is just going to be null.
            DeleteStatementNode* statement = new DeleteStatementNode();

            statement->table_ = table(); 
            if(!statement->table_)
                return nullptr;
            if(cur_pos_ < cur_size_ && tokens_[cur_pos_].val_ == "WHERE"){
                cur_pos_++;
                statement->predicate_ = predicate();
            }
            return statement; 
        }

        UpdateStatementNode* updateStatement(){
            // if there is no filter predicate it is just going to be null.
            UpdateStatementNode* statement = new UpdateStatementNode();

            statement->table_ = this->table(); 
            if(!statement->table_) return nullptr;

            if(cur_pos_ >= cur_size_ || tokens_[cur_pos_].val_ != "SET"){
                statement->clean();
                return nullptr;
            }
            cur_pos_++;

            statement->field_ = field();
            if(!statement->field_) {
                statement->clean();
                return nullptr;
            }

            if(cur_pos_ >= cur_size_ || tokens_[cur_pos_].val_ != "="){
                statement->clean();
                return nullptr;
            }
            cur_pos_++;

            statement->expression_ = expression();
            if(!statement->expression_) {
                statement->clean();
                return nullptr;
            }


            if(cur_pos_ < cur_size_ && tokens_[cur_pos_].val_ == "WHERE"){
                cur_pos_++;
                statement->predicate_ = predicate();
            }
            return statement; 
        }


        ASTNode* parse(std::string& query){
            tokens_ = tokenizer_.tokenize(query);
            cur_size_ = tokens_.size();
            if(cur_size_ == 0 || tokens_[0].type_ != KEYWORD) return nullptr;
            std::string v = tokens_[0].val_;
            cur_pos_ = 1;
            if(v == "SELECT")
                return selectStatement();
            else if(v == "INSERT")
                return insertStatement();
            else if(v == "DELETE")
                return deleteStatement();
            else if(v == "UPDATE")
                return updateStatement();
            // only creating tables is supported for now.
            else if(v == "CREATE")
                return createTableStatement();

            // current statement is not supported yet.
            return nullptr;
        }
    private:
        Tokenizer tokenizer_ {};
        std::vector<Token> tokens_;
        Catalog* catalog_;
        uint32_t cur_pos_;
        uint32_t cur_size_;
};


