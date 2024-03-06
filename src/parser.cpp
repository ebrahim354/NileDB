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
    CONSTANT,
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
    ASTNode* left_;
    ASTNode* right_;
};

struct FieldDefNode : ASTNode {
    FieldDefNode(): ASTNode(FIELD_DEF)
    {}
    ASTNode* field_;
    ASTNode* type_;
};




// this amound of specific list types is why inheritance is just bad. or maybe I'm doing it the wrong way.


// a linked list of constants linked by the ',' sympol.
struct FieldDefListNode : ASTNode {
    FieldDefListNode(): ASTNode(FIELD_DEF_LIST)
    {}
    FieldDefNode* field_def_ = nullptr;
    FieldDefListNode* next_ = nullptr;
};

// a linked list of constants linked by the ',' sympol.
struct ConstListNode : ASTNode {
    ConstListNode(): ASTNode(CONST_LIST)
    {}
    ConstListNode* next_ = nullptr;
};

// a linked list of table identifiers linked with the ',' sympol.
struct TableListNode : ASTNode {
    TableListNode(): ASTNode(TABLE_LIST)
    {}
    TableListNode* next_ = nullptr;
};

// a linked list of fields linked with the ',' sympol.
struct FieldListNode : ASTNode {
    FieldListNode(): ASTNode(SELECT_LIST)
    {}
    ASTNode* field_ = nullptr;
    FieldListNode* next_ = nullptr;
};

// a linked list of terms linked with keywords (and, or etc...).
struct PredicateNode : ASTNode {
    PredicateNode(Token val = {}): ASTNode(PREDICATE, val)
    {}
    TermNode* term_ = nullptr;
    PredicateNode* next_ = nullptr;
};


// SQL statements.
struct SelectStatementNode : ASTNode {

    SelectStatementNode(): ASTNode(SELECT_STATEMENT)
    {}
    FieldListNode* fields_;
    TableListNode*  tables_;
    PredicateNode* predicate_;
};

struct CreateTableStatementNode : ASTNode {

    CreateTableStatementNode(): ASTNode(CREATE_TABLE_STATEMENT)
    {}
    FieldDefListNode* field_defs_;
    ASTNode* table_;
};

struct DeleteStatementNode : ASTNode {

    DeleteStatementNode(): ASTNode(DELETE_STATEMENT)
    {}
    ASTNode*  table_;
    PredicateNode* predicate_;
};

struct UpdateStatementNode : ASTNode {

    UpdateStatementNode(): ASTNode(UPDATE_STATEMENT)
    {}
    ASTNode*  table_;
    ASTNode*  field_;
    PredicateNode* predicate_;
};

struct InsertStatementNode : ASTNode {

    InsertStatementNode(): ASTNode(INSERT_STATEMENT)
    {}
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
                if(f) delete f;
                if(t) delete t;
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
            if(cur_pos_ < cur_size_ 
                    && (tokens_[cur_pos_].type_ == STR_CONSTANT || tokens_[cur_pos_].type_ == INT_CONSTANT)) {
                return new ASTNode(CONSTANT, tokens_[cur_pos_++]);
            }
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
            if(!table) return nullptr;
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
            if(cur_pos_ < cur_size_ && tokens_[cur_pos_].val_ == ","){
                nw_cl->token_ = tokens_[cur_pos_++];
                nw_cl->next_ = constList();
            }
            return nw_cl;
        }
        // predicate, selectlist, tablelist, constlist and fieldlist 
        // can be improved by using just one list node struct for example. (later)


        
        SelectStatementNode* selectStatement(){
            FieldListNode* fields = fieldList();
            if(!fields || cur_pos_ >= cur_size_ || tokens_[cur_pos_].val_ != "FROM")
                return nullptr;

            cur_pos_++;
            TableListNode* tables = tableList();
            PredicateNode* pred = nullptr;
            if(!tables)
                return nullptr;
            if(cur_pos_ < cur_size_ && tokens_[cur_pos_].val_ == "WHERE"){
                cur_pos_++;
                pred = predicate();
            }
            // if there is no filter predicate it is just going to be null.
            SelectStatementNode* statement = new SelectStatementNode();
            statement->fields_ = fields;
            statement->tables_ = tables;
            statement->predicate_ = pred;
            return statement; 
        }

        CreateTableStatementNode* createTableStatement(){
            std::cout << "CREATE STATEMENT" << std::endl;
            if(cur_pos_ >= cur_size_ || tokens_[cur_pos_].val_ != "TABLE")
                return nullptr;
            cur_pos_++;

            auto t = this->table();
            if( t == nullptr) std::cout << " INVALID TABLE " << std::endl;

            if(!t   || cur_pos_ >= cur_size_ 
                    || tokens_[cur_pos_++].val_ != "(" 
                    || catalog_->isValidTable(t->token_.val_))
                return nullptr;
            std::cout << " is valid table " << std::endl;

            FieldDefListNode* field_defs = fieldDefList();

            if( field_defs == nullptr) std::cout << " INVALID fields " << std::endl;

            if(!field_defs || cur_pos_ >= cur_size_ 
                       || tokens_[cur_pos_++].val_ != ")")
                return nullptr;
            cur_pos_++;
            std::cout << "valid statement" << std::endl;

            auto statement = new CreateTableStatementNode();
            statement->table_ = t;
            statement->field_defs_ = field_defs;
            return statement;
        }



        InsertStatementNode* insertStatement(){
            if(cur_pos_ >= cur_size_ || tokens_[cur_pos_].val_ != "into")
                return nullptr;
            cur_pos_++;

            ASTNode* t = table();
            if(!t || cur_pos_ >= cur_size_ || tokens_[cur_pos_].val_ != "(")
                return nullptr;

            FieldListNode* fields = fieldList();
            if(!fields || cur_pos_ >= cur_size_ 
                       || tokens_[cur_pos_].val_ != ")")
                return nullptr;
            cur_pos_++;

            ConstListNode* values = nullptr;
            if(cur_pos_+1 < cur_size_ 
                    && tokens_[cur_pos_].val_ == "values" 
                    && tokens_[cur_pos_+1].val_ == "("){
                cur_pos_+=2;
                values = constList();
                if(!values || cur_pos_ >= cur_size_ || tokens_[cur_pos_].val_ != ")")
                    return nullptr;
                cur_pos_++;
            }
            InsertStatementNode* statement = new InsertStatementNode();
            statement->fields_ = fields;
            statement->values_ = values;
            return statement; 
        }


        DeleteStatementNode* deleteStatement(){
            if(cur_pos_ >= cur_size_ || tokens_[cur_pos_].val_ != "from")
                return nullptr;
            cur_pos_++;

            ASTNode* table = this->table(); 
            if(!table)
                return nullptr;
            PredicateNode* pred = nullptr;
            if(cur_pos_ < cur_size_ && tokens_[cur_pos_].val_ == "where"){
                cur_pos_++;
                pred = predicate();
            }
            // if there is no filter predicate it is just going to be null.
            DeleteStatementNode* statement = new DeleteStatementNode();
            statement->table_ = table;
            statement->predicate_ = pred;
            return statement; 
        }

        UpdateStatementNode* updateStatement(){
            ASTNode* t = this->table(); 
            if(!t) return nullptr;

            if(cur_pos_ >= cur_size_ || tokens_[cur_pos_].val_ != "set")
                return nullptr;
            cur_pos_++;

            ASTNode* f = this->field();
            if(!f) return nullptr;

            if(cur_pos_ >= cur_size_ || tokens_[cur_pos_].val_ != "=")
                return nullptr;
            cur_pos_++;

            ASTNode* ex = expression();
            if(!ex) return nullptr;


            PredicateNode* pred = nullptr;
            if(cur_pos_ < cur_size_ && tokens_[cur_pos_].val_ == "where"){
                cur_pos_++;
                pred = predicate();
            }

            // if there is no filter predicate it is just going to be null.
            UpdateStatementNode* statement = new UpdateStatementNode();
            statement->table_ = t;
            statement->field_ = f;
            statement->predicate_ = pred;
            return statement; 
        }


        ASTNode* parse(std::string& query){
            tokens_ = tokenizer_.tokenize(query);
            std::cout << "tokenizer returned" << std::endl;
            for(size_t i = 0; i < tokens_.size(); ++i){
                std::cout << tokens_[i].val_ << " " << tokens_[i].type_ << std::endl;
            }
            cur_size_ = tokens_.size();
            if(cur_size_ == 0 || tokens_[0].type_ != KEYWORD) return nullptr;
            std::string v = tokens_[0].val_;
            std::cout << v << std::endl;
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


