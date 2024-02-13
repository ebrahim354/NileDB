#include <iostream>
#include <string>
#include <unordered_map>
#include <vector>
#include <sstream>
#include <fstream>
#include "utils.cpp"



struct Table {
    std::string table_name;
    size_t num_of_columns = 0;
};


class DiskManager {
    public:

        DiskManager(){}
        ~DiskManager(){}

        // returns 1 in case of failure and 0 in case of success.
        int readFile(std::string file_name, std::vector<std::vector<std::string>> &ouput_buffer){
            std::ifstream input_selected_table(file_name);

            std::string row_buffer;
            while(std::getline(input_selected_table, row_buffer)){
                std::vector<std::string> columns = strSplit(row_buffer, ',');
                ouput_buffer.emplace_back(columns);
            }
            if(input_selected_table.bad())
                return 1;
            input_selected_table.close();
            return 0;
        }

        int writeFile(std::string file_name, std::vector<std::vector<std::string>> &input_buffer){
            std::ofstream output_selected_table(file_name);
            for(auto &row : input_buffer){
                std::string line = "";
                for(auto &col : row){
                    line += col+",";        
                }
                output_selected_table << line << "\n";
            }

            if(output_selected_table.bad())
                return 1;
            output_selected_table.flush(); 
            output_selected_table.close();
            return 0;
        }
};




class CacheManager {
    public:
    CacheManager(DiskManager *disk_manager):  disk_manager_(disk_manager){}
    ~CacheManager(){}

    std::vector<std::vector<std::string>>* fetchFile(std::string file_name){
        if(files_.count(file_name)) return &files_[file_name];

        files_[file_name] = std::vector<std::vector<std::string>>();
        int err = disk_manager_->readFile(file_name, files_[file_name]);
        if(err) return nullptr;

        return &files_[file_name];
    }

    void flushFile(std::string file_name){
        if(!files_.count(file_name))   return;

        int err = disk_manager_->writeFile(file_name, files_[file_name]);
        if(err) return;

        files_.erase(file_name);
    }


    private:
    DiskManager *disk_manager_;
    std::unordered_map<std::string, std::vector<std::vector<std::string>>> files_;
};

// each individual table is present in one line
// each line contains <table name>,<number of columns>
// using the CacheManager to get the main_tables.txt file as a matrix of values.
// this is better than handling disk directly.

class DirectoryManager {
    public:
        DirectoryManager(CacheManager *cache_manager): cache_manager_(cache_manager){
            //cache_manager_ = cache_manager;
            std::vector<std::vector<std::string>> *main_tables = cache_manager_->fetchFile("main_tables.txt");
            for(auto &table : *main_tables){
                std::string table_name = table[0];
                size_t n = std::stoi(table[1]);
                table_directory[table_name] = {
                    table_name,
                    n
                };
            }
        }
        ~DirectoryManager(){}

        bool tableExists(std::string table_name){
            return table_directory.count(table_name);
        }
        // the user should check for table exists first or gets an empty table object.
        Table getTable(std::string table_name){
            if(!tableExists(table_name)) return {};
           
            return table_directory[table_name];
        }
        bool createTable(std::string table_name, std::string table_size){
            // append the new file into the main_tables file.
            std::vector<std::vector<std::string>> *io_buffer = cache_manager_->fetchFile("main_tables.txt");

            if(!io_buffer) return false;

            std::vector<std::string> tmp;
            tmp.push_back(table_name);
            tmp.push_back(table_size);
            io_buffer->push_back(tmp);
            cache_manager_->flushFile("main_tables.txt");
            table_directory[table_name] = {
                table_name,
                (size_t)std::stoi(table_size)
            };
            return true;
        }
        

    private:
        CacheManager *cache_manager_;
        std::unordered_map<std::string, Table> table_directory;
};


class QueryProcessor{
    public:
        QueryProcessor(CacheManager *cache_manager, DirectoryManager *directory_manager): 
            cache_manager_(cache_manager),
            directory_manager_(directory_manager){}
        ~QueryProcessor(){}

        // executor functions for certain queries.
        //
        /*
         * CREATE TABLE <table name> <number of columns>.
         * SELECT FROM <table name> returns the entire table.
         * SELECT FROM <table name> WHERE <column number> = "STRING LITERAL".
         * DELETE FROM <table name> deletes the entire table.
         * INSERT INTO <table name> <n values> and n = number of columns.
         */
         
        bool createTableExec(std::string table_name, std::string table_size){
            if(!areDigits(table_size) || directory_manager_->tableExists(table_name) || table_name == "main_tables") 
                return false;
            bool created_successfully =  directory_manager_->createTable(table_name, table_size);
            return created_successfully; 
        }

        bool selectExec(std::string table_name){
            std::string file_name = table_name+".txt";

            std::vector<std::vector<std::string>> *table_buffer = cache_manager_->fetchFile(file_name);
            if(!table_buffer) return false;
            for(auto &row : *table_buffer){
                for(auto &col : row)
                    std::cout << col << " " ;
                std::cout << "\n";
            }
            return true;
        } 

        bool selectFilterExec(std::string table_name, size_t filter_idx, std::string filter_str){
            std::string file_name = table_name+".txt";
            
            std::vector<std::vector<std::string>> *table_buffer = cache_manager_->fetchFile(file_name);
            if(!table_buffer) return false;

            for(auto &row : *table_buffer){
                if(row[filter_idx-1] == filter_str){
                    for(size_t i = 0; i < row.size(); i++)
                        std::cout << row[i] << " " ;
                    std::cout << std::endl;
                }
            }
            return true;
        }

        bool deleteExec(std::string table_name){
            std::string file_name = table_name+".txt";
            std::vector<std::vector<std::string>> *table_buffer = cache_manager_->fetchFile(file_name);
            if(!table_buffer) return false;
            table_buffer->clear();
            cache_manager_->flushFile(file_name);
            return true;
        }

        // INSERT INTO <table name> <n values> and n = number of columns.
        bool insertExec(std::string table_name, std::vector<std::string> &vals){
            std::string file_name = table_name+".txt";
            std::size_t n = directory_manager_->getTable(table_name).num_of_columns;

            std::vector<std::vector<std::string>> *io_buffer = cache_manager_->fetchFile(file_name);
            if(!io_buffer) return false;

            io_buffer->push_back(vals);
            cache_manager_->flushFile(file_name);
            return true;
        }


        std::vector<std::string> tokenize(std::string query){
            std::vector<std::string> tokens;
            std::stringstream ss(query);
            std::string cur_token;
            while(ss >> cur_token){
                tokens.push_back(cur_token); 
            }
            return tokens;
        }

        void handleQuery(std::string query){
            std::vector<std::string> tokens = tokenize(query);
            bool valid_query = false;
            std::string query_type = tokens[0];
            if(query_type == "CREATE" && tokens.size() == 4){
                //  CREATE TABLE <table name> <number of columns>.
                std::string table_name = tokens[2];
                std::string n = tokens[3];
                valid_query = createTableExec(table_name, n);
                if(valid_query)
                    std::cout << "CREATED TABLE : " << table_name << "\n";
            } else if(query_type == "SELECT" && tokens.size() == 3 && directory_manager_->tableExists(tokens[2])){
                // SELECT FROM <!table name> returns the entire table.
                std::string table_name = tokens[2];
                valid_query = selectExec(table_name);

            } else if(query_type == "SELECT" 
                    && tokens.size() == 7 && directory_manager_->tableExists(tokens[2])
                    && areDigits(tokens[4]) 
                    && (size_t) std::stoi(tokens[4]) <= directory_manager_->getTable(tokens[2]).num_of_columns
                    && tokens[5] == "="){
                // SELECT FROM <table name> WHERE <column number> = "STRING LITERAL".
                std::string table_name = tokens[2];
                int filter_column_num = std::stoi(tokens[4]);
                std::string filter_str = tokens[6];
                valid_query = selectFilterExec(table_name, filter_column_num, filter_str);
            } else if(query_type == "DELETE" && tokens.size() == 3 && directory_manager_->tableExists(tokens[2])){
                // DELETE FROM <table name> deletes the entire table.
                std::string table_name = tokens[2];
                valid_query = deleteExec(table_name);

                if(valid_query)
                    std::cout << "CLEARED TABLE" << std::endl;

            } else if(query_type == "INSERT" && tokens.size() >= 3 
                    && directory_manager_->tableExists(tokens[2]) 
                    && directory_manager_->getTable(tokens[2]).num_of_columns == tokens.size() - 3
                    ){
                // INSERT INTO <table name> <n values> and n = number of columns.
                std::string table_name = tokens[2];
                std::string file_name = table_name+".txt";
                std::size_t n = directory_manager_->getTable(table_name).num_of_columns;
                std::vector<std::string> vals;
                for(size_t i = 0; i < n; i++)  vals.push_back(tokens[i+3]);

                valid_query = insertExec(table_name, vals);
                if(valid_query)
                    std::cout << "INSERTED A NEW RECORD" << std::endl;
            }

            if(!valid_query){
                std::cout << "INVALID QUERY" << "\n";
            }

        }

    private:
        CacheManager *cache_manager_;
        DirectoryManager *directory_manager_;
};




int main() {
    DiskManager disk_manager_{};
    CacheManager cache_manager_(&disk_manager_);
    DirectoryManager directory_manager_(&cache_manager_);
    QueryProcessor query_processor_(&cache_manager_,&directory_manager_);
    // setting up meta data.
    bool prompt_is_running = true;
    while(prompt_is_running){
        std::cout << "> ";
        std::string query;
        std::getline(std::cin, query);
        query_processor_.handleQuery(query);
    }
    return 0;
}



/* 
 * INITIAL PROGRAM
 * supported queries:
 * CREATE TABLE <table name> <number of columns>.
 * SELECT FROM <table name> returns the entire table.
 * SELECT FROM <table name> WHERE <column number> = "STRING LITERAL".
 * DELETE FROM <table name> deletes the entire table.
 * INSERT INTO <table name> <n values> and n = number of columns.
 */

