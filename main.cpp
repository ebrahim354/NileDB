#include <iostream>
#include <string>
#include <unordered_map>
#include <vector>
#include <sstream>
#include <fstream>



struct Table {
    std::string table_name;
    size_t num_of_columns = 0;
};

std::vector<std::string> strSplit(const std::string &str, char delimiter) {
    std::stringstream ss(str);
    std::vector<std::string> pieces;
    std::string tmp;
    while (std::getline(ss, tmp, delimiter)) {
        pieces.push_back(tmp);

    }
    return (pieces);
}

bool areDigits(std::string nums){
    bool are_digits = true;
    for(size_t i = 0; i < nums.size(); i++){
        if(nums[i] - '0'  > 9 || nums[i] - '0' < 0) {
            are_digits = false;
        }
    }
    return (are_digits && nums.size() > 0);
}


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




int main() {
    DiskManager disk_manager_{};
    CacheManager cache_manager_(&disk_manager_);
    DirectoryManager directory_manager_(&cache_manager_);
    // setting up meta data.
    bool prompt_is_running = true;
    while(prompt_is_running){
        std::cout << "> ";
        bool valid_query = false;
        std::vector<std::string> tokens;
        std::string line;
        std::getline(std::cin, line);
        // stringstream to filter white space between tokens.
        std::stringstream ss(line);
        std::string token;
        while(ss >> token){
            tokens.push_back(token); 
        }
        // handle query then start over.
        std::string query_type = tokens[0];
        if(query_type == "CREATE" && tokens.size() == 4){
            //  CREATE TABLE <table name> <number of columns>.
            std::string table_name = tokens[2];
            std::string n = tokens[3];

            if(areDigits(n) && !directory_manager_.tableExists(table_name) && table_name != "main_tables"){

                bool created = directory_manager_.createTable(table_name, n);
                if(created){
                    std::cout << "CREATED TABLE : " << table_name << "\n";
                    valid_query = true;
                }
            }


        } else if(query_type == "SELECT" && tokens.size() == 3 && directory_manager_.tableExists(tokens[2])){
            // SELECT FROM <!table name> returns the entire table.
            std::string table_name = tokens[2];
            std::string file_name = table_name+".txt";

            std::vector<std::vector<std::string>> *table_buffer = cache_manager_.fetchFile(file_name);
            if(table_buffer != nullptr){
                for(auto &row : *table_buffer){
                    for(auto &col : row)
                        std::cout << col << " " ;
                    std::cout << "\n";
                }
                valid_query = true;
            }
        } else if(query_type == "SELECT" 
                && tokens.size() == 7 && directory_manager_.tableExists(tokens[2])
                && areDigits(tokens[4]) 
                && (size_t) std::stoi(tokens[4]) <= directory_manager_.getTable(tokens[2]).num_of_columns
                && tokens[5] == "="){
            // SELECT FROM <table name> WHERE <column number> = "STRING LITERAL".
            std::string table_name = tokens[2];
            std::string file_name = table_name+".txt";
            int filter_column_num = std::stoi(tokens[4]);
            std::string filter_str = tokens[6];

            std::vector<std::vector<std::string>> *table_buffer = cache_manager_.fetchFile(file_name);

            if(table_buffer != nullptr){
                for(auto &row : *table_buffer){
                    if(row[filter_column_num-1] == filter_str){
                        for(size_t i = 0; i < row.size(); i++)
                            std::cout << row[i] << " " ;
                        std::cout << std::endl;
                    }
                }
                valid_query = true;
            }
        } else if(query_type == "DELETE" && tokens.size() == 3 && directory_manager_.tableExists(tokens[2])){
            // DELETE FROM <table name> deletes the entire table.
            std::string table_name = tokens[2];
            std::string file_name = table_name+".txt";

            std::vector<std::vector<std::string>> *table_buffer = cache_manager_.fetchFile(file_name);
            if(table_buffer != nullptr){
                table_buffer->clear();
                cache_manager_.flushFile(file_name);
                std::cout << "CLEARED TABLE" << std::endl;
                valid_query = true;
            }

        } else if(query_type == "INSERT" && tokens.size() >= 3 
                && directory_manager_.tableExists(tokens[2]) 
                && directory_manager_.getTable(tokens[2]).num_of_columns == tokens.size() - 3
                ){
            // INSERT INTO <table name> <n values> and n = number of columns.
            std::string table_name = tokens[2];
            std::string file_name = table_name+".txt";
            std::size_t n = directory_manager_.getTable(table_name).num_of_columns;

            std::vector<std::vector<std::string>> *io_buffer = cache_manager_.fetchFile(file_name);
            if(io_buffer != nullptr){
                std::vector<std::string> row;
                for(size_t i = 0; i < n; i++)  row.push_back(tokens[i+3]);
                io_buffer->push_back(row);
                cache_manager_.flushFile(file_name);
                std::cout << "INSERTED A NEW RECORD" << std::endl;
                valid_query = true;
            }
        }
        
        if(!valid_query){
            std::cout << "INVALID QUERY" << "\n";
        }
            
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

