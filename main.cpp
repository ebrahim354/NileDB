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





int main() {
    DiskManager DM;
    // setting up meta data.
    bool prompt_is_running = true;
    std::ifstream input_main_tables("main_tables.txt");
    std::unordered_map<std::string, Table> table_directory;
    std::string row;
    while(std::getline(input_main_tables, row)){
        // each individual table is present in one line
        // each line contains <table name> <number of columns>
        std::stringstream ss(row);
        std::string table_name; 
        size_t n;

        ss >> table_name;
        ss >> n;
        
        table_directory[table_name] = {
            table_name,
            n
        };
    }
    input_main_tables.close();
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
            if(areDigits(n) && !table_directory.count(table_name) && table_name != "main_tables"){
                std::ofstream output_main_tables("main_tables.txt", std::ios::app | std::ios::ate);
                output_main_tables << table_name << " " << n << std::endl;
                table_directory[table_name] = {
                    table_name,
                    (size_t)std::stoi(n)
                };
                std::cout << "CREATED TABLE : " << table_name << "\n";
                valid_query = true;
            }
        } else if(query_type == "SELECT" && tokens.size() == 3 && table_directory.count(tokens[2])){
            // SELECT FROM <table name> returns the entire table.
            std::string table_name = tokens[2];
            std::string file_name = table_name+".txt";

            std::vector<std::vector<std::string>> table_buffer;
            int err = DM.readFile(file_name, table_buffer);
            if(!err){
                for(auto &row : table_buffer){
                    for(auto &col : row)
                        std::cout << col << " " ;
                    std::cout << "\n";
                }
            }
            valid_query = true;
        } else if(query_type == "SELECT" 
                && tokens.size() == 7 && table_directory.count(tokens[2])
                && areDigits(tokens[4]) 
                && (size_t) std::stoi(tokens[4]) <= table_directory[tokens[2]].num_of_columns
                && tokens[5] == "="){
            // SELECT FROM <table name> WHERE <column number> = "STRING LITERAL".
            std::string table_name = tokens[2];
            std::string file_name = table_name+".txt";
            int filter_column_num = std::stoi(tokens[4]);
            std::string filter_str = tokens[6];
            std::vector<std::vector<std::string>> table_buffer;

            int err = DM.readFile(file_name, table_buffer);
            if(!err){
                for(auto &row : table_buffer){
                    if(row[filter_column_num-1] == filter_str){
                        for(size_t i = 0; i < row.size(); i++)
                            std::cout << row[i] << " " ;
                        std::cout << std::endl;
                    }
                }
            }
            valid_query = true;
        } else if(query_type == "DELETE" && tokens.size() == 3 && table_directory.count(tokens[2])){
            // DELETE FROM <table name> deletes the entire table.
            std::string table_name = tokens[2];
            std::string file_name = table_name+".txt";
            std::vector<std::vector<std::string>> tmp_buffer;
            int err = DM.writeFile(file_name, tmp_buffer);
            if(!err){
                std::cout << "CLEARED TABLE" << std::endl;
                valid_query = true;
            }
        } else if(query_type == "INSERT" && tokens.size() >= 3 
                && table_directory.count(tokens[2]) && table_directory[tokens[2]].num_of_columns == tokens.size() - 3
                ){
            // INSERT INTO <table name> <n values> and n = number of columns.
            std::string table_name = tokens[2];
            std::string file_name = table_name+".txt";
            std::size_t n = table_directory[table_name].num_of_columns;
            std::vector<std::vector<std::string>> io_buffer;
            std::string row = ""; 

            int r_err = DM.readFile(file_name, io_buffer);
            if(!r_err){
                std::vector<std::string> row;
                for(size_t i = 0; i < n; i++)  row.push_back(tokens[i+3]);
                io_buffer.push_back(row);
                int w_err = DM.writeFile(file_name, io_buffer);
                if(!w_err){
                    std::cout << "INSERTED A NEW RECORD" << std::endl;
                    valid_query = true;
                }
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

