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


int main() {
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
            std::ifstream input_selected_table(table_name + ".txt");
            std::string row;
            while(std::getline(input_selected_table, row)){
                std::vector<std::string> columns = strSplit(row, ',');
                for(size_t i = 0; i < columns.size(); i++)
                    std::cout << columns[i] << " " ;
                std::cout << std::endl;
            }
            input_selected_table.close();
            valid_query = true;
        } else if(query_type == "DELETE" && tokens.size() == 3 && table_directory.count(tokens[2])){
            // DELETE FROM <table name> deletes the entire table.
            std::string table_name = tokens[2];
            std::ofstream output_selected_table(table_name + ".txt", std::ios::trunc);
            output_selected_table.close();
            std::cout << "CLEARED TABLE" << std::endl;
            valid_query = true;
        } else if(query_type == "INSERT" && tokens.size() >= 3 
                && table_directory.count(tokens[2]) && table_directory[tokens[2]].num_of_columns == tokens.size() - 3
                ){
            // INSERT INTO <table name> <n values> and n = number of columns.
            std::string table_name = tokens[2];
            std::size_t n = table_directory[table_name].num_of_columns;
            std::string row = ""; 
            for(size_t i = 0; i < n; i++)  row += tokens[i+3] + ',';
            std::ofstream output_selected_table(table_name + ".txt", std::ios::app | std::ios::ate);
            output_selected_table << row << std::endl;
            output_selected_table.close();
            std::cout << "INSERTED A NEW RECORD" << std::endl;
            valid_query = true;
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
 * DELETE FROM <table name> deletes the entire table.
 * INSERT INTO <table name> <n values> and n = number of columns.
 */

