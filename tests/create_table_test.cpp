#include "../src/NileDB.cpp"


int main() {
    NileDB *ndb = new NileDB();
    std::string input1 = "CREATE TABLE first_table ( col1 INT, col2 VARCHAR )";

    ndb->SQL(input1);
    // table should exist before and after system tear down.

    if(!ndb->isValidTable("first_table")){
        std::cout << "ERROR: Could not find table before teardown" << std::endl;
        return 1;
    }

    TableSchema* sch = ndb->getTableSchema("first_table");
    if(!sch) {
        std::cout << "ERROR: Could not fetch Schema before teardown" << std::endl;
        return 1;
    }
    std::stringstream before_tear_down_schema;
    sch->printSchema(before_tear_down_schema);

    // perform the tear down.
    delete ndb;
    ndb = nullptr;

    // boot up again.

    ndb = new NileDB();

    if(!ndb->isValidTable("first_table")){
        std::cout << "ERROR: Could not find table after teardown" << std::endl;
        return 1;
    }

    sch = ndb->getTableSchema("first_table");
    if(!sch) {
        std::cout << "ERROR: Could not fetch Schema after teardown" << std::endl;
        return 1;
    }
    std::stringstream after_tear_down_schema;
    sch->printSchema(after_tear_down_schema);
    if(after_tear_down_schema.str() != before_tear_down_schema.str()){
        std::cout << "ERROR: The Schema has changed after teardown" << std::endl;
        return 1;
    }
    delete ndb;
    return 0;
}
