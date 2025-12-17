release:
	clang++  src/main.cpp -Isrc/includes -pthread -std=c++20 -o bin/NDB -g
gcc:
	g++ -Wall src/main.cpp -std=c++2a -Isrc/includes -pthread -o bin/NDB -g 
dev:
	g++ src/main.cpp -Wall -Wextra -Werror -std=c++2a -pthread -o bin/NDB -g
create_table_test:
	g++ tests/create_table_test.cpp -std=c++2a -pthread -o bin/create_table_test -g && rm *.ndb
sql_logic_test:
	g++ tests/sqllogictest.cpp -std=c++2a -pthread -o bin/logictest -g && rm *.ndb
