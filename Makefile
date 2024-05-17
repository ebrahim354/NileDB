dev:
	g++ src/main.cpp -Wall -std=c++2a -pthread -o bin/NDB -g
release:
	g++ src/main.cpp -std=c++2a -pthread -o bin/NDB
create_table_test:
	g++ tests/create_table_test.cpp -std=c++2a -pthread -o bin/create_table_test -g && rm *.ndb
