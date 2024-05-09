dev:
	g++ src/main.cpp -Wall -std=c++2a -pthread -o bin/NDB -g
release:
	g++ src/main.cpp -std=c++2a -pthread -o bin/NDB
parse_test:
	g++ tests/parser.cpp -std=c++2a -pthread -o bin/PARSE_TEST -g
