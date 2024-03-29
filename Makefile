dev:
	g++ src/main.cpp -Wall -std=c++2a -pthread -o NDB -g
release:
	g++ src/main.cpp -std=c++2a -pthread -o NDB
