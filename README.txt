# NileDB

A Database Management System that supports ACID Features, SQL and Indexes.

---

## Why?

To most people DBMSs are just a black box That accepts SQL statements and gives back data.
This way of using DBMSs is fine most of the time but sometimes understanding the internals of anything you use gives
you so much power of how to use it better, This is where this project comes in.
It's designed to not be super complicated to teach people how a database actually looks like from the inside and
why a certain DBMS like MySQL, PostgreSQL or whatever database you use may or may not be the best for your current
application that you are building.
This kind of knowledge is very important for Back-End Developers, Database Admins and even Infrastructure Engineers.

---

## How to Build 

This project is currently being developed using a linux machine, so if you are using linux or mac you can  
clone the project with the following command:
'''bash
git clone https://github.com/ebrahim354/NileDB
'''
After clonning the project run 'make dev' for development mode or 'make release' for release mode.
The produced binary is named NDB.

If you are using windows you can download the repo and start a visual studio c++ project using the src/ folder
and run using the src/main.cpp entry point, This method is not tested and may or may not produce errors.

---

## Usage

After compiling the project type ./NDB in your terminal.  
To show all supported commands type \h in the prompt.

