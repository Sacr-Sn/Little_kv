#pragma once

#include<iostream>
#include<unordered_map>
#include <fstream>
#include <string>
#include <sstream>
#include <filesystem>

using namespace std;

class Little_kv{
private:
    unordered_map<string, string> kv;

    fstream file;

    string log_path;

    bool recover();  // recover the data(operations) from log.txt when init a obj
    bool deal(const string& op, const string& key, const string& value);  // read and deal every line in log
    bool log(const string& op, const string& key);  // write line to log : 2 args
    bool log(const string& op, const string& key, const string& value);  // write line to log : 3 args
public:
    Little_kv();
    Little_kv(const string& log_path);
    ~Little_kv();

    bool put(const string& key, const string& value);
    string get(const string& key);
    bool del(const string& key);
};

