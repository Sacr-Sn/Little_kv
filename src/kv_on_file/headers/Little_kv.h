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

    string path;

public:
    Little_kv();
    Little_kv(const string& path);
    ~Little_kv();

    bool put(const string& key, const string& value);
    string get(const string& key);
    bool del(const string& key);
};

