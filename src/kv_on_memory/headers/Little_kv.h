#pragma once

#include<iostream>
#include<unordered_map>

using namespace std;

class Little_kv{
private:
    unordered_map<string, string> kv;

public:
    Little_kv();
    ~Little_kv();

    bool put(string key, string value);
    string get(string key);
    bool del(string key);
};

