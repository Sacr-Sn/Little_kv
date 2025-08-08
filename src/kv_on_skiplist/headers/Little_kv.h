#pragma once

#include <iostream>
#include <vector>
#include <cstdlib>
#include <ctime>
#include <limits>
#include <string>
#include <fstream>
#include <string>
#include <sstream>
#include <filesystem>

#include "./SkipList.h"

using namespace std;

class Little_kv {
    private:
    SkipList store_;

    fstream file;

    string log_path;

    bool recover();  // recover the data(operations) from log.txt when init a obj
    bool log(const string& op, const string& key);  // write line to log : 2 args
    bool log(const string& op, const string& key, const string& value);  // write line to log : 3 args

    public:
    Little_kv();
    Little_kv(const string& log_path);

    ~Little_kv();


    // 插入键值
    bool put(string key, string value);

    // 查找键值
    string get(string key);

    // 删除键值
    bool del(string key);

    // 打印跳表结构
    void display();
};

