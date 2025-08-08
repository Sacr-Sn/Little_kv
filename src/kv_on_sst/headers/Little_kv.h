#pragma once

#include <iostream>
#include <unordered_map>
#include <string>
#include <fstream>
#include <memory>
#include <cstdio>

#include "./MemTable.h"

using namespace std;


// 用一个特殊字符串表示删除标记（Tombstone）
// const string TOMBSTONE = "<TOMBSTONE>";

class Little_kv {
private:
    shared_ptr<MemTable> active_memtable;
    shared_ptr<MemTable> immutable_memtable;

    string wal_file_path;  // log_file path
    string sst_file_path;  
    size_t memtable_size_threshold; 
    int sst_file_id;

    vector<string> ssts_names;  // sst files name


    // 初始化，重放WAL恢复
    void Init();

    void readin_ssts_names();

    // 写WAL日志
    void WriteWAL(const string& op, const string& key, const string& value);

    // 计算当前 active_memtable 大小（简单计数）
    size_t GetMemTableSize();

    // get sst files name
    string GenerateSSTFilename(int sst_id);

    // 刷盘操作
    void Flush();

    void ClearWAL();

    // compact logic
    bool Compact_sst_files(const vector<string>& input_ssts);

public:
    Little_kv(const std::string& wal_path, const std::string& sst_path, size_t memtable_threshold, const int& sst_id);

    ~Little_kv();

    // put操作（包含普通写入和删除）
    bool put(const std::string& key, const std::string& value);

    // del操作，实际上写入Tombstone
    // 没有真正“删除”旧数据的操作，所有删除操作本质上是put一个特殊的“删除标记”（Tombstone）值。
    bool del(const std::string& key);

    // get操作
    string get(const string& key);

    // manual compcact
    bool manual_compact(int compact_file_count);

};