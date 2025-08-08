#pragma once

#include <iostream>
#include <unordered_map>
#include <string>
#include <fstream>
#include <memory>
#include <cstdio>

#include "./MemTable.h"
#include "./ManifestManager.h"  // manage the manifest.txt file

using namespace std;


// 用一个特殊字符串表示删除标记（Tombstone）
// const string TOMBSTONE = "<TOMBSTONE>";

class Little_kv {
private:
    shared_ptr<MemTable> active_memtable;
    shared_ptr<MemTable> immutable_memtable;

    string wal_file_path;  // log_file path
    string sst_dir_path;  // only dir path , no file_name
    size_t memtable_size_threshold;  // over the value, datas in memtable will be writen to L0
    size_t compact_threshold;  // if L0 files'num supass the value, will compact to L1
    size_t max_file_size;  // a L1 file should be the size


    // vector<string> ssts_names;  // sst files name
    shared_ptr<ManifestManager> manifest;


    // 初始化，重放WAL恢复
    void init_data();

    void readin_ssts_names();

    // 写WAL日志
    void WriteWAL(const string& op, const string& key, const string& value);

    // 计算当前 active_memtable 大小（简单计数）
    size_t GetMemTableSize();


    // 刷盘操作
    void Flush();

    void ClearWAL();


    // get key range of a sst_list
    bool GetKeyRange(const vector<FileMetaData>& files, char& min_k, char& max_k);
    bool GetKeyRange_ignore_del(const unordered_map<string, string> data_map, char& min_k, char& max_k);
    bool GetKeyRange_contains_del(const unordered_map<string, string> data_map, char& min_k, char& max_k);

    // get next sst file id
    int GetNextSstId(const vector<FileMetaData>& files);

    set<char> GetKeySet(const vector<FileMetaData>& files);

    vector<FileMetaData> GetRelativeL1(const vector<FileMetaData>& L1_files, const set<char>& key_set);

    void ReadFileToMap(vector<FileMetaData>& files, unordered_map<string, string>& data_map);

    void MapToSst(const unordered_map<string, string>& current_chunk, const string& filename);

    bool RemoveSstFiles(vector<FileMetaData>& files);

    // compact somt L0 to L1
    bool CompactL0ToL1(vector<FileMetaData>& L0_files, vector<FileMetaData>& L1_files);

public:
    Little_kv(const string& wal_file_path, const string& sst_dir_path, const size_t memtable_threshold, const size_t compact_threshold, const size_t max_size);

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