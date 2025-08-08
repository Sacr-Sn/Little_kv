#pragma once

#include <iostream>
#include <unordered_map>
#include <string>
#include <fstream>
#include <memory>
#include <cstdio>
#include <cmath>
#include <mutex>        // 提供 std::mutex, std::lock_guard, std::unique_lock
#include <shared_mutex>
#include <thread>
#include <condition_variable>  // std::condition_variable
#include <chrono>              // 如果你要用 sleep_for 或 wait_for

#include "./MemTable.h"
#include "./ManifestManager.h"  // manage the manifest.txt file
#include "./WAL.h"

using namespace std;


// 用一个特殊字符串表示删除标记（Tombstone）
// const string TOMBSTONE = "<TOMBSTONE>";

class Little_kv {
private:
    shared_ptr<MemTable> active_memtable;
    shared_ptr<MemTable> immutable_memtable;

    string sst_dir_path;  // only dir path , no file_name
    size_t memtable_size_threshold;  // over the value, datas in memtable will be writen to L0
    size_t compact_threshold;  // if L0 files'num supass the value, will compact to L1
    size_t max_file_size;  // a L1 file should be the size

    WAL& wal;

    size_t levels_multiple = 4;  // 相邻层次间容量倍数差距（两个用途：初始化bloom filter、合并函数传参）


    // to manage sst files   link to manifest.txt
    shared_ptr<ManifestManager> manifest;


    // mutex
    mutex edit_mutex;  // mutex fot put and del
    shared_mutex rw_manifest_mutex;

    // 线程
    std::thread flush_thread;
    std::thread compact_thread;

    // 同步
    std::mutex flush_mutex;
    std::condition_variable flush_cv;
    bool flush_needed = false;
    bool stop_flush = false;

    std::mutex compact_mutex;
    std::condition_variable compact_cv;
    bool compact_needed = false;
    bool stop_compact = false;


    // 初始化，重放WAL恢复
    void Init_data();

    void readin_ssts_names();

    // 刷盘操作
    void Flush();

    
    /**
     * 从vector中获取key的范围
    */
    bool GetKeyRange(const vector<FileMetaData>& files, char& min_k, char& max_k);
    /**
     * 从map中获取key的范围，忽略value为删除标记的数据
     * 在非目标为最底层的文件合并过程中，删除标记不可忽略，故不能用该函数
     * 适用于目标为最底层的文件的合并过程
    */
    bool GetKeyRange_ignore_del(const unordered_map<string, string> data_map, char& min_k, char& max_k);
    /**
     * 从map中获取key的范围，考虑value为删除标记的数据
     * 适用于非目标为最底层的合并过程
    */
    bool GetKeyRange_contains_del(const unordered_map<string, string> data_map, char& min_k, char& max_k);

    /**
     * 获取下一个sst文件的id
     * files ：某个level的文件
     * 原理：遍历每个file的id，取最大的当前id再+1作为返回值
    */
    int GetNextSstId(const vector<FileMetaData>& files);

    set<char> GetKeySet(const FileMetaData& file);
    set<char> GetKeySet(const vector<FileMetaData>& files);

    string SearchFromSst(const int& level, const string& key);

    vector<FileMetaData> GetRelativeSst(const vector<FileMetaData>& files, const set<char>& key_set);

    void ReadFileToMap(FileMetaData& file, unordered_map<string, string>& data_map);
    void ReadFileToMap(vector<FileMetaData>& files, unordered_map<string, string>& data_map);

    void MapToSst(const unordered_map<string, string>& current_chunk, const string& filename);

    bool RemoveSstFiles(FileMetaData& file);
    bool RemoveSstFiles(vector<FileMetaData>& files);

    // compact some L0 to L1
    // bool CompactL0ToL1(vector<FileMetaData>& L0_files, vector<FileMetaData>& L1_files);

    /**
     * L1->L2  L2->L3
     * source_files ：源数据文件，更高一层
     * target_files ：目标数据文件，更低一层
     * target_level ：目标层次
     * target_file_size ：目标文件基本大小，设计上下层文件大小是上层的10倍
     * last_level ：标识是否是合并到最底层，仅在最底层处理删除标记
     * 每次将某高层全部合并到低一层
    */
    bool CompactLnBat(int target_level);

    /**
     * 每次选择某层的一个最老文件合并到低一层
    */
    bool CompactLn(int target_level);

public:
    Little_kv(const string& sst_dir_path, const size_t memtable_threshold, const size_t compact_threshold, const size_t max_size, WAL& wal);

    ~Little_kv();

    // put操作（包含普通写入和删除）
    bool put(const std::string& key, const std::string& value);

    // del操作，实际上写入Tombstone
    // 没有真正“删除”旧数据的操作，所有删除操作本质上是put一个特殊的“删除标记”（Tombstone）值。
    bool del(const std::string& key);

    // get操作
    string get(const string& key);

    // manual compcact
    bool manual_compact();

    // 多线程相关
    // 后台刷盘 & 合并
    void flush_background();
    void compact_background();

    // 用于触发
    void maybe_trigger_flush();
    void maybe_trigger_compact();

};