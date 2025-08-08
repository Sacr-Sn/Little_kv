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
    size_t per_kv_size;

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


    bool RemoveSstFiles(FileMetaData& file);
    bool RemoveSstFiles(vector<FileMetaData>& files);
    

public:
    Little_kv(const string& sst_dir_path, const size_t memtable_threshold, const size_t compact_threshold, const size_t max_size, const size_t per_kv_size, WAL& wal);

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

    // 清除所有记录
    void clear();

};