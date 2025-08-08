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
#include <chrono>
#include <ctime>
#include <filesystem>

#include "./MemTable.h"
// #include "./ManifestManager.h"  // manage the manifest.txt file

class WAL {

private:
    string wal_file_dir;
    string log_file_dir;
    string wal_file_path;  // memtable 级别的日志
    string log_file_path; // 其它日志

    size_t simplify_threshold;  // wal文件“瘦身”门限，每写入一条+1，约定每8个字符为1条。达到门限时将调用“简化”函数
    size_t count;  // wal 条数
    ofstream wal_out;  // 持久打开的写流

    ofstream log_file_stream;  // 持久打开
    ostringstream log_buffer;  // 缓冲
    const size_t log_flush_lines = 10; // 缓冲行数阙值
    size_t log_line_count = 0;


    mutex wal_mutex;
    mutex log_mutex;

public:
    WAL(const string& wal_dir, const string& log_dir, const size_t simp_thd);
    ~WAL();

    // 写WAL日志
    void writeWAL(const string& op, const string& key, const string& value);

    void clearWAL();

    // 从wal中恢复残余数据
    void recover_data(shared_ptr<MemTable>& active_memtable);

    // 记录日志
    void log(const string& msg);
    void flush_log();

    /**
     * 将wal.txt归档（替换clearWAL）
     * 将现有的wal.txt重命名，之后使用新的wal
    */
    void archive();

    void simplify();


    // ====================== 工具性函数 ==========================
    // void getNextWalId();
};