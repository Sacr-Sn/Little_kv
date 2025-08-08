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

#include "./MemTable.h"
#include "./ManifestManager.h"  // manage the manifest.txt file

class WAL {

private:
    string wal_file_path;  // 日志路径

    mutex wal_mutex;

public:
    WAL(string wal_path);
    ~WAL();

    // 写WAL日志
    void writeWAL(const string& op, const string& key, const string& value);

    void clearWAL();

    // 从wal中恢复残余数据
    void recover_data(shared_ptr<MemTable>& active_memtable);
};