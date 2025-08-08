#pragma once

#include <iostream>
#include <unordered_map>
#include <string>
#include <fstream>
#include <memory>
#include <cstdio>
#include <cmath>
#include <mutex>        // �ṩ std::mutex, std::lock_guard, std::unique_lock
#include <shared_mutex>
#include <thread>
#include <chrono>
#include <ctime>

#include "./MemTable.h"
#include "./ManifestManager.h"  // manage the manifest.txt file

class WAL {

private:
    string wal_file_path;  // memtable �������־
    string log_file_path; // ������־
    

    mutex wal_mutex;
    mutex log_mutex;

public:
    WAL(string wal_dir);
    ~WAL();

    // дWAL��־
    void writeWAL(const string& op, const string& key, const string& value);

    void clearWAL();

    // ��wal�лָ���������
    void recover_data(shared_ptr<MemTable>& active_memtable);

    // ��¼��־
    void log(const string& msg);
};