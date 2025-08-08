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
#include <filesystem>

#include "./MemTable.h"
// #include "./ManifestManager.h"  // manage the manifest.txt file

class WAL {

private:
    string wal_file_dir;
    string log_file_dir;
    string wal_file_path;  // memtable �������־
    string log_file_path; // ������־

    size_t simplify_threshold;  // wal�ļ����������ޣ�ÿд��һ��+1��Լ��ÿ8���ַ�Ϊ1�����ﵽ����ʱ�����á��򻯡�����
    size_t count;  // wal ����
    ofstream wal_out;  // �־ô򿪵�д��

    ofstream log_file_stream;  // �־ô�
    ostringstream log_buffer;  // ����
    const size_t log_flush_lines = 10; // ����������ֵ
    size_t log_line_count = 0;


    mutex wal_mutex;
    mutex log_mutex;

public:
    WAL(const string& wal_dir, const string& log_dir, const size_t simp_thd);
    ~WAL();

    // дWAL��־
    void writeWAL(const string& op, const string& key, const string& value);

    void clearWAL();

    // ��wal�лָ���������
    void recover_data(shared_ptr<MemTable>& active_memtable);

    // ��¼��־
    void log(const string& msg);
    void flush_log();

    /**
     * ��wal.txt�鵵���滻clearWAL��
     * �����е�wal.txt��������֮��ʹ���µ�wal
    */
    void archive();

    void simplify();


    // ====================== �����Ժ��� ==========================
    // void getNextWalId();
};