#pragma once

#include <fstream>
#include <sstream>
#include <string>
#include <unordered_map>
#include <mutex>
#include <memory>
#include <chrono>
#include <iomanip>
#include <filesystem>
#include <zlib.h>

#include "../headers/MemTable.h"

using namespace std;

class WAL {
public:
    WAL(const string& wal_dir, const string& log_dir, const size_t simp_thd);
    ~WAL();

    void writeWAL(const string& op, const string& key, const string& value);
    bool writeBatWAL(const unordered_map<string, string>& put_buff);
    void recover_data(shared_ptr<MemTable>& active_memtable);
    void clearWAL();
    void archive();
    void simplify();
    void log(const string& msg);
    void flush_log();
    uint32_t computeCRC(const string& data);

private:
    string wal_file_path;
    string log_file_path;
    string wal_file_dir;
    string log_file_dir;

    ofstream wal_out;
    ofstream log_file_stream;

    size_t count = 0;
    size_t simplify_threshold;

    mutex wal_mutex;
    mutex log_mutex;

    ostringstream log_buffer;
    size_t log_line_count = 0;
    const size_t log_flush_lines = 10;

    

    string build_line_with_crc(const string& op, const string& key, const string& value);
    bool parse_line_with_crc(const string& line, string& op, string& key, string& value);
};
