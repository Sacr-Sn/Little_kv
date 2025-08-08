#pragma once

#include <iostream>
#include <vector>
#include <algorithm>
#include <iostream>
#include <string>
#include <fstream>
#include <sstream>
#include <cmath>
#include <mutex>        // 提供 std::mutex, std::lock_guard, std::unique_lock
#include <shared_mutex>
#include <thread>
#include <atomic>
#include <chrono>
#include "BloomFilter.h"

using namespace std;

struct FileMetaData {
    int level;
    int id;
    string filename;
    char smallest_key;
    char largest_key;
    size_t file_size;  // 当前文件大小
    BloomFilter filter; // 新增字段，每个sst单独拥有一个filter，在生成每个 SST 文件时，把所有 key 加入 filter

    FileMetaData(int& lel, int id, const string& fname, const char& skey, const char& lkey, size_t file_size, const BloomFilter& bf)
        : level(lel), id(id), filename(fname), smallest_key(skey), largest_key(lkey), file_size(file_size), filter(bf) {}
};

// manager all ssts
class ManifestManager {
private:
    string manifest_dir;
    vector<FileMetaData> file_list;
    size_t base_file_size;

    std::thread save_thread;
    std::atomic<bool> stop_thread;
    

public:
    size_t levels_multiple;
    vector<size_t> allow_file_sizes;

    ManifestManager(const string& dir, size_t base_file_size=32, size_t levels_multiple=4)
     : manifest_dir(dir), base_file_size(base_file_size), levels_multiple(levels_multiple) {
        // L0-L3各层允许的最大容量
        allow_file_sizes = {base_file_size, base_file_size, base_file_size*levels_multiple, base_file_size*levels_multiple*levels_multiple};

    };

    bool load();  // load all sst record from manifest.txt to memory
    bool save() const;  // write all records to manifest.txt
    void addFile(const FileMetaData& meta);  // add a record to minifest.txt
    void removeFile(const string& filename);
    string getNextSstName(const int& level, const int& id);
    const vector<FileMetaData>& getFileList() const;
    vector<FileMetaData> getFilesByLevel(const int& level) const;
    /**
     * 返回该level下id最小的fmd
    */
    FileMetaData getEarliestFileByLevel(const int& level) const;
    size_t getLevelSize(const int& level);

    // 后台线程部分

    // 启动后台线程
    void startBackgroundSave();

    // 停止后台线程
    void stopBackgroundSave();
};