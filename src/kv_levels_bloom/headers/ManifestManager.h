#pragma once

#include <iostream>
#include <vector>
#include <algorithm>
#include <iostream>
#include <string>
#include <fstream>
#include <sstream>
#include <cmath>
#include "BloomFilter.h"

using namespace std;

struct FileMetaData {
    string level;
    int id;
    string filename;
    char smallest_key;
    char largest_key;
    BloomFilter filter; // 新增字段，每个sst单独拥有一个filter，在生成每个 SST 文件时，把所有 key 加入 filter

    FileMetaData(string& lvl, int id, const string& fname, const char& skey, const char& lkey, const BloomFilter& bf)
        : level(lvl), id(id), filename(fname), smallest_key(skey), largest_key(lkey), filter(bf) {}
};

// manager all ssts
class ManifestManager {
private:
    std::string manifest_path;
    std::vector<FileMetaData> file_list;

public:
    ManifestManager(const std::string& path) : manifest_path(path) {}

    bool load(size_t max_file_size, size_t levels_multiple, const string& sst_dir_path);  // load all sst record from manifest.txt to memory
    bool save() const;  // write all records to manifest.txt
    void addFile(const FileMetaData& meta);  // add a record to minifest.txt
    void removeFile(const string& filename);
    string getNextSstName(const string& level, const int& id);
    const vector<FileMetaData>& getFileList() const;
    vector<FileMetaData> getFilesByLevel(const string& level) const;
};