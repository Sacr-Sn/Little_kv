#pragma once

#include <iostream>
#include <vector>
#include <algorithm>
#include <iostream>
#include <string>
#include <fstream>
#include <sstream>
#include <cmath>
#include <mutex>        // �ṩ std::mutex, std::lock_guard, std::unique_lock
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
    size_t file_size;  // ��ǰ�ļ���С
    BloomFilter filter; // �����ֶΣ�ÿ��sst����ӵ��һ��filter��������ÿ�� SST �ļ�ʱ�������� key ���� filter

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
        // L0-L3����������������
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
     * ���ظ�level��id��С��fmd
    */
    FileMetaData getEarliestFileByLevel(const int& level) const;
    size_t getLevelSize(const int& level);

    // ��̨�̲߳���

    // ������̨�߳�
    void startBackgroundSave();

    // ֹͣ��̨�߳�
    void stopBackgroundSave();
};