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
#include "WAL.h"
#include "BloomFilter.h"
#include "./FdCache.h"
#include "./SnappyCompressor.h"

using namespace std;

struct FileMetaData {
    int level;
    int id;
    string filename;
    string smallest_key;
    string largest_key;
    size_t file_size;  // ��ǰ�ļ���С
    BloomFilter filter; // �����ֶΣ�ÿ��sst����ӵ��һ��filter��������ÿ�� SST �ļ�ʱ�������� key ���� filter

    FileMetaData(int& lel, int id, const string& fname, const string& skey, const string& lkey, size_t file_size, const BloomFilter& bf)
        : level(lel), id(id), filename(fname), smallest_key(skey), largest_key(lkey), file_size(file_size), filter(bf) {}
};

// manager all ssts
class SSTableManager {
private:
    string manifest_dir;
    WAL& wal;
    vector<FileMetaData> file_list;
    size_t base_file_size;
    size_t per_kv_size;

    std::thread save_thread;
    std::atomic<bool> stop_thread;
    

public:
    size_t levels_multiple;
    vector<size_t> allow_file_sizes;

    SSTableManager(const string& dir, WAL& wal, size_t base_file_size=32, size_t levels_multiple=4, size_t per_kv_size=8)
     : manifest_dir(dir), wal(wal), base_file_size(base_file_size), levels_multiple(levels_multiple), per_kv_size(per_kv_size) {
        // L0-L3����������������
        allow_file_sizes = {base_file_size, base_file_size, base_file_size*levels_multiple, base_file_size*levels_multiple*levels_multiple};

    };

    bool load();  // load all sst record from manifest.txt to memory
    bool saveToTmp();  // �Ƚ�fmdsд��һ����ʱ�ļ�����ִֹ��save��;����
    bool save();  // write all records to manifest.txt
    void addFile(const FileMetaData& meta);  // add a record to minifest.txt
    void removeFile(const string& filename);
    
    const vector<FileMetaData>& getFileList() const;
    vector<FileMetaData> getFilesByLevel(const int& level) const;
    /**
     * ���ظ�level��id��С��fmd
    */
    FileMetaData getEarliestFileByLevel(const int& level) const;
    size_t getLevelSize(const int& level);
    int getNextSstId(const int& level);
    string getNextSstName(const int& level);
    string getNextSstName(const int& level, const int& id);
    vector<FileMetaData> getRelativeSsts(const int& level, const set<string> &key_set);
    /**
     * ��vector�л�ȡkey�ķ�Χ
    */
    bool getKeyRange(const vector<FileMetaData>& files, string& min_k, string& max_k);
    /**
     * ��map�л�ȡkey�ķ�Χ������valueΪɾ����ǵ�����
     * �ڷ�Ŀ��Ϊ��ײ���ļ��ϲ������У�ɾ����ǲ��ɺ��ԣ��ʲ����øú���
     * ������Ŀ��Ϊ��ײ���ļ��ĺϲ�����
    */
    bool getKeyRange_ignore_del(const unordered_map<string, string> data_map, string& min_k, string& max_k);
    /**
     * ��map�л�ȡkey�ķ�Χ������valueΪɾ����ǵ�����
     * �����ڷ�Ŀ��Ϊ��ײ�ĺϲ�����
    */
    bool getKeyRange_contains_del(const unordered_map<string, string> data_map, string& min_k, string& max_k);

    set<string> getKeySet(const FileMetaData& file);
    set<string> getKeySet(const vector<FileMetaData>& files);

    void readFileToMap(FileMetaData& file, unordered_map<string, string>& data_map);
    void readFileToMap(vector<FileMetaData>& files, unordered_map<string, string>& data_map);

    void writeMapToSst(const unordered_map<string, string>& data_map, const string& filename);

    string searchFromSst(const int& level, const string& key, FdCache& fd_cache);

    // ��̨�̲߳���

    // ������̨�߳�
    void startBackgroundSave();

    // ֹͣ��̨�߳�
    void stopBackgroundSave();
};