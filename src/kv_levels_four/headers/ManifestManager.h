#pragma once

#include <iostream>
#include <vector>
#include <algorithm>
#include <iostream>
#include <fstream>

using namespace std;

struct FileMetaData {
    string level;
    int id;
    string filename;
    char smallest_key;
    char largest_key;

    FileMetaData(string& lvl, int id, const string& fname, const char& skey, const char& lkey)
        : level(lvl), id(id), filename(fname), smallest_key(skey), largest_key(lkey) {}
};

// manager all ssts
class ManifestManager {
private:
    std::string manifest_path;
    std::vector<FileMetaData> file_list;

public:
    ManifestManager(const std::string& path) : manifest_path(path) {}

    bool load();  // load all sst record from manifest.txt to memory
    bool save() const;  // write all records to manifest.txt
    void addFile(const FileMetaData& meta);  // add a record to minifest.txt
    void removeFile(const string& level, const string& filename);
    string getNextSstName(const string& level, const int& id);
    const vector<FileMetaData>& getFileList() const;
    vector<FileMetaData> getFilesByLevel(const string& level) const;
};