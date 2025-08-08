#include "../headers/ManifestManager.h"

using namespace std;

bool ManifestManager::load() {
    ifstream in(manifest_dir + "manifest.txt");
    if (!in.is_open()) {
        cerr << "Failed to open " << "manifest.txt" << endl;
        return false;
    }

    file_list.clear();
    int level;
    int id;
    string fname; // level file_name
    char skey, lkey;     // smallest_key largest_key
    size_t file_size = 0;
    size_t expect_key_num;
    while (in >> level >> id >> fname >> skey >> lkey >> file_size) {
        expect_key_num = allow_file_sizes[level] / 8;
        cout << "level:" << level << ", loading, expect_key_num = " << expect_key_num << endl;
        BloomFilter bf(expect_key_num, 0.01);
        FileMetaData fmd(level, id, fname, skey, lkey, file_size, bf);
        // 将sst的key写入fmd的filter
        cout << "----------- init " << fname << " Bloom Filter -----------" << endl;
        string line;
        ifstream sst_in(manifest_dir + fmd.filename);
        if (!sst_in.is_open()) {
            cerr << (manifest_dir + fmd.filename) << "failed to open !" << endl;
            continue;
        }
        while (getline(sst_in, line)) {
            istringstream iss(line);
            string key, value;
            if (iss >> key) {
                cout << "add key:" << key << endl;
                fmd.filter.add(key);
            }
        }
        sst_in.close();
        file_list.emplace_back(fmd);
    }

    in.close();
    return true;
}

bool ManifestManager::save() const {
    ofstream out(manifest_dir + "manifest.txt", ios::trunc);
    if (!out.is_open()) {
        cerr << "Failed to open " << "manifest.txt" << endl;
        return false;
    }

    for (const auto &meta : file_list) {
        out << meta.level << " " << meta.id << " " << meta.filename << " "
            << meta.smallest_key << " " << meta.largest_key << " " << meta.file_size << "\n";
    }

    out.close();
    return true;
}

void ManifestManager::addFile(const FileMetaData &meta) {
    file_list.push_back(meta);
}

void ManifestManager::removeFile(const string &filename) {
    file_list.erase(remove_if(file_list.begin(), file_list.end(),
                              [&](const FileMetaData &m) {
                                  return (m.filename == filename);
                              }),
                    file_list.end());
}

string ManifestManager::getNextSstName(const int &level, const int &id) {
    string next_sst_name = "L" + to_string(level) + "_sst_" + to_string(id) + ".sst";
    return next_sst_name;
}

const vector<FileMetaData> &ManifestManager::getFileList() const {
    return file_list;
}

vector<FileMetaData>
ManifestManager::getFilesByLevel(const int &level) const {
    vector<FileMetaData> result;
    for (const auto &f : file_list) {
        if (f.level == level)
            result.push_back(f);
    }
    sort(result.begin(), result.end(),
         [](const FileMetaData &a, const FileMetaData &b) {
             return a.id > b.id; // 按照id从大到小
         });
    return result;
}

FileMetaData ManifestManager::getEarliestFileByLevel(const int& level) const {
    vector<FileMetaData> files;
    for (const auto &f : file_list) {
        if (f.level == level)
            files.push_back(f);
    }

    FileMetaData ret = files[0];
    for (const auto &file : files) {
        if (file.id < ret.id) ret = file;
    }
    return ret;
}

size_t ManifestManager::getLevelSize(const int& level) {
    size_t level_size = 0;
    for (const auto& f : file_list) {
        if (f.level == level) level_size += f.file_size;
    }
    return level_size;
}

void ManifestManager::startBackgroundSave() {
    save_thread = std::thread([this]() {
        while (!stop_thread) {
            std::this_thread::sleep_for(chrono::seconds(5));
            save();
        }
    });
}

void ManifestManager::stopBackgroundSave() {
    stop_thread = true;
    if (save_thread.joinable()) {
        save_thread.join();
    }
}