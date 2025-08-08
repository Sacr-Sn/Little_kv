#include "../headers/ManifestManager.h"

using namespace std;

bool ManifestManager::load() {
    ifstream in(manifest_dir + "manifest.txt");
    if (!in.is_open()) {
        // cerr << "Failed to open manifest.txt" << endl;
        wal.log("[ManifestManager::load] : Failed to open manifest.txt");
        return false;
    }

    file_list.clear();
    int level;
    int id;
    string fname; // level file_name
    string skey, lkey;     // smallest_key largest_key
    size_t file_size = 0;
    size_t expect_key_num;
    while (in >> level >> id >> fname >> skey >> lkey >> file_size) {
        expect_key_num = allow_file_sizes[level] / per_kv_size;
        // cout << "level:" << level << ", loading, expect_key_num = " << expect_key_num << endl;
        BloomFilter bf(expect_key_num, 0.01);
        FileMetaData fmd(level, id, fname, skey, lkey, file_size, bf);
        // 将sst的key写入fmd的filter
        // cout << "----------- init " << fname << " Bloom Filter -----------" << endl;
        string line;
        ifstream sst_in(manifest_dir + fmd.filename);
        if (!sst_in.is_open()) {
            // cerr << (manifest_dir + fmd.filename) << "failed to open !" << endl;
            wal.log("[ManifestManager::load] : " + (manifest_dir + fmd.filename) + "failed to open !");
            continue;
        }
        while (getline(sst_in, line)) {
            istringstream iss(line);
            string key, value;
            if (iss >> key) {
                // cout << "add key:" << key << endl;
                fmd.filter.add(key);
            }
        }
        sst_in.close();
        file_list.emplace_back(fmd);
    }

    in.close();
    return true;
}

bool ManifestManager::saveToTmp() {
    wal.log("[ManifestManager::saveToTmp] : writing manifest.tmp");
    ofstream out(manifest_dir + "manifest.tmp", ios::trunc);
    if (!out.is_open()) {
        // cerr << "Failed to open manifest.txt" << endl;
        wal.log("[ManifestManager::saveToTmp] : Failed to open manifest.txt");
        return false;
    }

    for (const auto &meta : file_list) {
        out << meta.level << " " << meta.id << " " << meta.filename << " "
            << meta.smallest_key << " " << meta.largest_key << " " << meta.file_size << "\n";
    }

    out.close();
    return true;
}

bool ManifestManager::save() {
    if (saveToTmp()) {
        wal.log("[ManifestManager::save] : remane manifest.tmp to manifest.txt");
        filesystem::rename(manifest_dir + "manifest.tmp", manifest_dir + "manifest.txt");
    }
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

int ManifestManager::getNextSstId(const int& level) {
    int id = -1;
    vector<FileMetaData> fmds = getFilesByLevel(level);
    for (const auto &fmd : fmds) {
        if (fmd.id > id) id = fmd.id;
    }
    return (id + 1);
}

string ManifestManager::getNextSstName(const int &level) {
    int id = getNextSstId(level);
    string next_sst_name = "L" + to_string(level) + "_sst_" + to_string(id) + ".sst";
    return next_sst_name;
}

string ManifestManager::getNextSstName(const int &level, const int& id) {
    string next_sst_name = "L" + to_string(level) + "_sst_" + to_string(id) + ".sst";
    return next_sst_name;
}

const vector<FileMetaData> &ManifestManager::getFileList() const {
    return file_list;
}

vector<FileMetaData> ManifestManager::getRelativeSsts(const int& level, const set<string> &key_set) {
    vector<FileMetaData> overlap_files;
    if (key_set.empty())
        return overlap_files;
    string min_k = *key_set.begin();
    string max_k = *key_set.rbegin();

    string keys = "keys: ";
    for (const auto &k : key_set) {
        keys.append(k);
        keys.append(" ");
    }
    string msg = "[ManifestManager::getRelativeSsts] : {" + keys + "}";
    wal.log(msg);

    for (const auto &file : file_list) {
        if (file.level != level) continue;
        if ((file.largest_key < min_k) || (file.smallest_key > max_k))
            continue;
        for (const auto &k : key_set) {
            if ((file.smallest_key <= k) && (file.largest_key >= k)) {
                overlap_files.push_back(file);
                if (file.largest_key >= max_k) return overlap_files;
                break;
            }
        }
    }
    return overlap_files;
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

bool ManifestManager::getKeyRange(const vector<FileMetaData>& files, string& min_k, string& max_k) {
    min_k = files[0].largest_key;
    max_k = files[0].largest_key;
    for (const auto &file : files) {
        if (file.smallest_key < min_k)
            min_k = file.smallest_key;
        if (file.largest_key > max_k)
            max_k = file.largest_key;
    }
    return true;
}

bool ManifestManager::getKeyRange_ignore_del(const unordered_map<string, string> data_map, string& min_k, string& max_k) {
    bool all_deled = true;
    for (const auto &[k, v] : data_map) {
        if (v == TOMBSTONE) {
            continue;
        } else {
            all_deled = false;
            min_k = k;
            max_k = k;
            break;
        }
    }
    if (!all_deled) {
        for (const auto &[k, v] : data_map) {
            if (v == TOMBSTONE) {
                continue;
            } else {
                if (k < min_k)
                    min_k = k;
                if (k > max_k)
                    max_k = k;
            }
        }
        return !all_deled;
    } else {
        return all_deled;
    }
}

bool ManifestManager::getKeyRange_contains_del(const unordered_map<string, string> data_map, string& min_k, string& max_k) {
    if (data_map.size() == 0)
        return false;
    for (const auto &[k, v] : data_map) {
        min_k = k;
        max_k = k;
        break;
    }
    for (const auto &[k, v] : data_map) {
        if (k < min_k)
            min_k = k;
        if (k > max_k)
            max_k = k;
    }
    return true;
}

set<string> ManifestManager::getKeySet(const FileMetaData& file) {
    set<string> key_set;
    string line;
    ifstream sst(manifest_dir + file.filename);
    if (!sst.is_open()) {
        // cerr << "Failed to open " << (sst_dir_path + file.filename) << endl;
        wal.log("Failed to open " + (manifest_dir + file.filename));
        return key_set;
    }
    while (getline(sst, line)) {
        istringstream iss(line);
        string key, value;
        if (iss >> key) {
            key_set.insert(key);
        }
    }
    sst.close();
    
    return key_set;
}

set<string> ManifestManager::getKeySet(const vector<FileMetaData>& files) {
    set<string> key_set;
    // traverse every files
    for (const auto &file : files) {
        string line;
        ifstream sst(manifest_dir + file.filename);
        if (!sst.is_open()) {
            // cerr << "Failed to open " << (sst_dir_path + file.filename) << endl;
            wal.log("[ManifestManager::getKeySet] : Failed to open " + (manifest_dir + file.filename));
            return key_set;
        }
        while (getline(sst, line)) {
            istringstream iss(line);
            string key, value;
            if (iss >> key) {
                key_set.insert(key);
            }
        }
        sst.close();
    }
    return key_set;
}

void ManifestManager::readFileToMap(FileMetaData& file, unordered_map<string, string>& data_map) {
    string line;
    ifstream sst(manifest_dir + file.filename);
    if (!sst.is_open()) {
        // cerr << "Failed to open " << (sst_dir_path + file.filename) << endl;
        wal.log("[ManifestManager::] : Failed to open " + file.filename);
        return;
    }
    while (getline(sst, line)) {
        istringstream iss(line);
        string key, value;
        if (iss >> key) {
            getline(iss, value);
            value = value.empty() ? "" : value.substr(1);
            data_map[key] = value;
        }
    }
    sst.close();
}

void ManifestManager::readFileToMap(vector<FileMetaData>& files, unordered_map<string, string>& data_map) {
    // read order : old --> new  id由小到大
    sort(files.begin(), files.end(),
         [](const FileMetaData &f1, const FileMetaData &f2) {
             return f1.id < f2.id;
         });
    for (const auto &file : files) {
        string line;
        ifstream sst(manifest_dir + file.filename);
        if (!sst.is_open()) {
            // cerr << "Failed to open " << (sst_dir_path + file.filename) << endl;
            wal.log("[ManifestManager::readFileToMap] : Failed to open " + file.filename);
            return;
        }
        while (getline(sst, line)) {
            istringstream iss(line);
            string key, value;
            if (iss >> key) {
                getline(iss, value);
                value = value.empty() ? "" : value.substr(1);
                data_map[key] = value;
            }
        }
        sst.close();
    }
}

void ManifestManager::writeMapToSst(const unordered_map<string, string>& data_map, const string& filename) {
    ofstream sst_out(manifest_dir + filename, ios::trunc);
    if (!sst_out.is_open()) {
        // cerr << "Failed to open " << (sst_dir_path + filename) << endl;
        wal.log("[ManifestManager::writeMapToSst] : Failed to open " + filename);
    }
    for (const auto &[k, v] : data_map) {
        sst_out << k << " " << v << "\n";
    }
    sst_out.close();
}

string ManifestManager::searchFromSst(const int& level, const string& key) {
    for (const auto &file : file_list) {
        if (file.level != level) continue;
        // 初�?�定位key范围
        if ((key < file.smallest_key) || (key > file.largest_key)) continue;
        // 使用布隆过滤器再次筛查目标sst文件�?否包�?�?key
        if (!file.filter.contains(key)) {
            // cout << "Bloom Pass : " << file.filename << endl;
            wal.log("[ManifestManager::searchFromSst  " + key + "] : Bloom Pass : " + file.filename);
            continue;
        }
        // 通过以上两道粗粒度的筛查后，才�?�取磁盘文件
        string full_sst_path = manifest_dir + file.filename;
        string line;
        ifstream sst_in(full_sst_path);
        if (!sst_in.is_open()) {
            // cerr << "[SearchFromSst] : sst file can not open !" << endl;
            wal.log("[ManifestManager::searchFromSst "+ key +"] : sst file can not open !");
            continue;
        }
        while (getline(sst_in, line)) {
            istringstream iss(line);
            string k, v;
            if (iss >> k) {
                getline(iss, v);
                if (key == k) {
                    sst_in.close();
                    v = v.empty() ? "" : v.substr(1);
                    return v;
                }
            }
        }
        sst_in.close();
    }
    return "NOT_FOUND";
}


// ================================= 线程 ======================================

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