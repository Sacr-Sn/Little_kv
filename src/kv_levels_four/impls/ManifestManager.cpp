#include "../headers/ManifestManager.h"

using namespace std;

bool ManifestManager::load() {
    ifstream in(manifest_path);
    if (!in.is_open()) {
        cerr << "Failed to open " << manifest_path << endl;
        return false;
    }

    file_list.clear();
    int id;
    string level, fname;  // level file_name
    char skey, lkey;  // smallest_key largest_key

    while (in >> level >> id >> fname >> skey >> lkey) {
        FileMetaData fmd(level, id, fname, skey, lkey);
        file_list.emplace_back(fmd);
    }

    in.close();
    return true;
}

bool ManifestManager::save() const {
    ofstream out(manifest_path, ios::trunc);
    if (!out.is_open()) {
        cerr << "Failed to open " << manifest_path << endl;
        return false;
    }

    for (const auto& meta : file_list) {
        out << meta.level << " " << meta.id << " " << meta.filename << " " 
            << meta.smallest_key << " " << meta.largest_key << "\n";
    }

    out.close();
    return true;
}

void ManifestManager::addFile(const FileMetaData& meta) {
    file_list.push_back(meta);
}

void ManifestManager::removeFile(const string& level, const string& filename) {
    file_list.erase(remove_if(file_list.begin(), file_list.end(),
        [&](const FileMetaData& m) { return (m.filename == filename)&&(m.level == level); }), file_list.end());
}

string ManifestManager::getNextSstName(const string& level, const int& id) {
    string next_sst_name = level + "_sst_" + to_string(id) + ".sst";
    return next_sst_name;
}

const vector<FileMetaData>& ManifestManager::getFileList() const {
    return file_list;
}

vector<FileMetaData> ManifestManager::getFilesByLevel(const string& level) const {
    std::vector<FileMetaData> result;
    for (const auto& f : file_list) {
        if (f.level == level) result.push_back(f);
    }
    sort(result.begin(), result.end(), [](const FileMetaData& a, const FileMetaData& b) {
        return a.id > b.id;  // 按照id从大到小
    });
    return result;
}
