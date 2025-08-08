#include "../headers/Little_kv.h"

using namespace std;

Little_kv::Little_kv(const string& wal_file_path, const string& sst_dir_path, 
                    const size_t memtable_threshold, const size_t compact_threshold, const size_t max_size)
    : wal_file_path(wal_file_path), sst_dir_path(sst_dir_path), memtable_size_threshold(memtable_threshold), compact_threshold(compact_threshold), max_file_size(max_size)
{
    active_memtable = make_shared<MemTable>();
    immutable_memtable = nullptr;
    manifest = make_shared<ManifestManager>(sst_dir_path + "manifest.txt");
    init_data();  // recover last data from wal
    (*manifest).load();  // load sst files to manifest
    // readin_ssts_names();
    cout << "========= MemTable init Success ! =========" << endl;
}

Little_kv::~Little_kv()
{
    (*manifest).save();  // // write sst files information to manifest file
    cout << "========= OVER ! =========" << endl;
}

void Little_kv::init_data()
{
    ifstream wal_in(wal_file_path);
    if (!wal_in.is_open())
        return;

    string op, key, value;
    while (wal_in >> op >> key) {
        if (op == "PUT") {
            if (!(wal_in >> value)) {
                cerr << "Invalid put format, skipping line" << endl;
                continue;
            }
            // (*active_memtable)[key] = value;
            (*active_memtable).insert(key, value);
        } else if (op == "DEL") {
            // (*active_memtable)[key] = TOMBSTONE;
            (*active_memtable).insert(key, TOMBSTONE);
        } else {
            cerr << "UNknown op: " << op << endl;
        }
    }
    wal_in.close();
}

void Little_kv::WriteWAL(const string &op, const string &key, const string &value)
{
    ofstream wal_out(wal_file_path, ios::app);
    if (!wal_out.is_open()) {
        cerr << "Failed to open " << wal_file_path << endl;
        return;
    }
    wal_out << op << " " << key << " ";
    if (op == "PUT")
        wal_out << value;
    wal_out << "\n";
    wal_out.close();
}

size_t Little_kv::GetMemTableSize() {
    return active_memtable->size();
}

void Little_kv::Flush() {
    if (!active_memtable || active_memtable->empty())
        return;

    // 1. 将active_memtable变为immutable_memtable，创建新active_memtable
    immutable_memtable = active_memtable;
    active_memtable = make_shared<MemTable>();

    // 2. 将immutable_memtable写入 L0 sst文件（简单写法：覆盖写）
    string level = "L0";
    int next_sst_id = GetNextSstId((*manifest).getFilesByLevel("L0"));
    string next_sst_name = (*manifest).getNextSstName("L0", next_sst_id);
    char smallest_k = 'a';
    char largest_k = 'a';
    FileMetaData fmd(level ,next_sst_id, next_sst_name, smallest_k, largest_k);

    string full_sst_name = sst_dir_path + next_sst_name;
    ofstream sst_out(full_sst_name, std::ios::app);
    if (!sst_out.is_open()) {
        cerr << "Failed to open " << full_sst_name << endl;
        return;
    }

    // immutable_memtable is ordered
    auto entries = (*immutable_memtable).traverse();
    for (const auto &kv : entries) {
        sst_out << kv.first << " " << kv.second << "\n";
    }
    sst_out.close();

    smallest_k = entries[0].first[0];
    largest_k = entries[entries.size()-1].first[0];
    fmd.smallest_key = smallest_k;
    fmd.largest_key = largest_k;
    (*manifest).addFile(fmd);

    // 3. 清空WAL
    ClearWAL();

    // 4. 释放immutable_memtable
    immutable_memtable.reset();

    // 5. auto compact some L0 to L1
    int l0_count = (*manifest).getFilesByLevel("L0").size();
    if (l0_count >= compact_threshold) {
        cout << (manual_compact(l0_count) ? "compact success  !" : "compact failed !") << endl;
    }
}

void Little_kv::ClearWAL() {
    ofstream ofs(wal_file_path, ios::trunc);
    if (!ofs.is_open()) {
        cerr << "Failed to open " << wal_file_path << endl;
    }
    ofs.close();
}

bool Little_kv::GetKeyRange(const vector<FileMetaData>& files, char& min_k, char& max_k) {
    min_k = files[0].largest_key;
    max_k = files[0].largest_key;
    for (const auto& file : files) {
        if (file.smallest_key < min_k) min_k = file.smallest_key;
        if (file.largest_key > max_k) max_k = file.largest_key;
    }
    return true;
}

bool Little_kv::GetKeyRange_ignore_del(const unordered_map<string, string> data_map, char& min_k, char& max_k) {
    bool all_deled = true;
    for (const auto& [k, v] : data_map) {
        if (v == TOMBSTONE) {
            continue;
        } else {
            all_deled = false;
            min_k = k[0];
            max_k = k[0];
            break;
        }
    }
    if (!all_deled) {
        for (const auto& [k, v] : data_map) {
            if (v == TOMBSTONE) {
                continue;
            } else {
                if (k[0] < min_k) min_k = k[0];
                if (k[0] > max_k) max_k = k[0];
            }
        }
        return !all_deled;
    } else {
        return all_deled;
    }
}
bool Little_kv::GetKeyRange_contains_del(const unordered_map<string, string> data_map, char& min_k, char& max_k) {
    if (data_map.size() == 0) return false;
    for (const auto& [k, v] : data_map) {
        min_k = k[0];
        max_k = k[0];
        break;
    }
    for (const auto& [k, v] : data_map) {
        if (k[0] < min_k) min_k = k[0];
        if (k[0] > max_k) max_k = k[0];
    }
    return true;
}

set<char> Little_kv::GetKeySet(const vector<FileMetaData>& files) {
    set<char> key_set;
    // traverse every files
    for (const auto& file : files) {
        ifstream sst(sst_dir_path + file.filename);
        if (!sst.is_open()) {
            cerr << "Failed to open " << (sst_dir_path + file.filename) << endl;
            return key_set;
        }
        string key, value;
        while (sst >> key >> value) {
            key_set.insert(key[0]);
        }
        sst.close();
    }
    return key_set;
}

vector<FileMetaData> Little_kv::GetRelativeL1(const vector<FileMetaData>& L1_files, const set<char>& key_set) {
    vector<FileMetaData> overlap_l1;
    if (key_set.empty()) return overlap_l1;
    char min_k = *key_set.begin();
    char max_k = *key_set.rbegin();
    for (const auto& file : L1_files) {
        if ((file.largest_key < min_k) || (file.smallest_key > max_k)) continue;
        for (const auto& k : key_set) {
            if ((file.smallest_key <= k)&&(file.largest_key >= k)) {
                overlap_l1.push_back(file);
                break;
            }
        }
    }
    return overlap_l1;
}

void Little_kv::ReadFileToMap(vector<FileMetaData>& files, unordered_map<string, string>& data_map) {
    // read order : old --> new
    sort(files.begin(), files.end(), [](const FileMetaData& f1, const FileMetaData& f2) {
        return f1.id < f2.id;
    });
    for (const auto& file : files) {
        ifstream sst(sst_dir_path + file.filename);
        if (!sst.is_open()) {
            cerr << "Failed to open " << (sst_dir_path + file.filename) << endl;
            return;
        }
        string key, value;
        while (sst >> key >> value) {
            data_map[key] = value;
        }
        sst.close();
    }
}

void Little_kv::MapToSst(const unordered_map<string, string>& data_map, const string& filename) {
    ofstream sst_out(sst_dir_path + filename, ios::trunc);
    if (!sst_out.is_open()) {
        cerr << "Failed to open " << (sst_dir_path + filename) << endl;
    }
    for (const auto& [k, v] : data_map) {
        sst_out << k << " " << v << "\n";
    }
    sst_out.close();
}

int Little_kv::GetNextSstId(const vector<FileMetaData>& files) {
    int id = -1;
    for (const auto& file : files) {
        if (file.id > id) id = file.id;
    }
    return (id + 1);
}

bool Little_kv::manual_compact(int compact_file_count) {
    if ((compact_file_count < 2)) {
        cout << "do not need to compact" << endl;
        return false;
    }

    vector<FileMetaData> l0_files = (*manifest).getFilesByLevel("L0");
    vector<FileMetaData> l1_files = (*manifest).getFilesByLevel("L1");

    return CompactL0ToL1(l0_files, l1_files);
}

bool Little_kv::RemoveSstFiles(vector<FileMetaData>& files) {
    for (const auto& file : files) {
        remove((sst_dir_path + file.filename).c_str());
        (*manifest).removeFile(file.level, file.filename);
    }
    return true;
}

bool Little_kv::CompactL0ToL1(vector<FileMetaData>& L0_files, vector<FileMetaData>& L1_files) {
    // get key range of L0
    char l0_min_k, l0_max_k;
    GetKeyRange(L0_files, l0_min_k, l0_max_k);
    cout << "l0_min_k: " << l0_min_k << ", l0_max_k: " << l0_max_k << endl;
    // find relative L1 sst files
    set<char> key_set = GetKeySet(L0_files);
    vector<FileMetaData> overlap_l1_files = GetRelativeL1(L1_files, key_set);
    cout << "overlap_l1_files: ";
    for (const auto& file : overlap_l1_files) {
        cout << file.filename << " ";
    }
    cout << endl;

    // read data from sst to vector<pair<string, string>>
    unordered_map<string, string> data_map;
    vector<pair<string, string>> merged_data;
    // read relative L1 sst files to map
    ReadFileToMap(overlap_l1_files, data_map);
    // read L0 sst files to map, overwrite the old values
    ReadFileToMap(L0_files, data_map);
    // unordered_map to vector<pair<string, string>>
    for (const auto& [k, v] : data_map) {
        merged_data.push_back(make_pair(k, v));
    }
    // sort vec
    sort(merged_data.begin(), merged_data.end(), [](const pair<string, string>& p1, const pair<string, string>& p2) {
        return p1.first <= p2.first;
    });

    // write data fron memory to several L1 file
    size_t current_chunk_size = 0;
    unordered_map<string, string> current_chunk;
    vector<FileMetaData> new_l1_files;

    for (int i=0; i<merged_data.size(); i++) {
        if (merged_data[i].second == TOMBSTONE) continue;   

        current_chunk[merged_data[i].first] = merged_data[i].second;
        current_chunk_size += (merged_data[i].first.size() + merged_data[i].second.size());

        if (current_chunk_size >= max_file_size && (i < merged_data.size()-1) && (merged_data[i].first[0] != merged_data[i+1].first[0])) {
            int id = GetNextSstId((*manifest).getFilesByLevel("L1"));
            string level = "L1", filename = (*manifest).getNextSstName(level, id);
            char s_k = l0_max_k, l_k = l0_min_k;
            if (GetKeyRange_ignore_del(current_chunk, s_k, l_k)) {
                FileMetaData fmd(level, id, filename, s_k, l_k);
                (*manifest).addFile(fmd);
                // write data from map to sst_file
                MapToSst(current_chunk, filename);
            }
            current_chunk.clear();
            current_chunk_size = 0;
        }
    }
    // write the last data
    if (!current_chunk.empty()) {
        int id = GetNextSstId((*manifest).getFilesByLevel("L1"));
        string level = "L1", filename = (*manifest).getNextSstName(level, id);
        char s_k = l0_max_k, l_k = l0_min_k;
        if (GetKeyRange_ignore_del(current_chunk, s_k, l_k)) {
            FileMetaData fmd(level, id, filename, s_k, l_k);
            (*manifest).addFile(fmd);
            // write data from map to sst_file
            MapToSst(current_chunk, filename);
        }
    }

    // delete these L0 and L1 sst files
    RemoveSstFiles(L0_files);
    RemoveSstFiles(overlap_l1_files);

    return true;
}


bool Little_kv::put(const std::string &key, const std::string &value) {
    WriteWAL("PUT", key, value);
    // (*active_memtable)[key] = value;
    bool ret = (*active_memtable).insert(key, value);

    if (GetMemTableSize() >= memtable_size_threshold)
    {
        Flush();
    }
    return ret;
}

bool Little_kv::del(const std::string &key) {
    return put(key, TOMBSTONE); // 注意，日志中仍写del而不是put，不可直接调用put方法
}

string Little_kv::get(const string &key) {
    // order : active_memtable immutable_memtable  L0  L1

    // 先查 active_memtable
    string ret = (*active_memtable).search(key);
    if (ret != "_N_E_K_") { // means exists the key
        if (ret == TOMBSTONE)
            return "NOT_FOUND"; // deled tag
        return ret;             // find the value
    }

    // 再查 immutable_memtable
    if (immutable_memtable) {
        ret = (*immutable_memtable).search(key);
        if (ret != "_N_E_K_")
        { // means exists the key
            if (ret == TOMBSTONE)
                return "NOT_FOUND"; // deled tag
            return ret;             // find the value
        }
    }

    // 查 L0 sst 文件   from new to old 
    vector<FileMetaData> l0_files = (*manifest).getFilesByLevel("L0");
    sort(l0_files.begin(), l0_files.end(), [](const FileMetaData& f1, const FileMetaData& f2) {
        return f1.id > f2.id;
    });
    for (const auto& file : l0_files) {
        string full_sst_path = sst_dir_path + file.filename;
        ifstream sst_in(full_sst_path);
        if (!sst_in.is_open()) {
            cerr << "sst file can not open !" << endl;
            continue;
        }
        string k, v;
        while (sst_in >> k >> v) {
            if (key == k) {
                if (v == TOMBSTONE)
                    return "NOT_FOUND";
                sst_in.close();
                return v;
            }
        }
        sst_in.close();
    }

    // check L1 files
    vector<FileMetaData> l1_files = (*manifest).getFilesByLevel("L1");
    for (const auto& file : l1_files) {
        if ((file.smallest_key <= key[0]) && (file.largest_key >= key[0])) {
            string full_sst_path = sst_dir_path + file.filename;
            ifstream sst_in(full_sst_path);
            if (!sst_in.is_open()) {
                cerr << "sst file can not open !" << endl;
                continue;
            }
            string k, v;
            while (sst_in >> k >> v) {
                if (key == k) {
                    if (v == TOMBSTONE)
                        return "NOT_FOUND";
                    sst_in.close();
                    return v;
                }
            }
        }
    }

    return "NOT_FOUND";
}

