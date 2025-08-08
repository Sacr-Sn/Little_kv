#include "../headers/Little_kv.h"

using namespace std;

Little_kv::Little_kv(const string &sst_dir_path,
                     const size_t memtable_threshold,
                     const size_t compact_threshold, const size_t max_size,
                     WAL& wal)
    : sst_dir_path(sst_dir_path),
      memtable_size_threshold(memtable_threshold),
      compact_threshold(compact_threshold), max_file_size(max_size), wal(wal) {
    cout << "========= MemTable init Begin ! =========" << endl;
    active_memtable = make_shared<MemTable>();
    immutable_memtable = nullptr;
    manifest = make_shared<ManifestManager>(sst_dir_path + "manifest.txt");
    Init_data();  // recover last data from wal

    (*manifest).load(max_file_size, levels_multiple, sst_dir_path); // load sst files to manifest
    
    cout << "========= MemTable init Success ! =========" << endl;
}

Little_kv::~Little_kv() {
    (*manifest).save(); // // write sst files information to manifest file
    cout << "========= OVER ! =========" << endl;
}

void Little_kv::Init_data() {
    wal.recover_data(active_memtable);
}


size_t Little_kv::GetMemTableSize() { return active_memtable->size(); }

void Little_kv::Flush() {
    if (!active_memtable || active_memtable->empty())
        return;

    immutable_memtable = active_memtable;
    active_memtable = make_shared<MemTable>();

    string level = "L0";
    int next_sst_id = GetNextSstId((*manifest).getFilesByLevel("L0"));
    string next_sst_name = (*manifest).getNextSstName("L0", next_sst_id);
    char smallest_k = 'a';
    char largest_k = 'a';
    BloomFilter filter(max_file_size / 8, 0.01);  // 预计key数量，容忍误报率（假设每个key-value共8字节）
    FileMetaData fmd(level, next_sst_id, next_sst_name, smallest_k, largest_k, filter);

    string full_sst_name = sst_dir_path + next_sst_name;
    ofstream sst_out(full_sst_name, std::ios::app);
    if (!sst_out.is_open()) {
        cerr << "Failed to open " << full_sst_name << endl;
        return;
    }

    // immutable_memtable is ordered
    // 将key-value写入L0级sst文件，顺便将key都添加到 fmd的filter
    auto entries = (*immutable_memtable).traverse();
    for (const auto &kv : entries) {
        sst_out << kv.first << " " << kv.second << "\n";
        fmd.filter.add(kv.first);
    }
    sst_out.close();

    smallest_k = entries[0].first[0];
    largest_k = entries[entries.size() - 1].first[0];
    fmd.smallest_key = smallest_k;
    fmd.largest_key = largest_k;
    (*manifest).addFile(fmd);

    wal.clearWAL();
    
    immutable_memtable.reset();

    manual_compact(compact_threshold);
}

bool Little_kv::GetKeyRange(const vector<FileMetaData> &files, char &min_k, char &max_k) {
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

bool Little_kv::GetKeyRange_ignore_del(const unordered_map<string, string> data_map, char &min_k, char &max_k) {
    bool all_deled = true;
    for (const auto &[k, v] : data_map) {
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
        for (const auto &[k, v] : data_map) {
            if (v == TOMBSTONE) {
                continue;
            } else {
                if (k[0] < min_k)
                    min_k = k[0];
                if (k[0] > max_k)
                    max_k = k[0];
            }
        }
        return !all_deled;
    } else {
        return all_deled;
    }
}

bool Little_kv::GetKeyRange_contains_del(const unordered_map<string, string> data_map, char &min_k, char &max_k) {
    if (data_map.size() == 0)
        return false;
    for (const auto &[k, v] : data_map) {
        min_k = k[0];
        max_k = k[0];
        break;
    }
    for (const auto &[k, v] : data_map) {
        if (k[0] < min_k)
            min_k = k[0];
        if (k[0] > max_k)
            max_k = k[0];
    }
    return true;
}

set<char> Little_kv::GetKeySet(const vector<FileMetaData> &files) {
    set<char> key_set;
    // traverse every files
    for (const auto &file : files) {
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

vector<FileMetaData> Little_kv::GetRelativeSst(const vector<FileMetaData> &files, const set<char> &key_set) {
    vector<FileMetaData> overlap_files;
    if (key_set.empty())
        return overlap_files;
    char min_k = *key_set.begin();
    char max_k = *key_set.rbegin();
    for (const auto &file : files) {
        if ((file.largest_key < min_k) || (file.smallest_key > max_k))
            continue;
        for (const auto &k : key_set) {
            if ((file.smallest_key <= k) && (file.largest_key >= k)) {
                overlap_files.push_back(file);
                break;
            }
        }
    }
    return overlap_files;
}

void Little_kv::ReadFileToMap(vector<FileMetaData> &files, unordered_map<string, string> &data_map) {
    // read order : old --> new
    sort(files.begin(), files.end(),
         [](const FileMetaData &f1, const FileMetaData &f2) {
             return f1.id < f2.id;
         });
    for (const auto &file : files) {
        string line;
        ifstream sst(sst_dir_path + file.filename);
        if (!sst.is_open()) {
            cerr << "Failed to open " << (sst_dir_path + file.filename) << endl;
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

void Little_kv::MapToSst(const unordered_map<string, string> &data_map, const string &filename) {
    ofstream sst_out(sst_dir_path + filename, ios::trunc);
    if (!sst_out.is_open()) {
        cerr << "Failed to open " << (sst_dir_path + filename) << endl;
    }
    for (const auto &[k, v] : data_map) {
        sst_out << k << " " << v << "\n";
    }
    sst_out.close();
}

int Little_kv::GetNextSstId(const vector<FileMetaData> &files) {
    int id = -1;
    for (const auto &file : files) {
        if (file.id > id)
            id = file.id;
    }
    return (id + 1);
}

bool Little_kv::RemoveSstFiles(vector<FileMetaData> &files) {
    for (const auto &file : files) {
        remove((sst_dir_path + file.filename).c_str());
        (*manifest).removeFile(file.filename);
    }
    return true;
}

bool Little_kv::manual_compact(int compaction_trigger_file_num) {
    if (compaction_trigger_file_num < 2) {
        cout << "compact_file_count is too small" << endl;
        return false;
    }

    bool compact_success = true;
    vector<FileMetaData> source_files;
    vector<FileMetaData> target_files;

    {
        // 先用读锁，只读元数据
        std::shared_lock<std::shared_mutex> manifest_read_lock(rw_manifest_mutex);

        if ((*manifest).getFilesByLevel("L0").size() >= compaction_trigger_file_num) {
            source_files = (*manifest).getFilesByLevel("L0");
            target_files = (*manifest).getFilesByLevel("L1");
        } else if ((*manifest).getFilesByLevel("L1").size() >= compaction_trigger_file_num) {
            source_files = (*manifest).getFilesByLevel("L1");
            target_files = (*manifest).getFilesByLevel("L2");
        } else if ((*manifest).getFilesByLevel("L2").size() >= compaction_trigger_file_num) {
            source_files = (*manifest).getFilesByLevel("L2");
            target_files = (*manifest).getFilesByLevel("L3");
        } else {
            cout << "no need to compact" << endl;
            return false;
        }
    } // 读锁作用域结束，释放

    // 下面开始需要对 manifest 做写操作，需要写锁
    {
        std::unique_lock<std::shared_mutex> manifest_write_lock(rw_manifest_mutex);

        if (!source_files.empty()) {
            string target_level;
            int target_max_size = max_file_size;

            if (source_files[0].level == "L0") {
                target_level = "L1";
                target_max_size = max_file_size;
            } else if (source_files[0].level == "L1") {
                target_level = "L2";
                target_max_size = max_file_size * levels_multiple;
            } else {
                target_level = "L3";
                target_max_size = max_file_size * levels_multiple * levels_multiple;
            }

            compact_success = CompactLn(source_files, target_files, target_level,
                                        target_max_size, target_level == "L3");
            cout << (compact_success ? "compact success!" : "compact failed!") << endl;
        }
    }

    return compact_success;
}

// 注意：调用前必须先持有 manifest 的写锁！
bool Little_kv::CompactLn(vector<FileMetaData> &source_files,
                            vector<FileMetaData> &target_files,
                            string target_level,
                            const size_t &target_file_size,
                            const bool &last_level) {

    // 将target_files、source_files中的数据读入内存，按照由旧到新的顺序读入以覆盖旧值，并按照key排序
    unordered_map<string, string> data_map;
    vector<pair<string, string>> merged_data;
    // map方便插入和覆盖旧值
    ReadFileToMap(target_files, data_map); // target_files中可能为空，更可能有数据
    // 如果source_files是L0层，则key可能有重复，必须按照由旧到新的顺序读入，即要对L0按id排序(从小到大)
    // if (target_level == "L1") {
    //     sort(source_files.begin(), source_files.end(), [](const FileMetaData &f1, const FileMetaData &f2) {
    //         return f1.id < f2.id;
    //     });
    // }
    ReadFileToMap(source_files, data_map);
    // 将map数据存入 vector<pair<string, string>>，以方便对key排序
    for (const auto &[k, v] : data_map) {
        merged_data.push_back(make_pair(k, v));
    }
    // 对set中的数据按key排序，从小到大
    sort(merged_data.begin(), merged_data.end(),
         [](const pair<string, string> &p1, const pair<string, string> &p2) {
             return p1.first <= p2.first;
         });

    // 找出key序列，并查找相关的target_files
    set<char> key_set = GetKeySet(source_files);
    vector<FileMetaData> overlap_files = GetRelativeSst(target_files, key_set);
    cout << "overlap_Ln_files: ";
    for (const auto &file : overlap_files) {
        cout << file.filename << " ";
    }
    cout << endl;

    // 根据目标文件level，确定预计key的数量
    size_t expect_key_num = (max_file_size / 8)*pow(levels_multiple, (target_level[1]-'0'));

    // 根据target_file_size划分merged_data并持久化到target_files
    size_t current_chunk_size = 0;
    unordered_map<string, string>
        current_chunk; // 临时存储每个target_file的数据
    vector<FileMetaData> new_target_files;

    for (int i = 0; i < merged_data.size(); i++) {
        // 仅在最底层处理删除标记
        if (last_level && (merged_data[i].second == TOMBSTONE))
            continue;

        current_chunk[merged_data[i].first] = merged_data[i].second;
        current_chunk_size += (merged_data[i].first.size() + merged_data[i].second.size());

        if (current_chunk_size >= target_file_size &&
            (i < merged_data.size() - 1) &&
            (merged_data[i].first[0] != merged_data[i + 1].first[0])) {  // 触发合并
            int id = GetNextSstId((*manifest).getFilesByLevel(target_level));
            string filename = (*manifest).getNextSstName(target_level, id);
            char s_k = 'z', l_k = 'a';
            if ((!last_level) && GetKeyRange_contains_del(current_chunk, s_k,
                                                          l_k)) { // 中间层合并
                BloomFilter bf(expect_key_num, 0.01);
                FileMetaData fmd(target_level, id, filename, s_k, l_k, bf);
                // 将current_chunk的key添加到fmd的filter
                for (const auto& [key, value] : current_chunk) {
                    fmd.filter.add(key);
                }
                (*manifest).addFile(fmd);
                // write data from map to sst_file
                MapToSst(current_chunk, filename);
            } else if (last_level &&
                       GetKeyRange_ignore_del(current_chunk, s_k,
                                              l_k)) { // 最底层合并
                BloomFilter bf(expect_key_num, 0.01);
                FileMetaData fmd(target_level, id, filename, s_k, l_k, bf);
                // 将current_chunk的key添加到fmd的filter
                for (const auto& [key, value] : current_chunk) {
                    fmd.filter.add(key);
                }
                (*manifest).addFile(fmd);
                // write data from map to sst_file
                MapToSst(current_chunk, filename);
            } else {
                cerr << "合并到 "<< target_level <<" 出错！" << endl;
                return false;
            }
            current_chunk.clear();
            current_chunk_size = 0;
        }
    }
    // 写入残余数据
    if (!current_chunk.empty()) {
        int id = GetNextSstId((*manifest).getFilesByLevel(target_level));
        string filename = (*manifest).getNextSstName(target_level, id);
        char s_k = 'z', l_k = 'a';
        if ((!last_level) &&
            GetKeyRange_contains_del(current_chunk, s_k, l_k)) { // 中间层合并
            BloomFilter bf(expect_key_num, 0.01);
            FileMetaData fmd(target_level, id, filename, s_k, l_k, bf);
            // 将current_chunk的key添加到fmd的filter
            for (const auto& [key, value] : current_chunk) {
                fmd.filter.add(key);
            }
            (*manifest).addFile(fmd);
            // write data from map to sst_file
            MapToSst(current_chunk, filename);
        } else if (last_level && GetKeyRange_ignore_del(current_chunk, s_k, l_k)) { // 最底层合并
            BloomFilter bf(expect_key_num, 0.01);
            FileMetaData fmd(target_level, id, filename, s_k, l_k, bf);
            for (const auto& [key, value] : current_chunk) {
                fmd.filter.add(key);
            }
            (*manifest).addFile(fmd);
            // write data from map to sst_file
            MapToSst(current_chunk, filename);
        } else {
            cerr << "合并出错！" << endl;
            return false;
        }
    }

    // 删除源数据文件和相关原target文件
    RemoveSstFiles(source_files);
    RemoveSstFiles(overlap_files);

    return true;
}

// put --- thread safe
bool Little_kv::put(const std::string &key, const std::string &value) {

    lock_guard<mutex> put_lock(edit_mutex);

    wal.writeWAL("PUT", key, value);

    bool ret = (*active_memtable).insert(key, value);

    if (GetMemTableSize() >= memtable_size_threshold) {
        Flush();
    }
    return ret;
}

bool Little_kv::del(const std::string &key) {
    return put(key, TOMBSTONE); // 实质上是做了put操作，value为删除标记
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

    // 再查 immutable_memtable  已被冻结，不用加锁
    if (immutable_memtable) {
        ret = (*immutable_memtable).search(key);
        if (ret != "_N_E_K_") { // means exists the key
            if (ret == TOMBSTONE)
                return "NOT_FOUND"; // deled tag
            return ret;             // find the value
        }
    }

    // 查各level文件，从顶层到底层：L0 -> L3，即由新到旧
    shared_lock<shared_mutex> manifest_lock(rw_manifest_mutex);

    // search from L0 files
    vector<FileMetaData> l0_files = (*manifest).getFilesByLevel("L0");
    // L0比较特殊，不同的L0中key可能重复，因此要按照id由大到小的顺序查找，以符合数据由新到旧的顺序
    sort(l0_files.begin(), l0_files.end(),
         [](const FileMetaData &f1, const FileMetaData &f2) {
             return f1.id > f2.id;
         });
    for (const auto &file : l0_files) {
        // 初步定位key范围
        if ((key[0] < file.smallest_key) || (key[0] > file.largest_key)) continue;
        // 使用布隆过滤器再次筛查目标sst文件是否包含该key
        if (!file.filter.contains(key)) {
            cout << "Bloom Pass : " << file.filename << endl;
            continue;
        }
        // 通过以上两道粗粒度的筛查后，才读取磁盘文件
        string full_sst_path = sst_dir_path + file.filename;
        string line;
        ifstream sst_in(full_sst_path);
        if (!sst_in.is_open()) {
            cerr << "sst file can not open !" << endl;
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
                    if (v == TOMBSTONE) return "NOT_FOUND";
                    return v;
                }
            }
        }
        sst_in.close();
    }
    // search from L1、L2、L3 files
    ret = "NOT_FOUND";
    vector<string> levels = {"L1", "L2", "L3"};
    for (const auto& level : levels) {
        ret = SearchFromSst(level, key);
        if (ret != "NOT_FOUND") break;
    }

    if (ret == TOMBSTONE) ret = "NOT_FOUND";
    return ret;
}

string Little_kv::SearchFromSst(const string& level, const string& key) {
    vector<FileMetaData> files = (*manifest).getFilesByLevel(level);
    for (const auto &file : files) {
        // 初步定位key范围
        if ((key[0] < file.smallest_key) || (key[0] > file.largest_key)) continue;
        // 使用布隆过滤器再次筛查目标sst文件是否包含该key
        if (!file.filter.contains(key)) {
            cout << "Bloom Pass : " << file.filename << endl;
            continue;
        }
        // 通过以上两道粗粒度的筛查后，才读取磁盘文件
        string full_sst_path = sst_dir_path + file.filename;
        string line;
        ifstream sst_in(full_sst_path);
        if (!sst_in.is_open()) {
            cerr << "sst file can not open !" << endl;
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