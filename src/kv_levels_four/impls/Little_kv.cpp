#include "../headers/Little_kv.h"

using namespace std;

Little_kv::Little_kv(const string &wal_file_path, const string &sst_dir_path,
                     const size_t memtable_threshold,
                     const size_t compact_threshold, const size_t max_size)
    : wal_file_path(wal_file_path), sst_dir_path(sst_dir_path),
      memtable_size_threshold(memtable_threshold),
      compact_threshold(compact_threshold), max_file_size(max_size) {
    active_memtable = make_shared<MemTable>();
    immutable_memtable = nullptr;
    manifest = make_shared<ManifestManager>(sst_dir_path + "manifest.txt");
    init_data();        // recover last data from wal
    (*manifest).load(); // load sst files to manifest
    // readin_ssts_names();
    cout << "========= MemTable init Success ! =========" << endl;
}

Little_kv::~Little_kv() {
    (*manifest).save(); // // write sst files information to manifest file
    cout << "========= OVER ! =========" << endl;
}

void Little_kv::init_data() {
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

void Little_kv::WriteWAL(const string &op, const string &key,
                         const string &value) {
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

size_t Little_kv::GetMemTableSize() { return active_memtable->size(); }

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
    FileMetaData fmd(level, next_sst_id, next_sst_name, smallest_k, largest_k);

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
    largest_k = entries[entries.size() - 1].first[0];
    fmd.smallest_key = smallest_k;
    fmd.largest_key = largest_k;
    (*manifest).addFile(fmd);

    // 3. 清空WAL
    ClearWAL();

    // 4. 释放immutable_memtable
    immutable_memtable.reset();

    // 5. auto compact some L0 to L1  TODO 
    manual_compact(compact_threshold);
}

void Little_kv::ClearWAL() {
    ofstream ofs(wal_file_path, ios::trunc);
    if (!ofs.is_open()) {
        cerr << "Failed to open " << wal_file_path << endl;
    }
    ofs.close();
}

bool Little_kv::GetKeyRange(const vector<FileMetaData> &files, char &min_k,
                            char &max_k) {
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

bool Little_kv::GetKeyRange_ignore_del(
    const unordered_map<string, string> data_map, char &min_k, char &max_k) {
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
bool Little_kv::GetKeyRange_contains_del(
    const unordered_map<string, string> data_map, char &min_k, char &max_k) {
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

vector<FileMetaData>
Little_kv::GetRelativeSst(const vector<FileMetaData> &files,
                          const set<char> &key_set) {
    vector<FileMetaData> overlap_l1;
    if (key_set.empty())
        return overlap_l1;
    char min_k = *key_set.begin();
    char max_k = *key_set.rbegin();
    for (const auto &file : files) {
        if ((file.largest_key < min_k) || (file.smallest_key > max_k))
            continue;
        for (const auto &k : key_set) {
            if ((file.smallest_key <= k) && (file.largest_key >= k)) {
                overlap_l1.push_back(file);
                break;
            }
        }
    }
    return overlap_l1;
}

void Little_kv::ReadFileToMap(vector<FileMetaData> &files,
                              unordered_map<string, string> &data_map) {
    // read order : old --> new
    sort(files.begin(), files.end(),
         [](const FileMetaData &f1, const FileMetaData &f2) {
             return f1.id < f2.id;
         });
    for (const auto &file : files) {
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

void Little_kv::MapToSst(const unordered_map<string, string> &data_map,
                         const string &filename) {
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

string Little_kv::SearchFromSst(const string& level, const string& key) {
    vector<FileMetaData> files = (*manifest).getFilesByLevel(level);
    for (const auto &file : files) {
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
                    sst_in.close();
                    return v;
                }
            }
        }
    }
    return "NOT_FOUND";
}

bool Little_kv::manual_compact(int compaction_trigger_file_num) {
    if ((compaction_trigger_file_num < 2)) {
        cout << "compact_file_count is too small" << endl;
        return false;
    }

    bool compact_success = true;
    vector<FileMetaData> source_files;
    vector<FileMetaData> target_files;
    // ���ʹ��һϵ��if�����ܳ��������ϲ�
    // ���ʹ��if-elseif��else�����������ÿ��ֻ�ϲ�1����Σ������������ϲ���
    // Ϊ�˲��Է��㣬��ʱ�����������С����Ϊ4��
    if ((*manifest).getFilesByLevel("L0").size() >= compaction_trigger_file_num) {
        source_files = (*manifest).getFilesByLevel("L0");
        target_files = (*manifest).getFilesByLevel("L1");
        compact_success =  CompactLn(source_files, target_files, "L1", max_file_size, false);
        cout << (compact_success ? "compact success!" : "compact failed!") << endl;
    } else if ((*manifest).getFilesByLevel("L1").size() >= compaction_trigger_file_num) {
        source_files = (*manifest).getFilesByLevel("L1");
        target_files = (*manifest).getFilesByLevel("L2");
        compact_success =  CompactLn(source_files, target_files, "L2", max_file_size*4, false);
        cout << (compact_success ? "compact success!" : "compact failed!") << endl;
    } else if ((*manifest).getFilesByLevel("L2").size() >= compaction_trigger_file_num) {
        source_files = (*manifest).getFilesByLevel("L2");
        target_files = (*manifest).getFilesByLevel("L3");
        compact_success =  CompactLn(source_files, target_files, "L3", max_file_size*4*4, true);
        cout << (compact_success ? "compact success!" : "compact failed!") << endl;
    } else {
        cout << "no need to compact" << endl;
    }
    
    return compact_success;
}

bool Little_kv::RemoveSstFiles(vector<FileMetaData> &files) {
    for (const auto &file : files) {
        remove((sst_dir_path + file.filename).c_str());
        (*manifest).removeFile(file.level, file.filename);
    }
    return true;
}

bool Little_kv::CompactLn(vector<FileMetaData> &source_files,
                            vector<FileMetaData> &target_files,
                            string target_level,
                            const size_t &target_file_size,
                            const bool &last_level) {
    // ��target_files��source_files�е����ݶ����ڴ棬�����ɾɵ��µ�˳������Ը��Ǿ�ֵ��������key����
    unordered_map<string, string> data_map;
    vector<pair<string, string>> merged_data;
    // map�������͸��Ǿ�ֵ
    ReadFileToMap(target_files, data_map); // target_files�п���Ϊ�գ�������������
    // ���source_files��L0�㣬��key�������ظ������밴���ɾɵ��µ�˳����룬��Ҫ��L0��id����(��С����)
    if (target_level == "L1") {
        sort(source_files.begin(), source_files.end(), [](const FileMetaData &f1, const FileMetaData &f2) {
            return f1.id < f2.id;
        });
    }
    ReadFileToMap(source_files, data_map);
    // ��map���ݴ��� vector<pair<string, string>>���Է����key����
    for (const auto &[k, v] : data_map) {
        merged_data.push_back(make_pair(k, v));
    }
    // ��set�е����ݰ�key���򣬴�С����
    sort(merged_data.begin(), merged_data.end(),
         [](const pair<string, string> &p1, const pair<string, string> &p2) {
             return p1.first <= p2.first;
         });

    // �ҳ�key���У���������ص�target_files
    set<char> key_set = GetKeySet(source_files);
    vector<FileMetaData> overlap_files = GetRelativeSst(target_files, key_set);
    cout << "overlap_Ln_files: ";
    for (const auto &file : overlap_files) {
        cout << file.filename << " ";
    }
    cout << endl;

    // ����target_file_size����merged_data���־û���target_files
    size_t current_chunk_size = 0;
    unordered_map<string, string>
        current_chunk; // ��ʱ�洢ÿ��target_file������
    vector<FileMetaData> new_target_files;

    for (int i = 0; i < merged_data.size(); i++) {
        // ������ײ㴦��ɾ�����
        if (last_level && (merged_data[i].second == TOMBSTONE))
            continue;

        current_chunk[merged_data[i].first] = merged_data[i].second;
        current_chunk_size += (merged_data[i].first.size() + merged_data[i].second.size());

        if (current_chunk_size >= target_file_size &&
            (i < merged_data.size() - 1) &&
            (merged_data[i].first[0] != merged_data[i + 1].first[0])) {
            int id = GetNextSstId((*manifest).getFilesByLevel(target_level));
            string filename = (*manifest).getNextSstName(target_level, id);
            char s_k = 'z', l_k = 'a';
            if ((!last_level) && GetKeyRange_contains_del(current_chunk, s_k,
                                                          l_k)) { // �м��ϲ�
                FileMetaData fmd(target_level, id, filename, s_k, l_k);
                (*manifest).addFile(fmd);
                // write data from map to sst_file
                MapToSst(current_chunk, filename);
            } else if (last_level &&
                       GetKeyRange_ignore_del(current_chunk, s_k,
                                              l_k)) { // ��ײ�ϲ�
                FileMetaData fmd(target_level, id, filename, s_k, l_k);
                (*manifest).addFile(fmd);
                // write data from map to sst_file
                MapToSst(current_chunk, filename);
            } else {
                cerr << "�ϲ��� "<< target_level <<" ����" << endl;
                return false;
            }
            current_chunk.clear();
            current_chunk_size = 0;
        }
    }
    // д���������
    if (!current_chunk.empty()) {
        int id = GetNextSstId((*manifest).getFilesByLevel(target_level));
        string filename = (*manifest).getNextSstName(target_level, id);
        char s_k = 'z', l_k = 'a';
        if ((!last_level) &&
            GetKeyRange_contains_del(current_chunk, s_k, l_k)) { // �м��ϲ�
            FileMetaData fmd(target_level, id, filename, s_k, l_k);
            (*manifest).addFile(fmd);
            // write data from map to sst_file
            MapToSst(current_chunk, filename);
        } else if (last_level && GetKeyRange_ignore_del(current_chunk, s_k, l_k)) { // ��ײ�ϲ�
            FileMetaData fmd(target_level, id, filename, s_k, l_k);
            (*manifest).addFile(fmd);
            // write data from map to sst_file
            MapToSst(current_chunk, filename);
        } else {
            cerr << "�ϲ�����" << endl;
            return false;
        }
    }

    // ɾ��Դ�����ļ������ԭtarget�ļ�
    RemoveSstFiles(source_files);
    RemoveSstFiles(overlap_files);

    return true;
}

bool Little_kv::put(const std::string &key, const std::string &value) {
    WriteWAL("PUT", key, value);
    // (*active_memtable)[key] = value;
    bool ret = (*active_memtable).insert(key, value);

    if (GetMemTableSize() >= memtable_size_threshold) {
        Flush();
    }
    return ret;
}

bool Little_kv::del(const std::string &key) {
    return put(key, TOMBSTONE); // ʵ����������put������valueΪɾ�����
}

string Little_kv::get(const string &key) {
    // order : active_memtable immutable_memtable  L0  L1

    // �Ȳ� active_memtable
    string ret = (*active_memtable).search(key);
    if (ret != "_N_E_K_") { // means exists the key
        if (ret == TOMBSTONE)
            return "NOT_FOUND"; // deled tag
        return ret;             // find the value
    }

    // �ٲ� immutable_memtable
    if (immutable_memtable) {
        ret = (*immutable_memtable).search(key);
        if (ret != "_N_E_K_") { // means exists the key
            if (ret == TOMBSTONE)
                return "NOT_FOUND"; // deled tag
            return ret;             // find the value
        }
    }

    // ���level�ļ����Ӷ��㵽�ײ㣺L0 -> L3�������µ���
    // search from L0 files
    vector<FileMetaData> l0_files = (*manifest).getFilesByLevel("L0");
    // L0�Ƚ����⣬��ͬ��L0��key�����ظ������Ҫ����id�ɴ�С��˳����ң��Է����������µ��ɵ�˳��
    sort(l0_files.begin(), l0_files.end(),
         [](const FileMetaData &f1, const FileMetaData &f2) {
             return f1.id > f2.id;
         });
    for (const auto &file : l0_files) {
        string full_sst_path = sst_dir_path + file.filename;
        ifstream sst_in(full_sst_path);
        if (!sst_in.is_open()) {
            cerr << "sst file can not open !" << endl;
            continue;
        }
        string k, v;
        while (sst_in >> k >> v) {
            if (key == k) {
                sst_in.close();
                if (v == TOMBSTONE)
                    return "NOT_FOUND";
                return v;
            }
        }
        sst_in.close();
    }
    // search from L1��L2��L3 files
    ret = "NOT_FOUND";
    vector<string> levels = {"L1", "L2", "L3"};
    for (const auto& level : levels) {
        ret = SearchFromSst(level, key);
        if (ret != "NOT_FOUND") break;
    }

    if (ret == TOMBSTONE) ret = "NOT_FOUND";
    return ret;
}
