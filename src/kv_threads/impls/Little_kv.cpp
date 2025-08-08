#include "../headers/Little_kv.h"

using namespace std;

Little_kv::Little_kv(const string &sst_dir_path,
                     const size_t memtable_threshold,
                     const size_t compact_threshold, const size_t max_size,
                     WAL& wal)
    : sst_dir_path(sst_dir_path),
      memtable_size_threshold(memtable_threshold),
      compact_threshold(compact_threshold), max_file_size(max_size), wal(wal) {
    // cout << "========= MemTable init Begin ! =========" << endl;
    wal.log("[Little_kv] : ========= MemTable init Begin ! =========");
    active_memtable = make_shared<MemTable>();
    immutable_memtable = nullptr;
    manifest = make_shared<ManifestManager>(sst_dir_path);
    Init_data();  // recover last data from wal

    (*manifest).load(); // load sst files to manifest
    (*manifest).startBackgroundSave();

    flush_thread = thread(&Little_kv::flush_background, this);
    compact_thread = thread(&Little_kv::compact_background, this);
    
    // cout << "========= MemTable init Success ! =========" << endl;
    wal.log("[Little_kv] : ========= MemTable init Success ! =========");
}

Little_kv::~Little_kv() {
    // ��ֹflush
    {
        lock_guard<mutex> flush_lock(flush_mutex);
        stop_flush = true;
    }
    flush_cv.notify_one();  // ֪ͨ�����ѣ�һ������wait()���̣߳�ֻ���źţ����ȴ�
    flush_thread.join();  // ���߳������￨ס��ֱ��flush_thread�ĺ���������ż�������ִ��

    // ��ֹcompact
    {
        lock_guard<mutex> compact_lock(compact_mutex);
        stop_compact = true;
    }
    compact_cv.notify_one();  // ֪ͨ�����ѣ�һ������wait()���߳�
    compact_thread.join();

    // �־û�manifest
    {
        unique_lock<shared_mutex> rw_manifest_lock(rw_manifest_mutex);
        (*manifest).save(); // // write sst files information to manifest file
        (*manifest).stopBackgroundSave();
    }
    // cout << "========= OVER ! =========" << endl;
    wal.log("[~Little_kv] : ========= OVER ! =========");
}

void Little_kv::Init_data() {
    wal.recover_data(active_memtable);
}


void Little_kv::Flush() {
    if (!active_memtable || active_memtable->empty())
        return;

    immutable_memtable = active_memtable;
    active_memtable = make_shared<MemTable>();

    int level = 0;
    int next_sst_id = GetNextSstId((*manifest).getFilesByLevel(0));
    string next_sst_name = (*manifest).getNextSstName(0, next_sst_id);
    char smallest_k = 'a';
    char largest_k = 'a';
    size_t file_size = 0;
    BloomFilter filter((*manifest).allow_file_sizes[0] / 8, 0.01);  // Ԥ��key�������������ʣ�����ÿ��key-value��8�ֽڣ�
    FileMetaData fmd(level, next_sst_id, next_sst_name, smallest_k, largest_k, file_size,filter);

    string full_sst_name = sst_dir_path + next_sst_name;
    ofstream sst_out(full_sst_name, ios::app);
    if (!sst_out.is_open()) {
        cerr << "Failed to open " << full_sst_name << endl;
        return;
    }

    // immutable_memtable is ordered
    // ��key-valueд��L0��sst�ļ���˳�㽫key����ӵ� fmd��filter
    auto entries = (*immutable_memtable).traverse();
    for (const auto &kv : entries) {
        sst_out << kv.first << " " << kv.second << "\n";
        fmd.file_size += (kv.first.size() + kv.second.size());
        fmd.filter.add(kv.first);
    }
    sst_out.close();

    smallest_k = entries[0].first[0];
    largest_k = entries[entries.size() - 1].first[0];
    fmd.smallest_key = smallest_k;
    fmd.largest_key = largest_k;
    (*manifest).addFile(fmd);
    (*manifest).save();

    wal.clearWAL();
    
    immutable_memtable.reset();
}

void Little_kv::flush_background() {
    while (true) {
        unique_lock<mutex> flush_lock(flush_mutex);
        flush_cv.wait(flush_lock, [this] {
            return flush_needed || stop_flush;
        });
        if (stop_flush) break;

        Flush();
        flush_needed = false;

        // ���������ˢ���˳�����Ƿ���Ҫ�ϲ�
        maybe_trigger_compact();
    }
}

void Little_kv::maybe_trigger_flush() {
    {
        lock_guard<mutex> flush_lock(mutex);
        flush_needed = true;
    }
    flush_cv.notify_one();
}

// ʵ�����ڴ˴��ж��Ƿ���Ҫ�ϲ�
bool Little_kv::manual_compact() {

    bool compact_success = true;

    // ѹʵ������L0���ļ����㣬�����㰴������С�㡣
    // ѹʵ����С��1���ϲ���ѹʵ����Խ�����ȼ�Խ�ߣ�ѹʵ������ͬ��Խ�����ڴ����ȼ�Խ�ߡ�
    // L0��ѹʵ��������1ʱ��L0���������Ⱥϲ���
    double compact_score = 0.0;  
    int target_level = 0;

    
    {  // ���ö�����ֻ��Ԫ����
        shared_lock<std::shared_mutex> manifest_read_lock(rw_manifest_mutex);

        // L0��ѹʵ����
        compact_score = (double)(*manifest).getFilesByLevel(0).size() / (double)compact_threshold;
        if (compact_score >= 1) {
            target_level = 1;
        } else {
            double tmp_max_score = 0;
            for (int level=1;level <= 2; level++) {
                compact_score = (double)(*manifest).getLevelSize(level) / (double)((*manifest).allow_file_sizes[level]*(*manifest).levels_multiple);
                if (compact_score > tmp_max_score) {
                    tmp_max_score = compact_score;
                    target_level = level + 1;
                }
            }
            compact_score = tmp_max_score;
        }
    } // ����������������ͷ�
    // cout << "level_size:" << (*manifest).getLevelSize(target_level-1) << " ,allow_size:" << (*manifest).allow_file_sizes[target_level-1] << endl;
    // cout << "compact_score:" << compact_score << " ,target_level:" << target_level << endl; 

    if (compact_score < 1) {  // ����Ҫ�ϲ�
        return compact_success;
    }
    
    {  // ��Ҫ�ϲ������濪ʼ��Ҫ�� manifest ��д��������Ҫд��
        unique_lock<std::shared_mutex> manifest_write_lock(rw_manifest_mutex);
        compact_success = CompactLn(target_level);
    }

    return compact_success;
}

bool Little_kv::CompactLn(int target_level) {

    FileMetaData source_file = (*manifest).getEarliestFileByLevel(target_level - 1);
    vector<FileMetaData> target_files = (*manifest).getFilesByLevel(target_level);

    // ��target_files��source_file�е����ݶ����ڴ棬�����ɾɵ��µ�˳������Ը��Ǿ�ֵ��������key����
    unordered_map<string, string> data_map;
    vector<pair<string, string>> merged_data;

    // �ҳ�key���У���������ص�target_files
    set<char> key_set = GetKeySet(source_file);
    vector<FileMetaData> overlap_files = GetRelativeSst(target_files, key_set);

    // map�������͸��Ǿ�ֵ
    ReadFileToMap(overlap_files, data_map); // target_files�п���Ϊ�գ�������������
    ReadFileToMap(source_file, data_map);

    // ��map���ݴ��� vector<pair<string, string>>���Է����key����
    for (const auto &[k, v] : data_map) {
        merged_data.push_back(make_pair(k, v));
    }
    // ��set�е����ݰ�key���򣬴�С����
    sort(merged_data.begin(), merged_data.end(),
         [](const pair<string, string> &p1, const pair<string, string> &p2) {
             return p1.first <= p2.first;
         });

    // ����Ŀ���ļ�level��ȷ��Ԥ��key������
    size_t expect_key_num = (*manifest).allow_file_sizes[target_level] / 8;

    bool last_compact = (target_level == 3);  // �Ƿ�������ײ�ĺϲ�

    // ����target_file_size����merged_data���־û���target_files
    size_t current_chunk_size = 0;
    unordered_map<string, string>
        current_chunk; // ��ʱ�洢ÿ��target_file������
    vector<FileMetaData> new_target_files;

    string target_fnames = "{";

    for (int i = 0; i < merged_data.size(); i++) {
        // ������ײ㴦��ɾ�����
        if (last_compact && (merged_data[i].second == TOMBSTONE))
            continue;

        current_chunk[merged_data[i].first] = merged_data[i].second;
        current_chunk_size += (merged_data[i].first.size() + merged_data[i].second.size());

        if (current_chunk_size >= (*manifest).allow_file_sizes[target_level] &&
            (i < merged_data.size() - 1) &&
            (merged_data[i].first[0] != merged_data[i + 1].first[0])) {  // �����ϲ�
            int id = GetNextSstId((*manifest).getFilesByLevel(target_level));
            string filename = (*manifest).getNextSstName(target_level, id);
            char s_k = 'z', l_k = 'a';
            if ((!last_compact) && GetKeyRange_contains_del(current_chunk, s_k,
                                                          l_k)) { // �м��ϲ�
                BloomFilter bf(expect_key_num, 0.01);
                FileMetaData fmd(target_level, id, filename, s_k, l_k, 0, bf);
                // ��current_chunk��key��ӵ�fmd��filter
                for (const auto& [key, value] : current_chunk) {
                    fmd.file_size += (key.size() + value.size());
                    fmd.filter.add(key);
                }
                (*manifest).addFile(fmd);
                // write data from map to sst_file
                MapToSst(current_chunk, filename);
                target_fnames += filename + " ";
            } else if (last_compact &&
                       GetKeyRange_ignore_del(current_chunk, s_k,
                                              l_k)) { // ��ײ�ϲ�
                BloomFilter bf(expect_key_num, 0.01);
                FileMetaData fmd(target_level, id, filename, s_k, l_k, 0, bf);
                // ��current_chunk��key��ӵ�fmd��filter
                for (const auto& [key, value] : current_chunk) {
                    fmd.file_size += (key.size() + value.size());
                    fmd.filter.add(key);
                }
                (*manifest).addFile(fmd);
                // write data from map to sst_file
                MapToSst(current_chunk, filename);
                target_fnames += filename + " ";
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
        if ((!last_compact) &&
            GetKeyRange_contains_del(current_chunk, s_k, l_k)) { // �м��ϲ�
            BloomFilter bf(expect_key_num, 0.01);
            FileMetaData fmd(target_level, id, filename, s_k, l_k, 0, bf);
            // ��current_chunk��key��ӵ�fmd��filter
            for (const auto& [key, value] : current_chunk) {
                fmd.file_size += (key.size() + value.size());
                fmd.filter.add(key);
            }
            (*manifest).addFile(fmd);
            // write data from map to sst_file
            MapToSst(current_chunk, filename);
            target_fnames += filename + " ";
        } else if (last_compact && GetKeyRange_ignore_del(current_chunk, s_k, l_k)) { // ��ײ�ϲ�
            BloomFilter bf(expect_key_num, 0.01);
            FileMetaData fmd(target_level, id, filename, s_k, l_k, 0, bf);
            for (const auto& [key, value] : current_chunk) {
                fmd.file_size += (key.size() + value.size());
                fmd.filter.add(key);
            }
            (*manifest).addFile(fmd);
            // write data from map to sst_file
            MapToSst(current_chunk, filename);
            target_fnames += filename + " ";
        } else {
            cerr << "�ϲ�����" << endl;
            return false;
        }
    }

    // ��¼��־
    string msg = "[CompactLn] : compacting {" + source_file.filename + "} and {";
    for (auto& fmd : overlap_files) {
        msg += fmd.filename + " ";
    }
    msg += "} to be ";
    target_fnames += "}";
    msg += target_fnames;
    wal.log(msg);

    // ɾ��Դ�����ļ������ԭtarget�ļ�
    RemoveSstFiles(source_file);
    RemoveSstFiles(overlap_files);

    // ����������־û�manifest
    (*manifest).save();

    return true;
}

// ʵ�ʵĺϲ����� -- �����ϲ�
// ע�⣺����ǰ�����ȳ��� manifest ��д����
bool Little_kv::CompactLnBat(int target_level) {

    vector<FileMetaData> source_files = (*manifest).getFilesByLevel(target_level - 1);
    vector<FileMetaData> target_files = (*manifest).getFilesByLevel(target_level);

    // ��target_files��source_files�е����ݶ����ڴ棬�����ɾɵ��µ�˳������Ը��Ǿ�ֵ��������key����
    unordered_map<string, string> data_map;
    vector<pair<string, string>> merged_data;

    // �ҳ�key���У���������ص�target_files
    set<char> key_set = GetKeySet(source_files);
    vector<FileMetaData> overlap_files = GetRelativeSst(target_files, key_set);

    // map�������͸��Ǿ�ֵ
    ReadFileToMap(overlap_files, data_map); // target_files�п���Ϊ�գ�������������
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

    // ����Ŀ���ļ�level��ȷ��Ԥ��key������
    size_t expect_key_num = (*manifest).allow_file_sizes[target_level] / 8;

    bool last_compact = (target_level == 3);  // �Ƿ�������ײ�ĺϲ�

    // ����target_file_size����merged_data���־û���target_files
    size_t current_chunk_size = 0;
    unordered_map<string, string>
        current_chunk; // ��ʱ�洢ÿ��target_file������
    vector<FileMetaData> new_target_files;

    for (int i = 0; i < merged_data.size(); i++) {
        // ������ײ㴦��ɾ�����
        if (last_compact && (merged_data[i].second == TOMBSTONE))
            continue;

        current_chunk[merged_data[i].first] = merged_data[i].second;
        current_chunk_size += (merged_data[i].first.size() + merged_data[i].second.size());

        if (current_chunk_size >= (*manifest).allow_file_sizes[target_level] &&
            (i < merged_data.size() - 1) &&
            (merged_data[i].first[0] != merged_data[i + 1].first[0])) {  // �����ϲ�
            int id = GetNextSstId((*manifest).getFilesByLevel(target_level));
            string filename = (*manifest).getNextSstName(target_level, id);
            char s_k = 'z', l_k = 'a';
            if ((!last_compact) && GetKeyRange_contains_del(current_chunk, s_k,
                                                          l_k)) { // �м��ϲ�
                BloomFilter bf(expect_key_num, 0.01);
                FileMetaData fmd(target_level, id, filename, s_k, l_k, 0, bf);
                // ��current_chunk��key��ӵ�fmd��filter
                for (const auto& [key, value] : current_chunk) {
                    fmd.file_size += (key.size() + value.size());
                    fmd.filter.add(key);
                }
                (*manifest).addFile(fmd);
                // write data from map to sst_file
                MapToSst(current_chunk, filename);
            } else if (last_compact &&
                       GetKeyRange_ignore_del(current_chunk, s_k,
                                              l_k)) { // ��ײ�ϲ�
                BloomFilter bf(expect_key_num, 0.01);
                FileMetaData fmd(target_level, id, filename, s_k, l_k, 0, bf);
                // ��current_chunk��key��ӵ�fmd��filter
                for (const auto& [key, value] : current_chunk) {
                    fmd.file_size += (key.size() + value.size());
                    fmd.filter.add(key);
                }
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
        if ((!last_compact) &&
            GetKeyRange_contains_del(current_chunk, s_k, l_k)) { // �м��ϲ�
            BloomFilter bf(expect_key_num, 0.01);
            FileMetaData fmd(target_level, id, filename, s_k, l_k, 0, bf);
            // ��current_chunk��key��ӵ�fmd��filter
            for (const auto& [key, value] : current_chunk) {
                fmd.file_size += (key.size() + value.size());
                fmd.filter.add(key);
            }
            (*manifest).addFile(fmd);
            // write data from map to sst_file
            MapToSst(current_chunk, filename);
        } else if (last_compact && GetKeyRange_ignore_del(current_chunk, s_k, l_k)) { // ��ײ�ϲ�
            BloomFilter bf(expect_key_num, 0.01);
            FileMetaData fmd(target_level, id, filename, s_k, l_k, 0, bf);
            for (const auto& [key, value] : current_chunk) {
                fmd.file_size += (key.size() + value.size());
                fmd.filter.add(key);
            }
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

void Little_kv::compact_background() {
    while (true) {
        unique_lock<mutex> compact_lock(compact_mutex);
        compact_cv.wait_for(compact_lock, chrono::seconds(10), [this] {
            return compact_needed || stop_compact;
        });
        if (stop_compact) break;

        manual_compact();
        compact_needed = false;
    }
}

void Little_kv::maybe_trigger_compact() {
    {
        lock_guard<mutex> compact_lock(compact_mutex);
        compact_needed = true;
    }
    compact_cv.notify_one();
}

// put --- thread safe
bool Little_kv::put(const std::string &key, const std::string &value) {

    lock_guard<mutex> put_lock(edit_mutex);

    wal.writeWAL("PUT", key, value);

    bool ret = (*active_memtable).insert(key, value);

    if ((*active_memtable).GetMemTableSize() >= memtable_size_threshold) {
        maybe_trigger_flush();
    }
    // maybe_trigger_compact();
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

    // �ٲ� immutable_memtable  �ѱ����ᣬ���ü���
    if (immutable_memtable) {
        ret = (*immutable_memtable).search(key);
        if (ret != "_N_E_K_") { // means exists the key
            if (ret == TOMBSTONE)
                return "NOT_FOUND"; // deled tag
            return ret;             // find the value
        }
    }

    // ���level�ļ����Ӷ��㵽�ײ㣺L0 -> L3�������µ���
    shared_lock<shared_mutex> manifest_lock(rw_manifest_mutex);

    // search from L0 files
    vector<FileMetaData> l0_files = (*manifest).getFilesByLevel(0);
    // L0�Ƚ����⣬��ͬ��L0��key�����ظ������Ҫ����id�ɴ�С��˳����ң��Է����������µ��ɵ�˳��
    sort(l0_files.begin(), l0_files.end(),
         [](const FileMetaData &f1, const FileMetaData &f2) {
             return f1.id > f2.id;
         });
    for (const auto &file : l0_files) {
        // ������λkey��Χ
        if ((key[0] < file.smallest_key) || (key[0] > file.largest_key)) continue;
        // ʹ�ò�¡�������ٴ�ɸ��Ŀ��sst�ļ��Ƿ������key
        if (!file.filter.contains(key)) {
            // cout << "Bloom Pass : " << file.filename << endl;
            wal.log("[get " + key + "] : Bloom Pass : " + file.filename);
            continue;
        }
        // ͨ���������������ȵ�ɸ��󣬲Ŷ�ȡ�����ļ�
        string full_sst_path = sst_dir_path + file.filename;
        string line;
        ifstream sst_in(full_sst_path);
        if (!sst_in.is_open()) {
            // cerr << "sst file can not open !" << endl;
            wal.log("[get" + key + "] : sst file can not open !");
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
    // search from L1��L2��L3 files
    ret = "NOT_FOUND";
    for (int level=1;level <= 3;level++) {
        ret = SearchFromSst(level, key);
        if (ret != "NOT_FOUND") break;
    }

    if (ret == TOMBSTONE) ret = "NOT_FOUND";
    return ret;
}




// =================================== �����Ժ��� ======================================
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

// TODO
set<char> Little_kv::GetKeySet(const FileMetaData &file) {
    set<char> key_set;
    string line;
    ifstream sst(sst_dir_path + file.filename);
    if (!sst.is_open()) {
        // cerr << "Failed to open " << (sst_dir_path + file.filename) << endl;
        wal.log("Failed to open " + (sst_dir_path + file.filename));
        return key_set;
    }
    while (getline(sst, line)) {
        istringstream iss(line);
        string key, value;
        if (iss >> key) {
            key_set.insert(key[0]);
        }
    }
    sst.close();
    
    return key_set;
}

// TODO
set<char> Little_kv::GetKeySet(const vector<FileMetaData> &files) {
    set<char> key_set;
    // traverse every files
    for (const auto &file : files) {
        string line;
        ifstream sst(sst_dir_path + file.filename);
        if (!sst.is_open()) {
            // cerr << "Failed to open " << (sst_dir_path + file.filename) << endl;
            wal.log("Failed to open " + (sst_dir_path + file.filename));
            return key_set;
        }
        while (getline(sst, line)) {
            istringstream iss(line);
            string key, value;
            if (iss >> key) {
                key_set.insert(key[0]);
            }
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

    string keys = "keys: ";
    for (const auto &k : key_set) {
        keys.push_back(k);
        keys.push_back(' ');
    }
    string msg = "[GetRelativeSst] : {" + keys + "}";
    wal.log(msg);

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

void Little_kv::ReadFileToMap(FileMetaData& file, unordered_map<string, string>& data_map) {
    string line;
    ifstream sst(sst_dir_path + file.filename);
    if (!sst.is_open()) {
        // cerr << "Failed to open " << (sst_dir_path + file.filename) << endl;
        wal.log("[ReadFileToMap] : Failed to open " + file.filename);
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

void Little_kv::ReadFileToMap(vector<FileMetaData> &files, unordered_map<string, string> &data_map) {
    // read order : old --> new  id��С����
    sort(files.begin(), files.end(),
         [](const FileMetaData &f1, const FileMetaData &f2) {
             return f1.id < f2.id;
         });
    for (const auto &file : files) {
        string line;
        ifstream sst(sst_dir_path + file.filename);
        if (!sst.is_open()) {
            // cerr << "Failed to open " << (sst_dir_path + file.filename) << endl;
            wal.log("[ReadFileToMap] : Failed to open " + file.filename);
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
        // cerr << "Failed to open " << (sst_dir_path + filename) << endl;
        wal.log("[MapToSst] : Failed to open " + filename);
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

bool Little_kv::RemoveSstFiles(FileMetaData &file) {
    remove((sst_dir_path + file.filename).c_str());
    (*manifest).removeFile(file.filename);
    (*manifest).save();
    return true;
}

bool Little_kv::RemoveSstFiles(vector<FileMetaData> &files) {
    for (const auto &file : files) {
        remove((sst_dir_path + file.filename).c_str());
        (*manifest).removeFile(file.filename);
    }
    (*manifest).save();
    return true;
}

string Little_kv::SearchFromSst(const int& level, const string& key) {
    vector<FileMetaData> files = (*manifest).getFilesByLevel(level);
    for (const auto &file : files) {
        // ������λkey��Χ
        if ((key[0] < file.smallest_key) || (key[0] > file.largest_key)) continue;
        // ʹ�ò�¡�������ٴ�ɸ��Ŀ��sst�ļ��Ƿ������key
        if (!file.filter.contains(key)) {
            // cout << "Bloom Pass : " << file.filename << endl;
            wal.log("[SearchFromSst " + key + "] : Bloom Pass : " + file.filename);
            continue;
        }
        // ͨ���������������ȵ�ɸ��󣬲Ŷ�ȡ�����ļ�
        string full_sst_path = sst_dir_path + file.filename;
        string line;
        ifstream sst_in(full_sst_path);
        if (!sst_in.is_open()) {
            // cerr << "[SearchFromSst] : sst file can not open !" << endl;
            wal.log("[SearchFromSst "+ key +"] : sst file can not open !");
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
