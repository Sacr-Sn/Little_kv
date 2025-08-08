#include "../headers/Little_kv.h"

using namespace std;

Little_kv::Little_kv(const string &sst_dir_path,
                     const size_t memtable_threshold,
                     const size_t compact_threshold, const size_t max_size, const size_t per_kv_size,
                     WAL& wal)
    : sst_dir_path(sst_dir_path),
      memtable_size_threshold(memtable_threshold),
      compact_threshold(compact_threshold), max_file_size(max_size), per_kv_size(per_kv_size), wal(wal) {
    // cout << "========= MemTable init Begin ! =========" << endl;
    wal.log("[Little_kv] : ========= MemTable init Begin ! =========");
    active_memtable = make_shared<MemTable>();
    // immutable_memtable = nullptr;
    sst_manager = make_shared<SSTableManager>(sst_dir_path, wal, max_file_size, 4, per_kv_size);
    mem_manager = make_shared<MemTableManager>();

    Init_data();  // recover last data from wal

    sst_manager->load(); // load sst files to manifest
    // (*manifest).startBackgroundSave();  // ��̨�̶߳�ʱ�־û�manifest

    flush_thread = thread(&Little_kv::flush_background, this);
    
    // compact ���
    compaction_mgr.set_compact_executor([this](CompactionTask task) {
        CompactLn(task.target_level);  // ִ��compact����
    });
    compaction_mgr.start();  // ��ʼ����������������compact�����߳�
    detector_thread = thread(&Little_kv::detector_loop, this);  // ��������߳�
    
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
    stop_detector = true;
    if (detector_thread.joinable()) detector_thread.join();
    compaction_mgr.stop();  // ��ֹcompact�����߳�

    // �־û�manifest
    {
        unique_lock<shared_mutex> rw_manifest_lock(rw_manifest_mutex);
        sst_manager->save(); // // write sst files information to manifest file
        // (*manifest).stopBackgroundSave();  // �رպ�ִ̨�ж�ʱ������߳�
    }
    // cout << "========= OVER ! =========" << endl;
    wal.log("[~Little_kv] : ========= OVER ! =========");
}

void Little_kv::Init_data() {
    // TODO  �ָ�δˢ�̵�wal��immune_mem��������ˢ��
    wal.recover_unflushed(mem_manager);
    wal.recover_current(active_memtable);  // �ָ���ǰ��Ծ��wal_current.log��active_mem
}

void Little_kv::Flush() {
    cout << "[Little_kv::Flush()] : ����Flush()" << endl;
    while ((*mem_manager).has_immutable()) {
        // ׼������ SST �ļ�
        int level = 0;
        int next_sst_id = sst_manager->getNextSstId(level);
        string next_sst_name = sst_manager->getNextSstName(level, next_sst_id);
        string full_sst_name = sst_dir_path + next_sst_name;
        ofstream sst_out(full_sst_name, ios::trunc);
        if (!sst_out.is_open()) {
            cerr << "Failed to open " << full_sst_name << endl;
            continue;
        }

        // ���� MemTable��д���ļ������� Bloom filter
        string smallest_k, largest_k;
        size_t file_size = 0;
        BloomFilter filter(sst_manager->allow_file_sizes[level] / per_kv_size, 0.01);
        auto oldest_immutable_entry = (*mem_manager).get_oldest_immutable();
        auto entries = oldest_immutable_entry.mem->traverse();  // ����immu_table�е�����
        if (entries.empty()) {
            wal.log("[Little_kv::Flush] Skip empty MemTable from " + oldest_immutable_entry.wal_filename);
            wal.remove_wal(oldest_immutable_entry.wal_filename);
            (*mem_manager).pop_oldest_immutable();
            continue;
        }

        for (const auto &kv : entries) {
            sst_out << kv.first << " " << kv.second << "\n";
            filter.add(kv.first);
            file_size += kv.first.size() + kv.second.size();
        }
        sst_out.close();

        if (!entries.empty()) {
            smallest_k = entries.front().first;
            largest_k = entries.back().first;
        }

        // ����Ԫ���ݲ�д�� Manifest
        cout << "[Little_kv::Flush()] : ���ڹ���Ԫ����fmd" << endl;
        FileMetaData fmd(level, next_sst_id, next_sst_name, smallest_k, largest_k, file_size, filter);
        sst_manager->addFile(fmd);

        wal.remove_wal(oldest_immutable_entry.wal_filename);  // �� wal_manifest ���Ƴ���ɾ���ļ�
        (*mem_manager).pop_oldest_immutable();  // �ӹ鵵����������Ƴ�

        // ����־
        wal.log("[Little_kv::Flush] Flushed " + to_string(entries.size()) + 
        " kv pairs from " + oldest_immutable_entry.wal_filename + 
        " to " + next_sst_name);
    }
    sst_manager->save();  // ����sst manifest
}


// �������� + ��ʱ����
void Little_kv::flush_background() {
    while (true) {
        std::unique_lock<std::mutex> flush_lock(flush_mutex);
        flush_cv.wait_for(flush_lock, std::chrono::seconds(3), [this] {
            return flush_needed || stop_flush;
        });

        if (stop_flush) break;
        if (flush_needed) {
            Flush();  // ˢ���߼�
            flush_needed = false;
        }
    }
}

// ������������ˢ�̣�ͨ������������������ָ��һ���ᴥ��
void Little_kv::maybe_trigger_flush() {
    {
        lock_guard<mutex> flush_lock(flush_mutex);
        flush_needed = true;
    }
    flush_cv.notify_one();
}


bool Little_kv::CompactLn(int target_level) {

    FileMetaData source_file = sst_manager->getEarliestFileByLevel(target_level - 1);

    // ��target_files��source_file�е����ݶ����ڴ棬�����ɾɵ��µ�˳������Ը��Ǿ�ֵ��������key����
    unordered_map<string, string> data_map;
    vector<pair<string, string>> merged_data;

    // �ҳ�key���У���������ص�target_files
    set<string> key_set = sst_manager->getKeySet(source_file);
    vector<FileMetaData> overlap_files = sst_manager->getRelativeSsts(target_level, key_set);

    // map�������͸��Ǿ�ֵ
    sst_manager->readFileToMap(overlap_files, data_map); // target_files�п���Ϊ�գ�������������
    sst_manager->readFileToMap(source_file, data_map);

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
    size_t expect_key_num = sst_manager->allow_file_sizes[target_level] / per_kv_size;

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

        if (current_chunk_size >= sst_manager->allow_file_sizes[target_level]) {  // �����ϲ�
            int id = sst_manager->getNextSstId(target_level);
            string filename = sst_manager->getNextSstName(target_level, id);
            string s_k = "z", l_k = "a";
            if ((!last_compact) && sst_manager->getKeyRange_contains_del(current_chunk, s_k,
                                                          l_k)) { // �м��ϲ�
                BloomFilter bf(expect_key_num, 0.01);
                FileMetaData fmd(target_level, id, filename, s_k, l_k, 0, bf);
                // ��current_chunk��key��ӵ�fmd��filter
                for (const auto& [key, value] : current_chunk) {
                    fmd.file_size += (key.size() + value.size());
                    fmd.filter.add(key);
                }
                sst_manager->addFile(fmd);
                // write data from map to sst_file
                sst_manager->writeMapToSst(current_chunk, filename);
                target_fnames += filename + " ";
            } else if (last_compact &&
                       sst_manager->getKeyRange_ignore_del(current_chunk, s_k,
                                              l_k)) { // ��ײ�ϲ�
                BloomFilter bf(expect_key_num, 0.01);
                FileMetaData fmd(target_level, id, filename, s_k, l_k, 0, bf);
                // ��current_chunk��key��ӵ�fmd��filter
                for (const auto& [key, value] : current_chunk) {
                    fmd.file_size += (key.size() + value.size());
                    fmd.filter.add(key);
                }
                sst_manager->addFile(fmd);
                // write data from map to sst_file
                sst_manager->writeMapToSst(current_chunk, filename);
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
        int id = sst_manager->getNextSstId(target_level);
        string filename = sst_manager->getNextSstName(target_level, id);
        string s_k = "z", l_k = "a";
        if ((!last_compact) &&
            sst_manager->getKeyRange_contains_del(current_chunk, s_k, l_k)) { // �м��ϲ�
            BloomFilter bf(expect_key_num, 0.01);
            FileMetaData fmd(target_level, id, filename, s_k, l_k, 0, bf);
            // ��current_chunk��key��ӵ�fmd��filter
            for (const auto& [key, value] : current_chunk) {
                fmd.file_size += (key.size() + value.size());
                fmd.filter.add(key);
            }
            sst_manager->addFile(fmd);
            // write data from map to sst_file
            sst_manager->writeMapToSst(current_chunk, filename);
            target_fnames += filename + " ";
        } else if (last_compact && sst_manager->getKeyRange_ignore_del(current_chunk, s_k, l_k)) { // ��ײ�ϲ�
            BloomFilter bf(expect_key_num, 0.01);
            FileMetaData fmd(target_level, id, filename, s_k, l_k, 0, bf);
            for (const auto& [key, value] : current_chunk) {
                fmd.file_size += (key.size() + value.size());
                fmd.filter.add(key);
            }
            sst_manager->addFile(fmd);
            // write data from map to sst_file
            sst_manager->writeMapToSst(current_chunk, filename);
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
    sst_manager->save();

    return true;
}

// ʵ�ʵĺϲ����� -- �����ϲ�
// ע�⣺����ǰ�����ȳ��� manifest ��д����
bool Little_kv::CompactLnBat(int target_level) {

    vector<FileMetaData> source_files = sst_manager->getFilesByLevel(target_level - 1);

    // ��target_files��source_files�е����ݶ����ڴ棬�����ɾɵ��µ�˳������Ը��Ǿ�ֵ��������key����
    unordered_map<string, string> data_map;
    vector<pair<string, string>> merged_data;

    // �ҳ�key���У���������ص�target_files
    set<string> key_set = sst_manager->getKeySet(source_files);
    vector<FileMetaData> overlap_files = sst_manager->getRelativeSsts(target_level, key_set);

    // map�������͸��Ǿ�ֵ
    sst_manager->readFileToMap(overlap_files, data_map); // target_files�п���Ϊ�գ�������������
    sst_manager->readFileToMap(source_files, data_map);
    
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
    size_t expect_key_num = sst_manager->allow_file_sizes[target_level] / per_kv_size;

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

        if (current_chunk_size >= sst_manager->allow_file_sizes[target_level]) {  // �����ϲ�
            int id = sst_manager->getNextSstId(target_level);
            string filename = sst_manager->getNextSstName(target_level, id);
            string s_k = "z", l_k = "a";
            if ((!last_compact) && sst_manager->getKeyRange_contains_del(current_chunk, s_k,
                                                          l_k)) { // �м��ϲ�
                BloomFilter bf(expect_key_num, 0.01);
                FileMetaData fmd(target_level, id, filename, s_k, l_k, 0, bf);
                // ��current_chunk��key��ӵ�fmd��filter
                for (const auto& [key, value] : current_chunk) {
                    fmd.file_size += (key.size() + value.size());
                    fmd.filter.add(key);
                }
                sst_manager->addFile(fmd);
                // write data from map to sst_file
                sst_manager->writeMapToSst(current_chunk, filename);
            } else if (last_compact &&
                       sst_manager->getKeyRange_ignore_del(current_chunk, s_k,
                                              l_k)) { // ��ײ�ϲ�
                BloomFilter bf(expect_key_num, 0.01);
                FileMetaData fmd(target_level, id, filename, s_k, l_k, 0, bf);
                // ��current_chunk��key��ӵ�fmd��filter
                for (const auto& [key, value] : current_chunk) {
                    fmd.file_size += (key.size() + value.size());
                    fmd.filter.add(key);
                }
                sst_manager->addFile(fmd);
                // write data from map to sst_file
                sst_manager->writeMapToSst(current_chunk, filename);
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
        int id = sst_manager->getNextSstId(target_level);
        string filename = sst_manager->getNextSstName(target_level, id);
        string s_k = "z", l_k = "a";
        if ((!last_compact) &&
            sst_manager->getKeyRange_contains_del(current_chunk, s_k, l_k)) { // �м��ϲ�
            BloomFilter bf(expect_key_num, 0.01);
            FileMetaData fmd(target_level, id, filename, s_k, l_k, 0, bf);
            // ��current_chunk��key��ӵ�fmd��filter
            for (const auto& [key, value] : current_chunk) {
                fmd.file_size += (key.size() + value.size());
                fmd.filter.add(key);
            }
            sst_manager->addFile(fmd);
            // write data from map to sst_file
            sst_manager->writeMapToSst(current_chunk, filename);
        } else if (last_compact && sst_manager->getKeyRange_ignore_del(current_chunk, s_k, l_k)) { // ��ײ�ϲ�
            BloomFilter bf(expect_key_num, 0.01);
            FileMetaData fmd(target_level, id, filename, s_k, l_k, 0, bf);
            for (const auto& [key, value] : current_chunk) {
                fmd.file_size += (key.size() + value.size());
                fmd.filter.add(key);
            }
            sst_manager->addFile(fmd);
            // write data from map to sst_file
            sst_manager->writeMapToSst(current_chunk, filename);
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

void Little_kv::detector_loop() {
    while (!stop_detector) {
        detect_and_schedule();
        std::this_thread::sleep_for(std::chrono::seconds(5));
    }
}

// ʵ�����ڴ˴��ж��Ƿ���Ҫ�ϲ�
void Little_kv::detect_and_schedule() {
    // ѹʵ������L0���ļ����㣬�����㰴������С�㡣
    // ѹʵ����С��1���ϲ���ѹʵ����Խ�����ȼ�Խ�ߣ�ѹʵ������ͬ��Խ�����ڴ����ȼ�Խ�ߡ�
    // L0��ѹʵ��������1ʱ��L0���������Ⱥϲ���
    double compact_score = 0.0;  
    int target_level = 0;

    {  // ���ö�����ֻ��Ԫ����
        shared_lock<std::shared_mutex> manifest_read_lock(rw_manifest_mutex);

        // L0��ѹʵ����
        compact_score = (double)sst_manager->getFilesByLevel(0).size() / (double)compact_threshold;
        if (compact_score >= 1) {
            target_level = 1;
        } else {
            double tmp_max_score = 0;
            for (int level=1;level <= 2; level++) {
                compact_score = (double)sst_manager->getLevelSize(level) / (double)(sst_manager->allow_file_sizes[level]*sst_manager->levels_multiple);
                if (compact_score > tmp_max_score) {
                    tmp_max_score = compact_score;
                    target_level = level + 1;
                }
            }
            compact_score = tmp_max_score;
        }
    } // ����������������ͷ�

    if (compact_score < 1) {  // ����Ҫ�ϲ�
        return;
    }
    
    {  // ��Ҫ�ϲ������濪ʼ��Ҫ�� manifest ��д��������Ҫд��
        unique_lock<std::shared_mutex> manifest_write_lock(rw_manifest_mutex);
        compaction_mgr.enqueue_task({target_level});
    }
}

// put --- thread safe
bool Little_kv::put(const std::string &key, const std::string &value) {

    lock_guard<mutex> put_lock(edit_mutex);

    wal.writeWAL("PUT", key, value);

    bool ret = (*active_memtable).insert(key, value);

    if ((*active_memtable).GetMemTableSize() >= memtable_size_threshold) {
        wal.log("[Little_kv::put] : size over, gona to add to flush queue");
        // ��ǰwal�鵵���һ���µ�wal������ǰwal����ˢ�̶���
        string wal_name = wal.rotate();  // �鵵���ؿ�wal
        wal.log("[Little_kv::put] : rotate over");
        shared_ptr<MemTable> immutable_memtable = active_memtable;
        active_memtable = make_shared<MemTable>();
        (*mem_manager).add_immutable(immutable_memtable, wal_name);
        wal.log("[Little_kv::put] : add_immutable over");
        maybe_trigger_flush();  // ������������ˢ��
    }

    return ret;
}

bool Little_kv::putbat(const int num) {
    lock_guard<mutex> put_lock(edit_mutex);
    unordered_map<string, string> put_buff;
    for (int i=0;i<num;i++) {
        cout << "[" << (i+1) << "/" << num <<"]-> ";
        string line;
        if (!getline(cin, line)) break;
        line.erase(0, line.find_first_not_of(" \t"));
        line.erase(line.find_last_not_of(" \t") + 1);
        if (line.empty()) continue; // ����������
        istringstream iss(line);
        string cmd;
        iss >> cmd;
        if (cmd == "put") {
            string key;
            iss >> key;
            string value;
            getline(iss >> ws, value); // ��ȡʣ�ಿ����Ϊvalue������ո�
            if (key.empty() || value.empty()) continue;
            put_buff[key] = value;
        } else if (cmd == "del") {
            string key;
            iss >> key;
            if (key.empty()) continue;
            put_buff[key] = TOMBSTONE;
        } else {
            wal.log("[Little_kv::putbat] : op Error");
            return false;
        }
    }
    bool ret = wal.writeBatWAL(put_buff);
    // ��put_buff�е�����д��wal
    if (ret) {
        lock_guard<mutex> put_lock(edit_mutex);
        for (const auto& [key, value] : put_buff) {
            ret &= (*active_memtable).insert(key, value);
        }
        if ((*active_memtable).GetMemTableSize() >= memtable_size_threshold) {
            // ��ǰwal�鵵���һ���µ�wal������ǰwal����ˢ�̶���
            string wal_name = wal.rotate();  // �鵵���ؿ�wal
            shared_ptr<MemTable> immutable_memtable = active_memtable;
            active_memtable = make_shared<MemTable>();
            (*mem_manager).add_immutable(immutable_memtable, wal_name);
            maybe_trigger_flush();
        }
    }
    return ret;
}

bool Little_kv::del(const std::string &key) {
    return put(key, TOMBSTONE); // ʵ����������put������valueΪɾ�����
}

string Little_kv::get(const string &key) {
    // order : active_memtable immutable_memtable  L0  L1

    // �Ȳ� active_memtable
    optional<string> ret = (*active_memtable).search(key);
    if (ret.has_value()) { // means exists the key
        if (ret == TOMBSTONE)
            return "NOT_FOUND"; // deled tag
        return ret.value();             // find the value
    }

    // ��Ϊ��immu_mem�����в���
    for (const auto& immu_mem : (*mem_manager).get_immutable_list_desc()) {
        ret = (*immu_mem).search(key);
        if (ret.has_value()) { // means exists the key
            if (ret == TOMBSTONE)
                return "NOT_FOUND"; // deled tag
            return ret.value();             // find the value
        }
    }

    // ���level�ļ����Ӷ��㵽�ײ㣺L0 -> L3�������µ���
    shared_lock<shared_mutex> manifest_lock(rw_manifest_mutex);

    // search from L0 files
    vector<FileMetaData> l0_files = sst_manager->getFilesByLevel(0);
    // L0�Ƚ����⣬��ͬ��L0��key�����ظ������Ҫ����id�ɴ�С��˳����ң��Է����������µ��ɵ�˳��
    // ��L0��valueû�о���ѹ������ֱ�ӷ���
    sort(l0_files.begin(), l0_files.end(),
         [](const FileMetaData &f1, const FileMetaData &f2) {
             return f1.id > f2.id;
         });
    for (const auto &file : l0_files) {
        // ������λkey��Χ
        if ((key < file.smallest_key) || (key > file.largest_key)) {
            wal.log("[Little_kv::get L0] : key ��Χ��̭��" + key + ", smallest_key:" + file.smallest_key + ", largest_key:" + file.largest_key);
            continue;
        }
        // ʹ�ò�¡�������ٴ�ɸ��Ŀ��sst�ļ��Ƿ������key
        if (!file.filter.contains(key)) {
            // cout << "Bloom Pass : " << file.filename << endl;
            wal.log("[get " + key + "] : Bloom Pass : " + file.filename);
            continue;
        }
        wal.log("����ʹ��"+ file.filename +"���ң�" + key);
        // ͨ���������������ȵ�ɸ��󣬲Ŷ�ȡ�����ļ�
        string full_sst_path = sst_dir_path + file.filename;
        auto fp = fd_cache.get(full_sst_path);
        if (fp && fp->is_open()) {
            string line;
            wal.log(file.filename + "���򿪲��ң�" + key);
            while (getline(*fp, line)) {
                istringstream iss(line);
                string k, v;
                if (iss >> k) {
                    wal.log("key: " + key);
                    getline(iss, v);
                    if (key == k) {
                        v = v.empty() ? "" : v.substr(1);
                        if (v == TOMBSTONE) return "NOT_FOUND";
                        return v;
                    }
                }
            }
        } else {
            wal.log("ʹ��fd_cache���ļ�ʧ�ܣ�");
        }
    }
    // search from L1��L2��L3 files
    // L1֮��Ĳ��value��������ѹ������Ҫ��searchFromSst�н�ѹ����Ƚ�
    ret = "NOT_FOUND";
    for (int level=1;level <= 3;level++) {
        ret = sst_manager->searchFromSst(level, key, fd_cache);
        if (ret != "NOT_FOUND") break;
    }

    if (ret == TOMBSTONE) ret = "NOT_FOUND";
    return ret.value();
}

void Little_kv::clear() {
    // ��¼��־
    wal.log("clear all the data\n");
    // ����ڴ�������
    {
        lock_guard<mutex> put_lock(edit_mutex);
        active_memtable.reset(new MemTable);
    }
    // ���wal
    wal.clearWAL();
    // ɾ��sst�ļ�
    {
        unique_lock<std::shared_mutex> manifest_write_lock(rw_manifest_mutex);
        vector<FileMetaData> fmdList = sst_manager->getFileList();
        RemoveSstFiles(fmdList);
    }

    fd_cache.clear();
}








// =================================== �����Ժ��� ======================================

bool Little_kv::RemoveSstFiles(FileMetaData &file) {
    remove((sst_dir_path + file.filename).c_str());
    sst_manager->removeFile(file.filename);
    sst_manager->save();
    return true;
}

bool Little_kv::RemoveSstFiles(vector<FileMetaData> &files) {
    for (const auto &file : files) {
        remove((sst_dir_path + file.filename).c_str());
        sst_manager->removeFile(file.filename);
    }
    sst_manager->save();
    return true;
}

void Little_kv::refresh_log() {
    wal.flush_log();
}