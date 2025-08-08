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
    immutable_memtable = nullptr;
    manifest = make_shared<ManifestManager>(sst_dir_path, wal, max_file_size, 4, per_kv_size);
    Init_data();  // recover last data from wal

    (*manifest).load(); // load sst files to manifest
    // (*manifest).startBackgroundSave();  // 后台线程定时持久化manifest

    flush_thread = thread(&Little_kv::flush_background, this);
    
    // compact 相关
    compaction_mgr.set_compact_executor([this](CompactionTask task) {
        CompactLn(task.target_level);  // 执行compact任务
    });
    compaction_mgr.start();  // 初始化条件变量并启动compact工作线程
    detector_thread = thread(&Little_kv::detector_loop, this);  // 启动检测线程
    
    // cout << "========= MemTable init Success ! =========" << endl;
    wal.log("[Little_kv] : ========= MemTable init Success ! =========");
}

Little_kv::~Little_kv() {
    // 终止flush
    {
        lock_guard<mutex> flush_lock(flush_mutex);
        stop_flush = true;
    }
    flush_cv.notify_one();  // 通知（唤醒）一个正在wait()的线程，只发信号，不等待
    flush_thread.join();  // 主线程在这里卡住，直到flush_thread的函数体结束才继续往下执行

    // 终止compact
    stop_detector = true;
    if (detector_thread.joinable()) detector_thread.join();
    compaction_mgr.stop();  // 终止compact工作线程

    // 持久化manifest
    {
        unique_lock<shared_mutex> rw_manifest_lock(rw_manifest_mutex);
        (*manifest).save(); // // write sst files information to manifest file
        // (*manifest).stopBackgroundSave();  // 关闭后台执行定时任务的线程
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
    int next_sst_id = (*manifest).getNextSstId(0);
    string next_sst_name = (*manifest).getNextSstName(0, next_sst_id);
    string smallest_k = "a";
    string largest_k = "a";
    size_t file_size = 0;
    BloomFilter filter((*manifest).allow_file_sizes[0] / per_kv_size, 0.01);  // 预计key数量，容忍误报率（假设每个key-value共8字节）
    FileMetaData fmd(level, next_sst_id, next_sst_name, smallest_k, largest_k, file_size,filter);

    string full_sst_name = sst_dir_path + next_sst_name;
    ofstream sst_out(full_sst_name, ios::app);
    if (!sst_out.is_open()) {
        cerr << "Failed to open " << full_sst_name << endl;
        return;
    }

    // immutable_memtable is ordered
    // 将key-value写入L0级sst文件，顺便将key都添加到 fmd的filter
    auto entries = (*immutable_memtable).traverse();
    for (const auto &kv : entries) {
        sst_out << kv.first << " " << kv.second << "\n";
        fmd.file_size += (kv.first.size() + kv.second.size());
        fmd.filter.add(kv.first);
    }
    sst_out.close();

    smallest_k = entries[0].first;
    largest_k = entries[entries.size() - 1].first;
    fmd.smallest_key = smallest_k;
    fmd.largest_key = largest_k;
    (*manifest).addFile(fmd);
    (*manifest).save();

    wal.clearWAL();  // 直接清除wal
    // wal.archive();  // 将wal归档
    
    immutable_memtable.reset();

    wal.log("[Little_kv::Flush] : flushed " + next_sst_name);
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
    }
}

void Little_kv::maybe_trigger_flush() {
    {
        lock_guard<mutex> flush_lock(flush_mutex);
        flush_needed = true;
    }
    flush_cv.notify_one();
}


bool Little_kv::CompactLn(int target_level) {

    FileMetaData source_file = (*manifest).getEarliestFileByLevel(target_level - 1);

    // 将target_files、source_file中的数据读入内存，按照由旧到新的顺序读入以覆盖旧值，并按照key排序
    unordered_map<string, string> data_map;
    vector<pair<string, string>> merged_data;

    // 找出key序列，并查找相关的target_files
    set<string> key_set = (*manifest).getKeySet(source_file);
    vector<FileMetaData> overlap_files = (*manifest).getRelativeSsts(target_level, key_set);

    // map方便插入和覆盖旧值
    (*manifest).readFileToMap(overlap_files, data_map); // target_files中可能为空，更可能有数据
    (*manifest).readFileToMap(source_file, data_map);

    // 将map数据存入 vector<pair<string, string>>，以方便对key排序
    for (const auto &[k, v] : data_map) {
        merged_data.push_back(make_pair(k, v));
    }
    // 对set中的数据按key排序，从小到大
    sort(merged_data.begin(), merged_data.end(),
         [](const pair<string, string> &p1, const pair<string, string> &p2) {
             return p1.first <= p2.first;
         });

    // 根据目标文件level，确定预计key的数量
    size_t expect_key_num = (*manifest).allow_file_sizes[target_level] / per_kv_size;

    bool last_compact = (target_level == 3);  // 是否是向最底层的合并

    // 根据target_file_size划分merged_data并持久化到target_files
    size_t current_chunk_size = 0;
    unordered_map<string, string>
        current_chunk; // 临时存储每个target_file的数据
    vector<FileMetaData> new_target_files;

    string target_fnames = "{";

    for (int i = 0; i < merged_data.size(); i++) {
        // 仅在最底层处理删除标记
        if (last_compact && (merged_data[i].second == TOMBSTONE))
            continue;

        current_chunk[merged_data[i].first] = merged_data[i].second;
        current_chunk_size += (merged_data[i].first.size() + merged_data[i].second.size());

        if (current_chunk_size >= (*manifest).allow_file_sizes[target_level]) {  // 触发合并
            int id = (*manifest).getNextSstId(target_level);
            string filename = (*manifest).getNextSstName(target_level, id);
            string s_k = "z", l_k = "a";
            if ((!last_compact) && (*manifest).getKeyRange_contains_del(current_chunk, s_k,
                                                          l_k)) { // 中间层合并
                BloomFilter bf(expect_key_num, 0.01);
                FileMetaData fmd(target_level, id, filename, s_k, l_k, 0, bf);
                // 将current_chunk的key添加到fmd的filter
                for (const auto& [key, value] : current_chunk) {
                    fmd.file_size += (key.size() + value.size());
                    fmd.filter.add(key);
                }
                (*manifest).addFile(fmd);
                // write data from map to sst_file
                (*manifest).writeMapToSst(current_chunk, filename);
                target_fnames += filename + " ";
            } else if (last_compact &&
                       (*manifest).getKeyRange_ignore_del(current_chunk, s_k,
                                              l_k)) { // 最底层合并
                BloomFilter bf(expect_key_num, 0.01);
                FileMetaData fmd(target_level, id, filename, s_k, l_k, 0, bf);
                // 将current_chunk的key添加到fmd的filter
                for (const auto& [key, value] : current_chunk) {
                    fmd.file_size += (key.size() + value.size());
                    fmd.filter.add(key);
                }
                (*manifest).addFile(fmd);
                // write data from map to sst_file
                (*manifest).writeMapToSst(current_chunk, filename);
                target_fnames += filename + " ";
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
        int id = (*manifest).getNextSstId(target_level);
        string filename = (*manifest).getNextSstName(target_level, id);
        string s_k = "z", l_k = "a";
        if ((!last_compact) &&
            (*manifest).getKeyRange_contains_del(current_chunk, s_k, l_k)) { // 中间层合并
            BloomFilter bf(expect_key_num, 0.01);
            FileMetaData fmd(target_level, id, filename, s_k, l_k, 0, bf);
            // 将current_chunk的key添加到fmd的filter
            for (const auto& [key, value] : current_chunk) {
                fmd.file_size += (key.size() + value.size());
                fmd.filter.add(key);
            }
            (*manifest).addFile(fmd);
            // write data from map to sst_file
            (*manifest).writeMapToSst(current_chunk, filename);
            target_fnames += filename + " ";
        } else if (last_compact && (*manifest).getKeyRange_ignore_del(current_chunk, s_k, l_k)) { // 最底层合并
            BloomFilter bf(expect_key_num, 0.01);
            FileMetaData fmd(target_level, id, filename, s_k, l_k, 0, bf);
            for (const auto& [key, value] : current_chunk) {
                fmd.file_size += (key.size() + value.size());
                fmd.filter.add(key);
            }
            (*manifest).addFile(fmd);
            // write data from map to sst_file
            (*manifest).writeMapToSst(current_chunk, filename);
            target_fnames += filename + " ";
        } else {
            cerr << "合并出错！" << endl;
            return false;
        }
    }

    // 记录日志
    string msg = "[CompactLn] : compacting {" + source_file.filename + "} and {";
    for (auto& fmd : overlap_files) {
        msg += fmd.filename + " ";
    }
    msg += "} to be ";
    target_fnames += "}";
    msg += target_fnames;
    wal.log(msg);

    // 删除源数据文件和相关原target文件
    RemoveSstFiles(source_file);
    RemoveSstFiles(overlap_files);

    // 保险起见，持久化manifest
    (*manifest).save();

    return true;
}

// 实际的合并过程 -- 按批合并
// 注意：调用前必须先持有 manifest 的写锁！
bool Little_kv::CompactLnBat(int target_level) {

    vector<FileMetaData> source_files = (*manifest).getFilesByLevel(target_level - 1);

    // 将target_files、source_files中的数据读入内存，按照由旧到新的顺序读入以覆盖旧值，并按照key排序
    unordered_map<string, string> data_map;
    vector<pair<string, string>> merged_data;

    // 找出key序列，并查找相关的target_files
    set<string> key_set = (*manifest).getKeySet(source_files);
    vector<FileMetaData> overlap_files = (*manifest).getRelativeSsts(target_level, key_set);

    // map方便插入和覆盖旧值
    (*manifest).readFileToMap(overlap_files, data_map); // target_files中可能为空，更可能有数据
    (*manifest).readFileToMap(source_files, data_map);
    
    // 将map数据存入 vector<pair<string, string>>，以方便对key排序
    for (const auto &[k, v] : data_map) {
        merged_data.push_back(make_pair(k, v));
    }
    // 对set中的数据按key排序，从小到大
    sort(merged_data.begin(), merged_data.end(),
         [](const pair<string, string> &p1, const pair<string, string> &p2) {
             return p1.first <= p2.first;
         });

    // 根据目标文件level，确定预计key的数量
    size_t expect_key_num = (*manifest).allow_file_sizes[target_level] / per_kv_size;

    bool last_compact = (target_level == 3);  // 是否是向最底层的合并

    // 根据target_file_size划分merged_data并持久化到target_files
    size_t current_chunk_size = 0;
    unordered_map<string, string>
        current_chunk; // 临时存储每个target_file的数据
    vector<FileMetaData> new_target_files;

    for (int i = 0; i < merged_data.size(); i++) {
        // 仅在最底层处理删除标记
        if (last_compact && (merged_data[i].second == TOMBSTONE))
            continue;

        current_chunk[merged_data[i].first] = merged_data[i].second;
        current_chunk_size += (merged_data[i].first.size() + merged_data[i].second.size());

        if (current_chunk_size >= (*manifest).allow_file_sizes[target_level]) {  // 触发合并
            int id = (*manifest).getNextSstId(target_level);
            string filename = (*manifest).getNextSstName(target_level, id);
            string s_k = "z", l_k = "a";
            if ((!last_compact) && (*manifest).getKeyRange_contains_del(current_chunk, s_k,
                                                          l_k)) { // 中间层合并
                BloomFilter bf(expect_key_num, 0.01);
                FileMetaData fmd(target_level, id, filename, s_k, l_k, 0, bf);
                // 将current_chunk的key添加到fmd的filter
                for (const auto& [key, value] : current_chunk) {
                    fmd.file_size += (key.size() + value.size());
                    fmd.filter.add(key);
                }
                (*manifest).addFile(fmd);
                // write data from map to sst_file
                (*manifest).writeMapToSst(current_chunk, filename);
            } else if (last_compact &&
                       (*manifest).getKeyRange_ignore_del(current_chunk, s_k,
                                              l_k)) { // 最底层合并
                BloomFilter bf(expect_key_num, 0.01);
                FileMetaData fmd(target_level, id, filename, s_k, l_k, 0, bf);
                // 将current_chunk的key添加到fmd的filter
                for (const auto& [key, value] : current_chunk) {
                    fmd.file_size += (key.size() + value.size());
                    fmd.filter.add(key);
                }
                (*manifest).addFile(fmd);
                // write data from map to sst_file
                (*manifest).writeMapToSst(current_chunk, filename);
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
        int id = (*manifest).getNextSstId(target_level);
        string filename = (*manifest).getNextSstName(target_level, id);
        string s_k = "z", l_k = "a";
        if ((!last_compact) &&
            (*manifest).getKeyRange_contains_del(current_chunk, s_k, l_k)) { // 中间层合并
            BloomFilter bf(expect_key_num, 0.01);
            FileMetaData fmd(target_level, id, filename, s_k, l_k, 0, bf);
            // 将current_chunk的key添加到fmd的filter
            for (const auto& [key, value] : current_chunk) {
                fmd.file_size += (key.size() + value.size());
                fmd.filter.add(key);
            }
            (*manifest).addFile(fmd);
            // write data from map to sst_file
            (*manifest).writeMapToSst(current_chunk, filename);
        } else if (last_compact && (*manifest).getKeyRange_ignore_del(current_chunk, s_k, l_k)) { // 最底层合并
            BloomFilter bf(expect_key_num, 0.01);
            FileMetaData fmd(target_level, id, filename, s_k, l_k, 0, bf);
            for (const auto& [key, value] : current_chunk) {
                fmd.file_size += (key.size() + value.size());
                fmd.filter.add(key);
            }
            (*manifest).addFile(fmd);
            // write data from map to sst_file
            (*manifest).writeMapToSst(current_chunk, filename);
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

void Little_kv::detector_loop() {
    while (!stop_detector) {
        detect_and_schedule();
        std::this_thread::sleep_for(std::chrono::seconds(5));
    }
}

// 实际上在此处判断是否需要合并
void Little_kv::detect_and_schedule() {
    // 压实分数，L0按文件数算，其它层按容量大小算。
    // 压实分数小于1不合并。压实分数越高优先级越高；压实分数相同，越靠近内存优先级越高。
    // L0的压实分数大于1时，L0无条件优先合并。
    double compact_score = 0.0;  
    int target_level = 0;

    {  // 先用读锁，只读元数据
        shared_lock<std::shared_mutex> manifest_read_lock(rw_manifest_mutex);

        // L0的压实分数
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
    } // 读锁作用域结束，释放

    if (compact_score < 1) {  // 不需要合并
        return;
    }
    
    {  // 需要合并，下面开始需要对 manifest 做写操作，需要写锁
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
        maybe_trigger_flush();
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
        if (line.empty()) continue; // 跳过空输入
        istringstream iss(line);
        string cmd;
        iss >> cmd;
        if (cmd == "put") {
            string key;
            iss >> key;
            string value;
            getline(iss >> ws, value); // 读取剩余部分作为value（允许空格）
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
    // 将put_buff中的数据写入wal
    if (ret) {
        lock_guard<mutex> put_lock(edit_mutex);
        for (const auto& [key, value] : put_buff) {
            ret &= (*active_memtable).insert(key, value);
        }
        if ((*active_memtable).GetMemTableSize() >= memtable_size_threshold) {
            maybe_trigger_flush();
        }
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
    vector<FileMetaData> l0_files = (*manifest).getFilesByLevel(0);
    // L0比较特殊，不同的L0中key可能重复，因此要按照id由大到小的顺序查找，以符合数据由新到旧的顺序
    // 且L0的value没有经过压缩，可直接返回
    sort(l0_files.begin(), l0_files.end(),
         [](const FileMetaData &f1, const FileMetaData &f2) {
             return f1.id > f2.id;
         });
    for (const auto &file : l0_files) {
        // 初步定位key范围
        if ((key < file.smallest_key) || (key > file.largest_key)) {
            wal.log("[Little_kv::get L0] : key 范围淘汰：" + key + ", smallest_key:" + file.smallest_key + ", largest_key:" + file.largest_key);
            continue;
        }
        // 使用布隆过滤器再次筛查目标sst文件是否包含该key
        if (!file.filter.contains(key)) {
            // cout << "Bloom Pass : " << file.filename << endl;
            wal.log("[get " + key + "] : Bloom Pass : " + file.filename);
            continue;
        }
        wal.log("正在使用"+ file.filename +"查找：" + key);
        // 通过以上两道粗粒度的筛查后，才读取磁盘文件
        string full_sst_path = sst_dir_path + file.filename;
        auto fp = fd_cache.get(full_sst_path);
        if (fp && fp->is_open()) {
            string line;
            wal.log(file.filename + "被打开查找：" + key);
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
            wal.log("使用fd_cache打开文件失败！");
        }
    }
    // search from L1、L2、L3 files
    // L1之后的层的value都经过了压缩，需要在searchFromSst中解压缩后比较
    ret = "NOT_FOUND";
    for (int level=1;level <= 3;level++) {
        ret = (*manifest).searchFromSst(level, key, fd_cache);
        if (ret != "NOT_FOUND") break;
    }

    if (ret == TOMBSTONE) ret = "NOT_FOUND";
    return ret;
}

void Little_kv::clear() {
    // 记录日志
    wal.log("clear all the data\n");
    // 清除内存中数据
    {
        lock_guard<mutex> put_lock(edit_mutex);
        active_memtable.reset(new MemTable);
    }
    // 清除wal
    wal.clearWAL();
    // 删除sst文件
    {
        unique_lock<std::shared_mutex> manifest_write_lock(rw_manifest_mutex);
        vector<FileMetaData> fmdList = (*manifest).getFileList();
        RemoveSstFiles(fmdList);
    }

    fd_cache.clear();
}








// =================================== 工具性函数 ======================================

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

void Little_kv::refresh_log() {
    wal.flush_log();
}