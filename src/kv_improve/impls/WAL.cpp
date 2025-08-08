#include "../headers/WAL.h"

using namespace std;

WAL::WAL(const string& wal_dir, const string& log_dir, const size_t simp_thd) {
    wal_file_dir = wal_dir;
    log_file_dir = log_dir;
    wal_file_path = wal_dir + "wal.txt";
    log_file_path = log_dir + "log.txt";

    simplify_threshold = simp_thd;

    count = 0;

    wal_out.open(wal_file_path, ios::app);
    if (!wal_out.is_open()) {
        cerr << "Failed to open WAL file!" << endl;
    }

    // 先清空上次运行的log
    ofstream log_out(log_file_path, ios::trunc);
    if (!log_out.is_open()) {
        cerr << ("[WAL::clearWAL] : Failed to open " + log_file_path);
    }
    log_out.close();
    log_file_stream.open(log_file_path, ios::app);
    if (!log_file_stream.is_open()) {
        cerr << "[WAL::WAL] : Failed to open log file " << log_file_path << endl;
    }

    // cout << "---------- WAL START ----------" << endl;
    log("[WAL::WAL] : ---------- WAL START ----------");
}

WAL::~WAL() {
    if (wal_out.is_open()) wal_out.close();
    flush_log();
    if (log_file_stream.is_open()) {
        log_file_stream.close();
    }
    log("[WAL::WAL] : ---------- WAL OVER ----------");
}

void WAL::writeWAL(const string &op, const string &key, const string &value) {
    // 加锁
    lock_guard<mutex> wal_lock(wal_mutex);

    if (!wal_out.is_open()) {
        // cerr << "Failed to open " << wal_file_path << endl;
        log("[WAL::writeWAL] : Failed to open " + wal_file_path);
        return;
    }
    wal_out << op << " " << key << " ";
    if (op == "PUT")
        wal_out << value;
    wal_out << "\n";
    wal_out.flush();  // 确保持久化
    count ++;
    if (count >= simplify_threshold) simplify();
}

void WAL::clearWAL() {
    // 先关
    if (wal_out.is_open()) {
        wal_out.flush();
        wal_out.close();
    }
    // 加锁
    lock_guard<mutex> wal_lock(wal_mutex);

    ofstream ofs(wal_file_path, ios::trunc);
    if (!ofs.is_open()) {
        // cerr << "Failed to open " << wal_file_path << endl;
        log("[WAL::clearWAL] : Failed to open " + wal_file_path);
    }
    ofs.close();
    count = 0;
    // 再重开
    wal_out.open(wal_file_path, ios::app);
    if (!wal_out.is_open()) {
        log("[WAL::clearWAL] : Failed to reopen " + wal_file_path);
    }
}

void WAL::recover_data(shared_ptr<MemTable>& active_memtable) {
    // 加锁
    lock_guard<mutex> wal_lock(wal_mutex);

    string line;
    ifstream wal_in(wal_file_path);
    if (!wal_in.is_open()) {
        log("[WAL::recover_data] : Failed to open " + wal_file_path);
        return;
    }

    while(getline(wal_in, line)) {
        istringstream iss(line);
        string op, key, value;
        if (iss >> op >> key) {
            getline(iss, value);
            if (!value.empty() && value[0] == ' ') {
                value.erase(0, 1);
            }
            if (op == "PUT") {
                (*active_memtable).insert(key, value);
            } else if (op == "DEL") {
                (*active_memtable).insert(key, TOMBSTONE);
            } else {
                log("UNknown op: " + op);
            }
        }
        count++;
    }
    wal_in.close();
}

string getCurrentTime() {
    auto now = std::chrono::system_clock::now();
    std::time_t now_time = std::chrono::system_clock::to_time_t(now);
    
    std::tm tm = *std::localtime(&now_time);
    std::ostringstream oss;
    oss << std::put_time(&tm, "%Y-%m-%d %H:%M:%S");
    return oss.str();
}

string getTimeStr() {
    auto now = std::chrono::system_clock::now();
    std::time_t now_time = std::chrono::system_clock::to_time_t(now);
    
    std::tm tm = *std::localtime(&now_time);
    std::ostringstream oss;
    oss << std::put_time(&tm, "%Y_%m_%d_%H_%M_%S");
    return oss.str();
}

void WAL::log(const string& msg) {
    lock_guard<mutex> log_lock(log_mutex);

    log_buffer << getCurrentTime() << "    " << msg << "\n";
    log_line_count++;
    if(log_line_count >= log_flush_lines) {
        flush_log();
    }
}

void WAL::flush_log() {
    if (!log_file_stream.is_open()) return;

    log_file_stream << log_buffer.str();
    log_file_stream.flush();
    log_buffer.str("");  // 清空缓冲
    log_buffer.clear();
    log_line_count = 0;
}

void WAL::archive() {
    filesystem::rename(wal_file_path, wal_file_dir+getTimeStr()+"_wal.txt");   
}

void WAL::simplify() {
    // 关闭持久打开的文件
    wal_out.flush();
    wal_out.close();

    unordered_map<string, string> tmp_map;

    string line;
    ifstream wal_in(wal_file_path);
    if (!wal_in.is_open()) {
        log("[WAL::simplify] : Failed to open " + wal_file_path);
        return;
    }
    while (getline(wal_in, line)) {
        istringstream iss(line);
        string op, key, value;

        if (iss >> op >> key) {
            // 获取剩余部分，去掉前导空格
            getline(iss, value);
            if (!value.empty() && value[0] == ' ') {
                value.erase(0, 1);
            }

            log("op:" + op + " ,key:" + key + " ,value:" + value);

            if (op == "PUT") {
                tmp_map[key] = value;

            } else if (op == "DEL") {
                tmp_map[key] = TOMBSTONE;

            } else {
                log("[WAL::simplify] : UNknown op: " + op);
            }
        }
    }
    wal_in.close();


    // ?map??????????????wal.tmp??????wal.txt
    ofstream tmp_out(wal_file_dir + "wal.tmp");
    size_t tmp_count = 0;
    if (!tmp_out.is_open()) {
        log("[WAL::simplify] : Failed to open " + wal_file_dir + "wal.tmp");
        return;
    }
    for(const auto &[key, value] : tmp_map) {
        tmp_out << "PUT" << " " << key << " " << value << "\n";
        tmp_count ++;
    }
    tmp_out.close();

    // ??(??)
    // if (filesystem::exists(wal_file_path)) {
    //     filesystem::remove(wal_file_path);
    // }
    error_code ec;
    filesystem::remove(wal_file_path, ec);
    filesystem::rename(wal_file_dir + "wal.tmp", wal_file_path);
    count = tmp_count;

    // 重新打开
    wal_out.open(wal_file_path, ios::app);
    if (!wal_out.is_open()) {
        cerr << "Failed to reopen WAL file!" << endl;
    }
}