#include "../headers/WAL.h"

string getCurrentTime() {
    auto now = chrono::system_clock::now();
    time_t now_time = chrono::system_clock::to_time_t(now);
    tm tm = *localtime(&now_time);
    ostringstream oss;
    oss << put_time(&tm, "%Y-%m-%d %H:%M:%S");
    return oss.str();
}

string getTimeStr() {
    auto now = chrono::system_clock::now();
    time_t now_time = chrono::system_clock::to_time_t(now);
    tm tm = *localtime(&now_time);
    ostringstream oss;
    oss << put_time(&tm, "%Y_%m_%d_%H_%M_%S");
    return oss.str();
}

WAL::WAL(const string& wal_dir, const string& log_dir, const size_t simp_thd)
    : wal_file_dir(wal_dir), log_file_dir(log_dir), simplify_threshold(simp_thd), count(0) {

    wal_file_path = wal_dir + "wal.txt";
    log_file_path = log_dir + "log.txt";

    wal_out.open(wal_file_path, ios::app);
    if (!wal_out.is_open()) {
        cerr << "Failed to open WAL file!" << endl;
    }

    ofstream log_out(log_file_path, ios::trunc);
    log_out.close();
    log_file_stream.open(log_file_path, ios::app);
    if (!log_file_stream.is_open()) {
        cerr << "[WAL::WAL] : Failed to open log file " << log_file_path << endl;
    }

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

uint32_t WAL::computeCRC(const string& data) {
    return crc32(0, reinterpret_cast<const Bytef*>(data.data()), data.size());
}


void WAL::writeWAL(const string &op, const string &key, const string &value) {
    lock_guard<mutex> wal_lock(wal_mutex);
    if (!wal_out.is_open()) {
        log("[WAL::writeWAL] : Failed to open " + wal_file_path);
        return;
    }

    stringstream record;
    record << op << " " << key;
    if (op == "PUT") record << " " << value;

    string data = record.str();
    uint32_t crc = computeCRC(data);

    wal_out << crc << " " << data << "\n";
    wal_out.flush();

    count++;
    if (count >= simplify_threshold) simplify();
}

bool WAL::writeBatWAL(const unordered_map<string, string>& put_buff) {
    lock_guard<mutex> wal_lock(wal_mutex);
    if (!wal_out.is_open()) {
        log("[WAL::writeBatWAL] : Failed to open " + wal_file_path);
        return false;
    }
    ostringstream oss;  // 一次拼好
    for (const auto& [key, value] : put_buff) {
        string data = "PUT " + key + " " + value;
        uint32_t crc = computeCRC(data);
        oss << crc << " " << data << "\n";
    }

    wal_out << oss.str();  // 一次性写入
    wal_out.flush();  // 确保持久化
    count += put_buff.size();
    
    return true;
}


void WAL::clearWAL() {
    if (wal_out.is_open()) {
        wal_out.flush();
        wal_out.close();
    }

    lock_guard<mutex> wal_lock(wal_mutex);

    ofstream ofs(wal_file_path, ios::trunc);
    ofs.close();
    count = 0;

    wal_out.open(wal_file_path, ios::app);
    if (!wal_out.is_open()) {
        log("[WAL::clearWAL] : Failed to reopen " + wal_file_path);
    }
}

void WAL::recover_data(shared_ptr<MemTable>& active_memtable) {
    lock_guard<mutex> wal_lock(wal_mutex);

    ifstream wal_in(wal_file_path);
    if (!wal_in.is_open()) {
        log("[WAL::recover_data] : Failed to open " + wal_file_path);
        return;
    }

    string line;
    while (getline(wal_in, line)) {
        istringstream iss(line);
        uint32_t stored_crc;
        string op, key, value;

        if (!(iss >> stored_crc >> op >> key)) {
            log("[WAL::recover_data] : Invalid format: " + line);
            continue;
        }

        getline(iss, value);
        if (!value.empty() && value[0] == ' ') value.erase(0, 1);
        string full_data = op + " " + key + (op == "PUT" ? " " + value : "");
        uint32_t calc_crc = computeCRC(full_data);

        if (stored_crc != calc_crc) {
            log("[WAL::recover_data] : CRC MISMATCH: " + line);
            continue;
        }

        if (op == "PUT") {
            active_memtable->insert(key, value);
        } else if (op == "DEL") {
            active_memtable->insert(key, TOMBSTONE);
        } else {
            log("[WAL::recover_data] : Unknown op: " + op);
        }

        count++;
    }
    wal_in.close();
}

void WAL::simplify() {
    wal_out.flush();
    wal_out.close();

    unordered_map<string, string> tmp_map;
    ifstream wal_in(wal_file_path);
    if (!wal_in.is_open()) {
        log("[WAL::simplify] : Failed to open " + wal_file_path);
        return;
    }

    string line;
    while (getline(wal_in, line)) {
        istringstream iss(line);
        uint32_t stored_crc;
        string op, key, value;

        if (!(iss >> stored_crc >> op >> key)) continue;

        getline(iss, value);
        if (!value.empty() && value[0] == ' ') value.erase(0, 1);
        string full_data = op + " " + key + (op == "PUT" ? " " + value : "");
        uint32_t calc_crc = computeCRC(full_data);

        if (stored_crc != calc_crc) continue;

        tmp_map[key] = (op == "DEL") ? TOMBSTONE : value;
    }
    wal_in.close();

    string tmp_path = wal_file_dir + "wal.tmp";
    ofstream tmp_out(tmp_path);
    if (!tmp_out.is_open()) {
        log("[WAL::simplify] : Failed to open " + tmp_path);
        return;
    }

    size_t tmp_count = 0;
    for (const auto& [key, value] : tmp_map) {
        string data = "PUT " + key + " " + value;
        uint32_t crc = computeCRC(data);
        tmp_out << crc << " " << data << "\n";
        tmp_count++;
    }
    tmp_out.close();

    error_code ec;
    filesystem::remove(wal_file_path, ec);
    filesystem::rename(tmp_path, wal_file_path);

    count = tmp_count;
    wal_out.open(wal_file_path, ios::app);
    if (!wal_out.is_open()) {
        cerr << "Failed to reopen WAL file!" << endl;
    }
}

void WAL::archive() {
    filesystem::rename(wal_file_path, wal_file_dir + getTimeStr() + "_wal.txt");
}

string WAL::build_line_with_crc(const string& op, const string& key, const string& value) {
    string raw = op + " " + key + " " + value;
    uLong crc = crc32(0L, Z_NULL, 0);
    crc = crc32(crc, reinterpret_cast<const Bytef*>(raw.data()), raw.size());
    return raw + " " + to_string(crc);
}

bool WAL::parse_line_with_crc(const string& line, string& op, string& key, string& value) {
    istringstream iss(line);
    string data, crc_str;
    if (!(iss >> op >> key)) return false;

    getline(iss, value, ' ');
    getline(iss, crc_str);

    string raw = op + " " + key + " " + value;
    uLong actual_crc = crc32(0L, Z_NULL, 0);
    actual_crc = crc32(actual_crc, reinterpret_cast<const Bytef*>(raw.data()), raw.size());

    try {
        uLong read_crc = static_cast<uLong>(stoul(crc_str));
        return actual_crc == read_crc;
    } catch (...) {
        return false;
    }
}

void WAL::log(const string& msg) {
    lock_guard<mutex> log_lock(log_mutex);
    log_buffer << getCurrentTime() << "    " << msg << "\n";
    log_line_count++;
    if (log_line_count >= log_flush_lines) flush_log();
}

void WAL::flush_log() {
    if (!log_file_stream.is_open()) return;
    log_file_stream << log_buffer.str();
    log_file_stream.flush();
    log_buffer.str("");
    log_buffer.clear();
    log_line_count = 0;
}
