#include "../headers/WAL.h"

using namespace std;

WAL::WAL(string wal_dir) {
    wal_file_path = wal_dir + "wal.txt";
    log_file_path = wal_dir + "log.txt";
    cout << "---------- WAL START ----------" << endl;
}

WAL::~WAL() {
    cout << "---------- WAL OVER ----------" << endl;
}

void WAL::writeWAL(const string &op, const string &key, const string &value) {
    // ¼ÓËø
    lock_guard<mutex> wal_lock(wal_mutex);

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

void WAL::clearWAL() {
    // ¼ÓËø
    lock_guard<mutex> wal_lock(wal_mutex);

    ofstream ofs(wal_file_path, ios::trunc);
    if (!ofs.is_open()) {
        cerr << "Failed to open " << wal_file_path << endl;
    }
    ofs.close();
}

void WAL::recover_data(shared_ptr<MemTable>& active_memtable) {
    // ¼ÓËø
    lock_guard<mutex> wal_lock(wal_mutex);

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
            (*active_memtable).insert(key, value);
        } else if (op == "DEL") {
            (*active_memtable).insert(key, TOMBSTONE);
        } else {
            cerr << "UNknown op: " << op << endl;
        }
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

void WAL::log(const string& msg) {
    lock_guard<mutex> log_lock(log_mutex);

    ofstream log_out(log_file_path, ios::app);
    if (!log_out.is_open()) {
        cerr << "Failed to open " << log_file_path << endl;
        return;
    }
    log_out << getCurrentTime() << "    " << msg << "\n";
    log_out.close();
}