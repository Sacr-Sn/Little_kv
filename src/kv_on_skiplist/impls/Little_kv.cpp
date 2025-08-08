#include "../headers/Little_kv.h"

using namespace std;


Little_kv::Little_kv(){
    this->log_path = "../logs/log.txt";
    recover();
    cout << "=================== init succeed! ===================\n";
}


Little_kv::Little_kv(const string& log_path){
    this->log_path = log_path;
    recover();
    cout << "=================== init succeed! ===================\n";
}

Little_kv::~Little_kv(){
    cout << "=================== over! ===================\n";
}

bool Little_kv::recover() {
    ifstream infile(log_path);
    if (!infile.is_open()) {
        cerr << "打开文件失败" << endl;
        return false;
    }

    string line;
    while(getline(infile, line)) {
        istringstream iss(line);
        string op, key, value;
        if (!(iss >> op >> key)) {
            cerr << "Warning: invalid log line: " << line << endl;
            continue;
        }
        if (op == "PUT") {
            if (!(iss >> value)) {
                cerr << "Warning: invalid PUT line: " << line << endl;
                continue;
            }
            store_.insert(key, value);
        } else if (op == "DEL") {
            store_.erase(key);
        } else {
            cerr << "Warning: unknown op: " << op << endl;
        }
    }
    infile.close();
    return true;
}

bool Little_kv::log(const string& op, const string& key) {
    ofstream outfile(log_path, ios::out | ios::app);
    if (!outfile.is_open()) {
        cerr << "out open err" << endl;
        return false;
    }

    outfile << op << " " << key  << "\n";
    outfile.flush();
    if (outfile.fail()) {
        cerr << "Error: failed to write to log. \n";
        outfile.close();
        return false;
    }
    outfile.close();
    return true;
}


bool Little_kv::log(const string& op, const string& key, const string& value) {
    ofstream outfile(log_path, ios::out | ios::app);
    if (!outfile.is_open()) {
        cerr << "out open err" << endl;
        return false;
    }

    outfile << op << " " << key << " " << value << "\n";
    outfile.flush();
    if (outfile.fail()) {
        cerr << "Error: failed to write to log. \n";
        outfile.close();
        return false;
    }
    outfile.close();
    return true;
}


// 插入键值
bool Little_kv::put(string key, string value) {
    return ((key.empty() || value.empty()) ? false : store_.insert(key, value));
}

// 查找键值
string Little_kv::get(string key) {
    return (key.empty() ? "_N_A_K_" : store_.search(key));
}

// 删除键值
bool Little_kv::del(string key) {
    return (key.empty() ? false : store_.erase(key));
}

// 打印跳表结构
void Little_kv::display() {
    store_.display();
}