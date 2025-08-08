#include"../headers/Little_kv.h"

using namespace std;

Little_kv::Little_kv(){
    // cout << "no args construction" << endl;
    this->log_path = "../src/kv_on_wal/logs/log.txt";
    recover();
    cout << "=================== init succeed! ===================\n";
}


Little_kv::Little_kv(const string& log_path){
    // cout << "exists args construction" << endl;
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
    // while (getline(infile, line)) {
    //     istringstream iss(line);
    //     string op, key, value;
    //     if (getline(iss, op, ' ') && getline(iss, key, ' ') && getline(iss, value)) {
    //         // cout << op << " " << key << " " << value << endl;
    //         this->deal(op, key, value);
    //     } else {
    //         cout << "recover err" << endl;
    //     }
    // }
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
            kv[key] = value;
        } else if (op == "DEL") {
            kv.erase(key);
        } else {
            cerr << "Warning: unknown op: " << op << endl;
        }
    }
    infile.close();
    return true;
}

// bool Little_kv::deal(const string& op, const string& key, const string& value) {
//     // deal operation bases on op
//     if (op == "put") {
//         auto ret = kv.insert({key, value});
//         if(!ret.second) {  // key already exists
//             kv[key] = value;
//         }
//     } else if (op == "del") {
//         auto n = kv.erase(key);
//     }
//     return true;
// }

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
        return false;
    }
    outfile.close();
    return true;
}


bool Little_kv::put(const string& key, const string& value) {
    // evaluate atgs
    if(key.empty() || value.empty()){
        return false;
    }
    auto ret = log("PUT", key, value);    
    if(ret) {
        kv[key] = value;
        return true;
    }
    return false;
}

string Little_kv::get(const string& key) {
    auto it = kv.find(key);
    if(it != kv.end()) {
        return it->second;
    }
    return "";  // not found
}

bool Little_kv::del(const string& key) {
    auto ret = log("DEL", key);
    if (ret) return kv.erase(key);
    return false;
}