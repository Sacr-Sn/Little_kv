#include"../headers/Little_kv.h"

using namespace std;

Little_kv::Little_kv(){
    this->path = "../src/kv_on_file/data/db.txt";
    ifstream infile(path);
    if (!infile.is_open()) {
        cerr << "打开文件失败" << endl;
        return;
    }

    string line;
    while (getline(infile, line)) {
        istringstream iss(line);
        string key, value;
        if (getline(iss, key, ' ') && getline(iss, value)) {
            kv.insert({key, value});
        } else {
            cout << "init err" << endl;
        }
    }
    infile.close();
    cout << "init succeed!\n";
}


Little_kv::Little_kv(const string& path){
    this->path = path;
    ifstream infile(path);
    if (!infile.is_open()) {
        cerr << "打开文件失败" << endl;
        return;
    }

    string line;
    while (getline(infile, line)) {
        istringstream iss(line);
        string key, value;
        if (getline(iss, key, ' ') && getline(iss, value)) {
            kv.insert({key, value});
        } else {
            cout << "init err" << endl;
        }
    }
    infile.close();
    cout << "init succeed!\n";
}

Little_kv::~Little_kv(){
    ofstream outfile(path, ios::out | ios::trunc);
    if (!outfile.is_open()) {
        cerr << "写入文件失败！" << endl;
        return;
    }

    for(const auto& pair : kv) {
        outfile << pair.first << " " << pair.second << "\n";
        cout << "writing " << pair.first << " : " << pair.second << endl;
    }
    outfile.flush();
    outfile.close();
    cout << "over!\n";
}


bool Little_kv::put(const string& key, const string& value) {
    // evaluate atgs
    if(key.empty() || value.empty()){
        return false;
    }
    auto ret = kv.insert({key, value});
    if(!ret.second) {  // key already exists
        kv[key] = value;
    }
    return true;
}

string Little_kv::get(const string& key) {
    auto it = kv.find(key);
    if(it != kv.end()) {
        return it->second;
    }
    return "null";  // not found
}

bool Little_kv::del(const string& key) {
    auto n = kv.erase(key);
    return n;
}