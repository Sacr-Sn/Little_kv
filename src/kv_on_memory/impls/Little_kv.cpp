#include"../headers/Little_kv.h"

using namespace std;

Little_kv::Little_kv(){
    cout << "init succeed!\n";
}
Little_kv::~Little_kv(){
    cout << "over!\n";
}

bool Little_kv::put(string key, string value) {
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

string Little_kv::get(string key) {
    auto it = kv.find(key);
    if(it != kv.end()) {
        return it->second;
    }
    return "null";  // not found
}

bool Little_kv::del(string key) {
    auto n = kv.erase(key);
    return n;
}