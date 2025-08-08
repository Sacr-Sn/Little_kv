#include "../headers/Little_kv.h"

using namespace std;

Little_kv::Little_kv(const string &wal_path, const string &sst_path, size_t memtable_threshold, const int& sst_id)
    : wal_file_path(wal_path), sst_file_path(sst_path), memtable_size_threshold(memtable_threshold), sst_file_id(sst_id)
{
    active_memtable = make_shared<MemTable>();
    immutable_memtable = nullptr;
    Init();
    readin_ssts_names();
    cout << "========= MemTable init Success ! =========" << endl;
}

Little_kv::~Little_kv()
{
    // write ssts_names to index file
    ofstream sst_index_out(sst_file_path + "ssts_index.txt", ios::trunc);
    if (!sst_index_out.is_open()) {
        cerr << "Failed to open ssts_index.txt" << endl;
        return;
    }
    for (const auto &name : ssts_names) {
        sst_index_out << name << "\n";
    }
    sst_index_out.close();
    cout << "========= OVER ! =========" << endl;
}

void Little_kv::Init()
{
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
            // (*active_memtable)[key] = value;
            (*active_memtable).insert(key, value);
        } else if (op == "DEL") {
            // (*active_memtable)[key] = TOMBSTONE;
            (*active_memtable).insert(key, TOMBSTONE);
        } else {
            cerr << "UNknown op: " << op << endl;
        }
    }
    wal_in.close();
}

void Little_kv::readin_ssts_names() {
    ifstream ssts_names_in(sst_file_path + "ssts_index.txt");
    if (!ssts_names_in.is_open()) {
        cerr << "Failed to open " << sst_file_path << endl;
        return;
    }    
    string name;
    while (ssts_names_in >> name) {
        ssts_names.push_back(name);
    }
    ssts_names_in.close();
}

void Little_kv::WriteWAL(const string &op, const string &key, const string &value)
{
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

size_t Little_kv::GetMemTableSize()
{
    return active_memtable->size();
}

string Little_kv::GenerateSSTFilename(int sst_id) {
    ostringstream oss;
    oss << "sst_" << setw(6) << setfill('0') << sst_id << ".sst";
    return oss.str();
}

void Little_kv::Flush()
{
    if (!active_memtable || active_memtable->empty())
        return;

    // 1. 将active_memtable变为immutable_memtable，创建新active_memtable
    immutable_memtable = active_memtable;
    active_memtable = make_shared<MemTable>();

    // 2. 将immutable_memtable写入sst文件（简单写法：覆盖写）
    string real_sst_path = sst_file_path + GenerateSSTFilename(sst_file_id);
    // cout << real_sst_path << endl;
    ssts_names.insert(ssts_names.begin(), GenerateSSTFilename(sst_file_id));  // add name to index file
    sst_file_id++;  // update sst_file_id

    ofstream sst_out(real_sst_path, std::ios::app);
    if (!sst_out.is_open()) {
        cerr << "Failed to open " << real_sst_path << endl;
        return;
    }
    auto entries = (*immutable_memtable).traverse();
    for (const auto &kv : entries)
    {
        sst_out << kv.first << " " << kv.second << "\n";
    }
    sst_out.close();

    // 3. 清空WAL
    ClearWAL();

    // 4. 释放immutable_memtable
    immutable_memtable.reset();
}

void Little_kv::ClearWAL()
{
    ofstream ofs(wal_file_path, ios::trunc);
    if (!ofs.is_open()) {
        cerr << "Failed to open " << wal_file_path << endl;
    }
    ofs.close();
}

bool Little_kv::Compact_sst_files(const vector<string>& input_ssts) {

    for(const auto& f : input_ssts) {
        cout << f << "    ";
    }
    cout << endl;

    unordered_map<string, string> tmp_map;

    string output_sst = "tmp.sst";

    // read in tmp_map
    for (const auto& file : input_ssts) {
        ifstream sst(sst_file_path + file);
        if (!sst.is_open()) {
            cerr << "Failed to open " << file << endl;
        }
        string key, value;
        while (sst >> key >> value) {
            if (tmp_map.find(key) == tmp_map.end()) {
                tmp_map[key] = value;
            }
        }
        sst.close();
    }


    // write to a new sst
    ofstream out(sst_file_path + output_sst);
    if (!out.is_open()) {
        cerr << "Failed to open " << output_sst << endl;
        return false;
    }
    for (const auto& [k, v] : tmp_map) {
        if ((v == TOMBSTONE) && (GenerateSSTFilename(0) == input_ssts.back())) continue;
        // if (v != TOMBSTONE) {
        //     out << k << " " << v << "\n";
        // }
        out << k << " " << v << "\n";
    }
    // del the old ssts
    string new_sst_name = input_ssts.back();
    for (const auto& file : input_ssts) {
        remove((sst_file_path+file).c_str());
        auto it = remove(ssts_names.begin(), ssts_names.end(), file);
        ssts_names.erase(it, ssts_names.end());
        sst_file_id--;
    }
    // change file name
    if (rename((sst_file_path + output_sst).c_str(), (sst_file_path + new_sst_name).c_str()) == 0) {
        cout << "compact success !" << endl;
        ssts_names.insert(ssts_names.begin(), new_sst_name);
        sst_file_id++;
        return true;
    } else {
        perror("File rename failed !");
        return false;
    }
}


bool Little_kv::put(const std::string &key, const std::string &value)
{
    WriteWAL("PUT", key, value);
    // (*active_memtable)[key] = value;
    bool ret = (*active_memtable).insert(key, value);

    if (GetMemTableSize() >= memtable_size_threshold)
    {
        Flush();
    }
    return ret;
}

bool Little_kv::del(const std::string &key)
{
    return put(key, TOMBSTONE); // 注意，日志中仍写del而不是put，不可直接调用put方法
}

string Little_kv::get(const string &key)
{
    // 先查 active_memtable
    string ret = (*active_memtable).search(key);
    if (ret != "_N_E_K_")
    { // // means exists the key
        if (ret == TOMBSTONE)
            return "NOT_FOUND"; // deled tag
        return ret;             // find the value
    }
    // 再查 immutable_memtable
    if (immutable_memtable)
    {
        ret = (*immutable_memtable).search(key);
        if (ret != "_N_E_K_")
        { // means exists the key
            if (ret == TOMBSTONE)
                return "NOT_FOUND"; // deled tag
            return ret;             // find the value
        }
    }
    // 查 sst 文件   from new to old 
    for (const auto& sst_name : ssts_names) {
        // cout << "read from " << sst_name << endl;
        string full_sst_path = sst_file_path + sst_name;
        ifstream sst_in(full_sst_path);
        if (!sst_in.is_open()) {
            cerr << "sst file can not open !" << endl;
            continue;
        }
        string k, v;
        while (sst_in >> k >> v) {
            if (key == k) {
                if (v == TOMBSTONE)
                    return "NOT_FOUND";
                sst_in.close();
                return v;
            }
        }
        sst_in.close();
    }

    return "NOT_FOUND";
}

bool Little_kv::manual_compact(int compact_file_count) {
    if ((compact_file_count < 2) || (compact_file_count > ssts_names.size())) {
        cout << "do not need to compact" << endl;
        return false;
    }
    vector<string> sst_to_compact;
    for (int i=0;i<compact_file_count; i++) {
        string f = ssts_names[i];
        sst_to_compact.push_back(f);
    }
    return Compact_sst_files(sst_to_compact);
}