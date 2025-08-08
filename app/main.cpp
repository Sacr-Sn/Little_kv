#include <filesystem>

#include "../src/cli/headers/kv_cli.h"
#include "../src/kv_alpha/headers/ConfigLoader.h"
// #include "../src/kv_improve/headers/Little_kv.h"
// #include "../src/kv_improve/headers/WAL.h"

using namespace std;


int main() {
    // �������� begin
    // size_t memtable_threshold = 32;  // פ���ڴ�����ݴ�С����
    // size_t compact_threshold = 3;  // ÿ���ļ������ֵ��Լ������L0�ⲻǿ�ƣ�
    // size_t max_file_size = 32;  // (�����ļ���С��L0��L1)
    // size_t per_kv_size = 8;  // Ԥ��ÿ��kv��¼����
    // string wal_dir = "../logs/improve_log/wals/";
    // string log_dir = "../logs/improve_log/logs/";
    // string sst_dir = "../ssts/improve_sst/";
    // filesystem::create_directories(wal_dir);
    // filesystem::create_directories(log_dir);
    // filesystem::create_directories(sst_dir);
    
    // ʹ�������ļ���ʼ������
    ConfigLoader loader("../app/config.json");
    KVConfig conf = loader.getKVConfig();

    filesystem::create_directories(conf.wal_dir);
    filesystem::create_directories(conf.log_dir);
    filesystem::create_directories(conf.sst_dir);

    WAL wal(conf.wal_dir, conf.log_dir, (conf.memtable_threshold / conf.per_kv_size)*2);
    Little_kv kv(conf.sst_dir, conf.memtable_threshold, conf.compact_threshold, conf.max_file_size, conf.per_kv_size, wal);

    cout << "=== ===  LITTLE_KV BOOT  === ===" << endl << endl;

    // �������� end

    cout << "<<========= input 'cli' to boot cli =========>>" << endl;
    cout << "<<========= input others to end usage =========>>" << endl;

    while (true) {
        cout << "Your input: ";
        string input;
        cin >> input;
        if (input == "cli") {
            cin.ignore(numeric_limits<streamsize>::max(), '\n');  // ��ղ�������
            boot(kv);
        } else if (input == "exit") {
            cout << "=== ===  LITTLE_KV will exit  === ===" << endl;
            break;
        }
    }
    
}
