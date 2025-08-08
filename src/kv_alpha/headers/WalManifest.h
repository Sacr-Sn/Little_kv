#pragma once

#include <string>
#include <vector>
#include <unordered_set>
#include <mutex>
#include <filesystem>
#include <iostream>
#include <algorithm>

class WalManifest {
public:
    std::string manifest_dir_;

    explicit WalManifest(const std::string& manifest_dir_);

    // ���� manifest �ļ����ݣ���������ʱ���ã�
    bool load();

    // ������һ�����鵵��wal_name
    std::string get_next_wal_name();

    // ���һ���µ�δˢ�� WAL �ļ���ˢ��ǰ���ã�
    void add_pending_wal(const std::string& wal_name);

    // ɾ���Ѿ�ˢ�̳ɹ��� WAL �ļ���ˢ�̺���ã�
    void remove_wal(const std::string& wal_name);

    // ��ȡ��ǰ��δˢ�̵����� WAL �ļ������ڻָ���
    std::vector<std::string> get_pending_wals() const;

private:
    bool rewrite_manifest(); // ԭ��д�� manifest �ļ�

    
    std::string manifest_path_;
    std::unordered_set<std::string> pending_wals_;
    mutable std::mutex mutex_;
};
