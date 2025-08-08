#pragma once

#include <deque>
#include <memory>
#include <string>
#include <mutex>
#include "MemTable.h"  // �����е� MemTable ʵ��

struct ImmutableEntry {
    std::shared_ptr<MemTable> mem;
    std::string wal_filename;
};

class MemTableManager {
public:
    void add_immutable(std::shared_ptr<MemTable>& mem, const std::string& wal_name);
    bool has_immutable();
    ImmutableEntry get_oldest_immutable();
    void pop_oldest_immutable();
    // �����µ��ɵ�˳�򷵻�
    vector<shared_ptr<MemTable>> get_immutable_list_desc();

private:
    std::mutex mutex_;
    std::deque<ImmutableEntry> immutable_queue_;
};
