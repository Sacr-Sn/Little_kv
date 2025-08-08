#pragma once

#include <queue>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <functional>

struct CompactionTask {
    int target_level;
};

class CompactionManager {
public:
    CompactionManager();
    ~CompactionManager();

    void start();
    void stop();

    // compact�������
    void enqueue_task(const CompactionTask& task);

    // ע��ϲ�����ִ���߼���������ǵ���compact��
    void set_compact_executor(std::function<void(CompactionTask)> executor);

private:
    std::queue<CompactionTask> task_queue;  // �������
    std::mutex queue_mutex;
    std::condition_variable queue_cv;  // ��������
    std::thread worker;  // compact�����߳�
    std::atomic<bool> stop_flag{false};

    std::function<void(CompactionTask)> executor;

    // ѭ���߼�
    void run();
};
