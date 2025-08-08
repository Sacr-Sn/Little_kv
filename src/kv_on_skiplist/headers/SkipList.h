#pragma once

#include <iostream>
#include <vector>
#include <cstdlib>
#include <ctime>
#include <limits>
#include <string>
#include <fstream>
#include <string>
#include <sstream>
#include <filesystem>

using namespace std;

class SkipList {
private:
    struct Node {
        string key;
        string value;
        std::vector<Node*> forward;  // forward[i] 是指向第 i 层中该节点的下一个节点。
        
        Node(string k, string v, int level) : key(k), value(v), forward(level, nullptr) {}
    };

    int maxLevel;      // 最大层数
    float probability;  // 节点晋升概率
    Node* header;      // 头节点，指向每层的起点。
    int currentLevel;  // 当前最大层数

    public:
    SkipList(int maxLvl = 16, float p = 0.5)
        : maxLevel(maxLvl), probability(p), currentLevel(1) {
        this->header = new Node("", "", maxLevel);
    }

    ~SkipList() {
        Node *current = header->forward[0];
        while (current != nullptr) {
            Node *next = current->forward[0];
            delete current;
            current = next;
        }
        delete header;
    }


    // 随机生成节点层数
    // 每个新插入的节点会通过这个函数决定自己的层数。
    // 晋升概率控制了跳表的“稀疏程度”。
    int randomLevel();

    // 插入键值
    bool insert(string key, string value);

    // 查找键值
    string search(string key);

    // 删除键值
    bool erase(string key);

    // 打印跳表结构
    void display();
};

