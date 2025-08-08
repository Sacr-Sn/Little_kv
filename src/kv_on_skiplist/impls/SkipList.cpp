#include "../headers/SkipList.h"

using namespace std;


int SkipList::randomLevel() {
    int lvl = 1;
    while ((rand() % 100) < (probability * 100) && lvl < maxLevel) {
        lvl++;
    }
    return lvl;
}

// 插入键值
bool SkipList::insert(string key, string value) {
    std::vector<Node *> update(maxLevel, nullptr);
    Node *current = header;

    // 从最高层开始查找插入位置
    for (int i = currentLevel - 1; i >= 0; i--) {
        while (current->forward[i] != nullptr &&
               current->forward[i]->key < key) {
            current = current->forward[i];
        }
        update[i] = current; // 记录每层需要更新的节点
    }

    current = current->forward[0];

    // 如果key已存在，cover the old value
    if (current == nullptr || current->key != key) {
        int newLevel = randomLevel();

        // 如果新节点层数高于当前层数，更新上层指针
        if (newLevel > currentLevel) {
            for (int i = currentLevel; i < newLevel; i++)
            {
                update[i] = header;
            }
            currentLevel = newLevel;
        }

        // 创建新节点
        Node *newNode = new Node(key, value, newLevel);

        // 更新各层指针
        for (int i = 0; i < newLevel; i++) {
            newNode->forward[i] = update[i]->forward[i];
            update[i]->forward[i] = newNode;
        }
        // std::cout << "Inserted key " << key << " at level " << newLevel << std::endl;
        return true;
    } else {
        current->value = value;
        return true;
    }
}

// 查找键值
string SkipList::search(string key)
{
    Node *current = header;

    // 从最高层开始查找，直到底层（不从底层开始，前面跳跃，可以少查一些）
    for (int i = currentLevel - 1; i >= 0; i--) {
        while (current->forward[i] != nullptr &&
               current->forward[i]->key < key) {
            current = current->forward[i];
        }
    }

    current = current->forward[0];
    // return current != nullptr && current->key == key;
    return ((current != nullptr && current->key == key) ? current->value : "_N_E_K_");
}

// 删除键值
bool SkipList::erase(string key) {
    std::vector<Node *> update(maxLevel, nullptr);
    Node *current = header;

    // 查找要删除的节点
    for (int i = currentLevel - 1; i >= 0; i--) {
        while (current->forward[i] != nullptr &&
               current->forward[i]->key < key) {
            current = current->forward[i];
        }
        update[i] = current;
    }

    current = current->forward[0];

    // 如果找到key，则删除
    if (current != nullptr && current->key == key) {
        // 更新各层指针
        for (int i = 0; i < currentLevel; i++)
        {
            if (update[i]->forward[i] != current)
                break;
            update[i]->forward[i] = current->forward[i];
        }

        delete current;

        // 如果删除的是最高层节点，降低currentLevel
        while (currentLevel > 1 && header->forward[currentLevel - 1] == nullptr)
        {
            currentLevel--;
        }

        // std::cout << "Deleted key " << key << std::endl;
        return true;
    } else {
        return false;  // key not exists
    }
}

// 打印跳表结构
void SkipList::display()
{
    std::cout << "\n*****Skip List*****" << std::endl;
    for (int i = 0; i < currentLevel; i++)
    {
        Node *node = header->forward[i];
        std::cout << "Level " << i << ": ";
        while (node != nullptr)
        {
            std::cout << node->key << ":" << node->value << " ";
            node = node->forward[i];
        }
        std::cout << std::endl;
    }
}